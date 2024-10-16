from boto.exception import BotoServerError
from datetime import timedelta, datetime
from django.conf import settings
from django.core.files.base import ContentFile
import hashlib
import json
import logging
from openedx.core.storage import get_storage
import os.path
from six import text_type, PY2

logger = logging.getLogger(__name__)


class JsonReportEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, timedelta):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)
    

class JsonReportStore(object):
    """
    Simple abstraction layer that can fetch and store JSON files for reports
    download. Should probably refactor later to create a ReportFile object that
    can simply be appended to for the sake of memory efficiency, rather than
    passing in the whole dataset. Doing that for now just because it's simpler.
    """
    @classmethod
    def from_config(cls, config_name):
        """
        Return one of the ReportStore subclasses depending on django
        configuration. Look at subclasses for expected configuration.
        """
        # Convert old configuration parameters to those expected by
        # DjangoStorageReportStore for backward compatibility
        config = getattr(settings, config_name, {})
        storage_type = config.get('STORAGE_TYPE', '').lower()
        if storage_type == 's3':
            return DjangoStorageJsonReportStore(
                storage_class='storages.backends.s3boto.S3BotoStorage',
                storage_kwargs={
                    'bucket': config['BUCKET'],
                    'location': config['ROOT_PATH'],
                    'custom_domain': config.get("CUSTOM_DOMAIN", None),
                    'querystring_expire': 300,
                    'gzip': True,
                },
            )
        elif storage_type == 'localfs':
            return DjangoStorageJsonReportStore(
                storage_class='django.core.files.storage.FileSystemStorage',
                storage_kwargs={
                    'location': config['ROOT_PATH'],
                },
            )
        return DjangoStorageJsonReportStore.from_config(config_name)



class DjangoStorageJsonReportStore(JsonReportStore):
    """
    ReportStore implementation that delegates to django's storage api.
    """
    def __init__(self, storage_class=None, storage_kwargs=None):
        if storage_kwargs is None:
            storage_kwargs = {}
        self.storage = get_storage(storage_class, **storage_kwargs)

    @classmethod
    def from_config(cls, config_name):
        """
        By default, the default file storage specified by the `DEFAULT_FILE_STORAGE`
        setting will be used. To configure the storage used, add a dict in
        settings with the following fields::

            STORAGE_CLASS : The import path of the storage class to use. If
                            not set, the DEFAULT_FILE_STORAGE setting will be used.
            STORAGE_KWARGS : An optional dict of kwargs to pass to the storage
                             constructor. This can be used to specify a
                             different S3 bucket or root path, for example.

        Reference the setting name when calling `.from_config`.
        """
        return cls(
            getattr(settings, config_name).get('STORAGE_CLASS'),
            getattr(settings, config_name).get('STORAGE_KWARGS'),
        )

    def store(self, course_id, filename, buff):
        """
        Store the contents of `buff` in a directory determined by hashing
        `course_id`, and name the file `filename`. `buff` can be any file-like
        object, ready to be read from the beginning.
        """
        path = self.path_to(course_id, filename)
        if not PY2:
            buff_contents = buff.read()

            if not isinstance(buff_contents, bytes):
                buff_contents = buff_contents.encode('utf-8')

            buff = ContentFile(buff_contents)

        self.storage.save(path, buff)


    def store_json(self, course_id, filename, data):
        """
        Given a course_id, filename, and data (a Python dict or list), 
        write the data to the storage backend in JSON format.
        """
        json_data = json.dumps(data, ensure_ascii=False, indent=4, cls=JsonReportEncoder)
        output_buffer = ContentFile(json_data)
        self.store(course_id, filename, output_buffer)


    def links_for(self, course_id):
        """
        For a given `course_id`, return a list of `(filename, url)` tuples.
        Calls the `url` method of the underlying storage backend. Returned
        urls can be plugged straight into an href
        """
        course_dir = self.path_to(course_id)
        try:
            _, filenames = self.storage.listdir(course_dir)
        except OSError:
            # Django's FileSystemStorage fails with an OSError if the course
            # dir does not exist; other storage types return an empty list.
            return []
        except BotoServerError as ex:
            logger.error(
                u'Fetching files failed for course: %s, status: %s, reason: %s',
                course_id,
                ex.status,
                ex.reason
            )
            return []
        files = [(filename, os.path.join(course_dir, filename)) for filename in filenames]
        files.sort(key=lambda f: self.storage.get_modified_time(f[1]), reverse=True)
        return [
            (filename, self.storage.url(full_path))
            for filename, full_path in files
        ]

    def path_to(self, course_id, filename=''):
        """
        Return the full path to a given file for a given course.
        """
        hashed_course_id = hashlib.sha1(text_type(course_id).encode('utf-8')).hexdigest()
        return os.path.join(hashed_course_id, filename)