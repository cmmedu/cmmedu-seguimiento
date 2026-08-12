from datetime import datetime, timedelta
from django.test import Client, TestCase
from django.urls import reverse
from xmodule.modulestore.tests.django_utils import ModuleStoreTestCase
from xmodule.modulestore.tests.factories import CourseFactory, ItemFactory
from common.djangoapps.student.tests.factories import UserFactory, CourseEnrollmentFactory
from capa.tests.response_xml_factory import StringResponseXMLFactory
from lms.djangoapps.courseware.models import StudentModule
from lms.djangoapps.courseware.tests.factories import StudentModuleFactory
from boto.exception import BotoServerError
from django.test import override_settings
from lms.djangoapps.instructor_task.api_helper import AlreadyRunningError
from opaque_keys.edx.locator import CourseLocator
from six.moves import range
from unittest.mock import MagicMock, patch
import json

from cmmedu_seguimiento.models import DjangoStorageJsonReportStore, JsonReportEncoder, JsonReportStore
from cmmedu_seguimiento.utils import build_blocks_data, list_problem_responses


XBLOCK_COUNT = 10

USER_COUNT = 5


class FakeInstructorTask(object):
    """Lightweight stand-in for lms.djangoapps.instructor_task.models.InstructorTask."""

    def __init__(self, task_state, task_output=None, task_id='task-1', created=None, updated=None):
        self.task_state = task_state
        self.task_output = task_output
        self.task_id = task_id
        self.created = created or datetime(2020, 1, 1, 12, 0, 0)
        self.updated = updated or datetime(2020, 1, 1, 12, 5, 0)


class TestCMMEduSeguimiento(ModuleStoreTestCase):

    def setUp(self):
        super(TestCMMEduSeguimiento, self).setUp()

        # Create clients
        self.non_auth_client = Client()
        self.auth_client = Client()
        self.user_staff = UserFactory(
            username='testuser3',
            password='12345',
            email='student2@edx.org',
            is_staff=True)
        self.auth_client.login(username='testuser3', password='12345')

        # Create courses
        self.course1 = CourseFactory.create(org='mss', course='100', run='2020', display_name='Sample course 1')
        self.course2 = CourseFactory.create(org='mss', course='101', run='2020', display_name='Sample course 2')
        self.course3 = CourseFactory.create(org='mss', course='102', run='2020', display_name='Sample course 3')

        # Now give it some content
        with self.store.bulk_operations(self.course1.id, emit_signals=False):
            chapter = ItemFactory.create(
                parent_location=self.course1.location,
                category="sequential",
            )
            section = ItemFactory.create(
                parent_location=chapter.location,
                category="sequential",
                metadata={'graded': True, 'format': 'Homework'}
            )
            self.items = [
                ItemFactory.create(
                    parent_location=section.location,
                    category="problem",
                    data=StringResponseXMLFactory().build_xml(answer='foo'),
                    metadata={'rerandomize': 'always'}
                )
                for __ in range(XBLOCK_COUNT - 1)
            ]

        # Create users and enroll them in the course
        self.users = [UserFactory.create() for _ in range(USER_COUNT)]
        for user in self.users:
            CourseEnrollmentFactory.create(user=user, course_id=self.course1.id)
        for i, item in enumerate(self.items):
            for j, user in enumerate(self.users):
                StudentModuleFactory.create(
                    grade=1 if i < j else 0,
                    max_grade=1,
                    student=user,
                    course_id=self.course1.id,
                    module_state_key=item.location
                )


    def test_endpoints_authentication(self):
        """
        Test that the endpoints require authentication.
        """
        response1 = self.non_auth_client.post(reverse('cmmedu_seguimiento:cmmedu_seguimiento_get_report'), data={})
        self.assertEqual(response1.status_code, 401)
        response2 = self.non_auth_client.post(reverse('cmmedu_seguimiento:cmmedu_seguimiento_make_report'), data={})
        self.assertEqual(response2.status_code, 401)


    def test_no_task_created_if_not_course_key(self):
        """
        Test that no task is created if the course key is missing.
        """
        response = self.auth_client.post(reverse('cmmedu_seguimiento:cmmedu_seguimiento_make_report'), content_type="application/json", data={})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.content, b'Missing course_key')


    def test_no_task_created_if_bad_course_key(self):
        """
        Test that no task is created if the course key is missing.
        """
        response = self.auth_client.post(reverse('cmmedu_seguimiento:cmmedu_seguimiento_make_report'), content_type="application/json", data={"course_key": "BAD_COURSE_KEY"})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.content, b'Invalid course_key')


    def test_get_report_no_course_key(self):
        """
        Test that no report is returned if the course key is missing.
        """
        response = self.auth_client.post(reverse('cmmedu_seguimiento:cmmedu_seguimiento_get_report'), content_type="application/json", data={})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.content, b'Missing course_key')


    def test_get_report_bad_course_key(self):
        """
        Test that no report is returned if the course key is missing.
        """
        response = self.auth_client.post(reverse('cmmedu_seguimiento:cmmedu_seguimiento_get_report'), content_type="application/json", data={"course_key": "BAD_COURSE_KEY"})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.content, b'Invalid course_key')

    
    def test_get_report_no_report(self):
        """
        Test that no report is returned if no task has been created.
        """
        response = self.auth_client.post(
            reverse('cmmedu_seguimiento:cmmedu_seguimiento_get_report'),
            content_type="application/json",
            data='{"course_key": "%s"}' % str(self.course2.id),
        )
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        self.assertEqual(response_json['status'], 0)
        self.assertEqual(response_json['msg'], 'No hay tareas de reportes asociadas a este curso.')


    def test_task_create(self):
        """
        Test that a task is created when the course key is valid.
        """
        response1 = self.auth_client.post(
            reverse('cmmedu_seguimiento:cmmedu_seguimiento_make_report'),
            content_type="application/json",
            data='{"course_key": "%s"}' % str(self.course1.id),
        )
        self.assertEqual(response1.status_code, 200)
        response1_json = response1.json()
        self.assertEqual(response1_json['status'], 1)
        self.assertEqual(response1_json['msg'], 'Se ha iniciado la generación del reporte.')
        self.assertIn('task_id', response1_json)


    def test_make_report_invalid_json(self):
        response = self.auth_client.post(
            reverse('cmmedu_seguimiento:cmmedu_seguimiento_make_report'),
            content_type="application/json",
            data='not-json',
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.content, b'Invalid JSON')


    def test_make_report_course_not_found(self):
        response = self.auth_client.post(
            reverse('cmmedu_seguimiento:cmmedu_seguimiento_make_report'),
            content_type="application/json",
            data='{"course_key": "course-v1:mss+999+2020"}',
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.content, b'Invalid course_key')


    def test_make_report_already_running(self):
        with patch('cmmedu_seguimiento.views.submit_task_make_report', side_effect=AlreadyRunningError('running')):
            response = self.auth_client.post(
                reverse('cmmedu_seguimiento:cmmedu_seguimiento_make_report'),
                content_type="application/json",
                data='{"course_key": "%s"}' % str(self.course1.id),
            )
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        self.assertEqual(response_json['status'], 0)
        self.assertEqual(response_json['msg'], 'Esta tarea ya está en progreso.')


    def test_get_report_invalid_json(self):
        response = self.auth_client.post(
            reverse('cmmedu_seguimiento:cmmedu_seguimiento_get_report'),
            content_type="application/json",
            data='not-json',
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.content, b'Invalid JSON')


    def test_get_report_course_not_found(self):
        response = self.auth_client.post(
            reverse('cmmedu_seguimiento:cmmedu_seguimiento_get_report'),
            content_type="application/json",
            data='{"course_key": "course-v1:mss+999+2020"}',
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.content, b'Invalid course_key')


    def test_get_report_in_progress(self):
        fake_task = FakeInstructorTask(task_state='PROGRESS')
        with patch('cmmedu_seguimiento.views.InstructorTask') as MockTask:
            MockTask.objects.filter.return_value.order_by.return_value.all.return_value = [fake_task]
            response = self.auth_client.post(
                reverse('cmmedu_seguimiento:cmmedu_seguimiento_get_report'),
                content_type="application/json",
                data='{"course_key": "%s"}' % str(self.course1.id),
            )
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        self.assertEqual(response_json['status'], 0)
        self.assertEqual(response_json['msg'], 'La tarea de reportes aún no está lista.')


    def test_get_report_failure(self):
        fake_task = FakeInstructorTask(task_state='FAILURE', task_output='boom')
        with patch('cmmedu_seguimiento.views.InstructorTask') as MockTask:
            MockTask.objects.filter.return_value.order_by.return_value.all.return_value = [fake_task]
            response = self.auth_client.post(
                reverse('cmmedu_seguimiento:cmmedu_seguimiento_get_report'),
                content_type="application/json",
                data='{"course_key": "%s"}' % str(self.course1.id),
            )
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        self.assertEqual(response_json['status'], 0)
        self.assertEqual(response_json['msg'], 'La tarea de reportes ha fallado.')
        self.assertEqual(response_json['task_error'], 'boom')


    def test_get_report_unknown_state(self):
        fake_task = FakeInstructorTask(task_state='REVOKED')
        with patch('cmmedu_seguimiento.views.InstructorTask') as MockTask:
            MockTask.objects.filter.return_value.order_by.return_value.all.return_value = [fake_task]
            response = self.auth_client.post(
                reverse('cmmedu_seguimiento:cmmedu_seguimiento_get_report'),
                content_type="application/json",
                data='{"course_key": "%s"}' % str(self.course1.id),
            )
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        self.assertEqual(response_json['status'], 0)
        self.assertEqual(response_json['msg'], 'Estado de la tarea desconocido.')
        self.assertEqual(response_json['task_state'], 'REVOKED')


    def test_get_report_success_malformed_output(self):
        fake_task = FakeInstructorTask(task_state='SUCCESS', task_output=json.dumps({'foo': 'bar'}))
        with patch('cmmedu_seguimiento.views.InstructorTask') as MockTask:
            MockTask.objects.filter.return_value.order_by.return_value.all.return_value = [fake_task]
            response = self.auth_client.post(
                reverse('cmmedu_seguimiento:cmmedu_seguimiento_get_report'),
                content_type="application/json",
                data='{"course_key": "%s"}' % str(self.course1.id),
            )
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        self.assertEqual(response_json['status'], 0)
        self.assertEqual(response_json['msg'], 'Formato de output de tarea inválido.')


    def test_get_report_success(self):
        task_output = json.dumps({
            'course_key': 'mss_100_2020',
            'timestamp': '2020-01-01-1200',
            'n_reports': 2,
        })
        fake_task = FakeInstructorTask(task_state='SUCCESS', task_output=task_output, task_id='task-42')
        fake_links = [
            ('mss_100_2020_student_profile_2020-01-01-1200.tar.gz', 'http://example.com/profile'),
            ('mss_100_2020_ora_data_2020-01-01-1200.tar.gz', 'http://example.com/ora'),
            ('mss_100_2020_report_data_1_2020-01-01-1200.tar.gz', 'http://example.com/r1'),
            ('mss_100_2020_report_data_2_2020-01-01-1200.tar.gz', 'http://example.com/r2'),
        ]
        with patch('cmmedu_seguimiento.views.InstructorTask') as MockTask, \
                patch('cmmedu_seguimiento.views.JsonReportStore') as MockStore:
            MockTask.objects.filter.return_value.order_by.return_value.all.return_value = [fake_task]
            MockStore.from_config.return_value.links_for_names.return_value = fake_links
            response = self.auth_client.post(
                reverse('cmmedu_seguimiento:cmmedu_seguimiento_get_report'),
                content_type="application/json",
                data='{"course_key": "%s"}' % str(self.course1.id),
            )
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        self.assertEqual(response_json['status'], 1)
        output = response_json['output']
        self.assertEqual(output['student_profile'], 'http://example.com/profile')
        self.assertEqual(output['ora_data'], 'http://example.com/ora')
        self.assertEqual(output['blocks_data'], {'1': 'http://example.com/r1', '2': 'http://example.com/r2'})
        self.assertEqual(output['task_id'], 'task-42')


    def test_delete_report_invalid_json(self):
        response = self.auth_client.post(
            reverse('cmmedu_seguimiento:cmmedu_seguimiento_delete_report'),
            content_type="application/json",
            data='not-json',
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.content, b'Invalid JSON')


    def test_delete_report_no_course_key(self):
        response = self.auth_client.post(
            reverse('cmmedu_seguimiento:cmmedu_seguimiento_delete_report'),
            content_type="application/json",
            data={},
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.content, b'Missing course_key')


    def test_delete_report_bad_course_key(self):
        response = self.auth_client.post(
            reverse('cmmedu_seguimiento:cmmedu_seguimiento_delete_report'),
            content_type="application/json",
            data='{"course_key": "BAD_COURSE_KEY"}',
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.content, b'Invalid course_key')


    def test_delete_report_course_not_found(self):
        response = self.auth_client.post(
            reverse('cmmedu_seguimiento:cmmedu_seguimiento_delete_report'),
            content_type="application/json",
            data='{"course_key": "course-v1:mss+999+2020"}',
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.content, b'Invalid course_key')


    def test_delete_report_no_tasks(self):
        response = self.auth_client.post(
            reverse('cmmedu_seguimiento:cmmedu_seguimiento_delete_report'),
            content_type="application/json",
            data='{"course_key": "%s"}' % str(self.course2.id),
        )
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        self.assertEqual(response_json['status'], 0)
        self.assertEqual(response_json['msg'], 'No hay tareas de reportes asociadas a este curso.')


    def test_delete_report_success(self):
        task_output = json.dumps({
            'course_key': 'mss_100_2020',
            'timestamp': '2020-01-01-1200',
            'n_reports': 1,
        })
        fake_task_success = FakeInstructorTask(task_state='SUCCESS', task_output=task_output)
        fake_task_pending = FakeInstructorTask(task_state='PROGRESS')
        fake_task_bad_output = FakeInstructorTask(task_state='SUCCESS', task_output='not-json')

        mock_storage = MagicMock()
        mock_storage.exists.side_effect = [True, False, True]
        mock_report_store = MagicMock()
        mock_report_store.storage = mock_storage
        mock_report_store.path_to.side_effect = lambda course_id, name: name

        with patch('cmmedu_seguimiento.views.InstructorTask') as MockTask, \
                patch('cmmedu_seguimiento.views.JsonReportStore') as MockStore:
            MockTask.objects.filter.return_value.order_by.return_value.all.return_value = [
                fake_task_success, fake_task_pending, fake_task_bad_output,
            ]
            MockStore.from_config.return_value = mock_report_store
            response = self.auth_client.post(
                reverse('cmmedu_seguimiento:cmmedu_seguimiento_delete_report'),
                content_type="application/json",
                data='{"course_key": "%s"}' % str(self.course1.id),
            )
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        self.assertEqual(response_json['status'], 1)
        self.assertEqual(len(response_json['deleted_files']), 2)


    def test_list_problem_responses(self):
        problem_key = self.items[0].location
        StudentModule.objects.filter(
            course_id=self.course1.id, module_state_key=problem_key
        ).update(state=json.dumps({'done': True}))
        responses = list_problem_responses(self.course1.id, str(problem_key))
        self.assertEqual(len(responses), USER_COUNT)
        for response in responses:
            self.assertIn('username', response)
            self.assertIn('timestamp', response)
            self.assertIn('state', response)


    def test_list_problem_responses_wrong_course(self):
        problem_key = self.items[0].location
        responses = list_problem_responses(self.course2.id, str(problem_key))
        self.assertEqual(responses, [])


    def test_list_problem_responses_limit(self):
        problem_key = self.items[0].location
        StudentModule.objects.filter(
            course_id=self.course1.id, module_state_key=problem_key
        ).update(state=json.dumps({'done': True}))
        responses = list_problem_responses(self.course1.id, str(problem_key), limit_responses=2)
        self.assertEqual(len(responses), 2)


    def test_build_blocks_data(self):
        """
        Diagnostic/direct test: exercises build_blocks_data the same way
        make_report does, but bypasses the Celery task wrapper so any
        exception surfaces here as a normal test failure with a full
        traceback, instead of being swallowed as a FAILURE task state.
        """
        problem_locations = str(self.store.get_course(self.course1.id).location)
        report_names = build_blocks_data(
            user_id=self.user_staff.pk,
            course_key=self.course1.id,
            usage_key_str=problem_locations,
            start_date=datetime(2020, 1, 1, 12, 0, 0),
        )
        self.assertIsInstance(report_names, list)


class TestCMMEduSeguimientoModels(TestCase):

    def test_json_report_encoder_timedelta(self):
        result = json.dumps({'d': timedelta(seconds=5)}, cls=JsonReportEncoder)
        self.assertIn('0:00:05', result)


    def test_json_report_encoder_datetime(self):
        now = datetime(2020, 1, 1, 12, 0, 0)
        result = json.dumps({'t': now}, cls=JsonReportEncoder)
        self.assertIn(now.isoformat(), result)


    def test_json_report_encoder_deprecated_string(self):
        class FakeUsageKey(object):
            def to_deprecated_string(self):
                return "fake-key"

            def __str__(self):
                return "fake-key"

        result = json.dumps({'k': FakeUsageKey()}, cls=JsonReportEncoder)
        self.assertIn("fake-key", result)


    def test_json_report_encoder_unsupported_type(self):
        with self.assertRaises(TypeError):
            json.dumps({'k': object()}, cls=JsonReportEncoder)


    def test_from_config_default_branch(self):
        store = DjangoStorageJsonReportStore.from_config('GRADES_DOWNLOAD')
        self.assertIsInstance(store, DjangoStorageJsonReportStore)


    def test_json_report_store_from_config(self):
        store = JsonReportStore.from_config('GRADES_DOWNLOAD')
        self.assertIsInstance(store, DjangoStorageJsonReportStore)


    def test_path_to_and_links_for_names(self):
        store = DjangoStorageJsonReportStore()
        course_id = CourseLocator(org='mss', course='100', run='2020')
        path = store.path_to(course_id, 'file.json')
        self.assertTrue(path.endswith('file.json'))

        links = store.links_for_names(course_id, ['a.json', 'b.json'])
        self.assertEqual([name for name, _ in links], ['a.json', 'b.json'])


    def test_links_for_missing_dir_returns_empty(self):
        store = DjangoStorageJsonReportStore()
        course_id = CourseLocator(org='mss', course='999', run='2020')
        self.assertEqual(store.links_for(course_id), [])


    def test_store_and_links_for(self):
        store = DjangoStorageJsonReportStore()
        course_id = CourseLocator(org='mss', course='101', run='report-store-test')
        store.store_json(course_id, 'report.tar.gz', {'a': 1})
        links = store.links_for(course_id)
        self.assertIn('report.tar.gz', [name for name, _ in links])


    @override_settings(FAKE_S3_CONFIG={
        'STORAGE_TYPE': 's3',
        'BUCKET': 'test-bucket',
        'ROOT_PATH': 'test-root',
        'CUSTOM_DOMAIN': 'cdn.example.com',
    })
    def test_from_config_s3_branch(self):
        with patch('cmmedu_seguimiento.models.DjangoStorageJsonReportStore') as MockStore:
            JsonReportStore.from_config('FAKE_S3_CONFIG')
            MockStore.assert_called_once()
            _, kwargs = MockStore.call_args
            self.assertEqual(kwargs['storage_class'], 'storages.backends.s3boto.S3BotoStorage')
            self.assertEqual(kwargs['storage_kwargs']['bucket'], 'test-bucket')
            self.assertEqual(kwargs['storage_kwargs']['custom_domain'], 'cdn.example.com')


    @override_settings(FAKE_GENERIC_CONFIG={})
    def test_from_config_generic_default_branch(self):
        store = JsonReportStore.from_config('FAKE_GENERIC_CONFIG')
        self.assertIsInstance(store, DjangoStorageJsonReportStore)


    def test_links_for_boto_error(self):
        store = DjangoStorageJsonReportStore()
        course_id = CourseLocator(org='mss', course='103', run='2020')
        boto_error = BotoServerError(500, 'Internal Error')
        with patch.object(store.storage, 'listdir', side_effect=boto_error):
            result = store.links_for(course_id)
        self.assertEqual(result, [])
