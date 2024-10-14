from celery import task
from django.utils.translation import ugettext_noop
from functools import partial
from lms.djangoapps.instructor_task.api_helper import submit_task
from lms.djangoapps.instructor_task.tasks_base import BaseInstructorTask
from lms.djangoapps.instructor_task.tasks_helper.runner import run_main_task

from .utils import upload_students_csv


def submit_calculate_students_features_csv(request, course_key, features):
    """
    Submits a task to generate a CSV containing student profile info.

    Raises AlreadyRunningError if said CSV is already being updated.
    """
    task_type = 'cmmedu_seguimiento_student_profile_csv'
    task_class = calculate_students_features_csv
    task_input = features
    task_key = "CMMEDU-SEGUIMIENTO-STUDENT-PROFILE-{}".format(str(course_key))

    return submit_task(request, task_type, task_class, course_key, task_input, task_key)


@task(base=BaseInstructorTask)
def calculate_students_features_csv(entry_id, xmodule_instance_args):
    """
    Compute student profile information for a course and upload the
    CSV to an S3 bucket for download.
    """
    # Translators: This is a past-tense verb that is inserted into task progress messages as {action}.
    action_name = ugettext_noop('generated')
    task_fn = partial(upload_students_csv, xmodule_instance_args)
    return run_main_task(entry_id, task_fn, action_name)