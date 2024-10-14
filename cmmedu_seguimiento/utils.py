from common.djangoapps.student.models import CourseEnrollment
from common.djangoapps.util.file import course_filename_prefix_generator
from datetime import datetime
from django.conf import settings
from eventtracking import tracker
from lms.djangoapps.instructor_analytics.basic import enrolled_students_features
from lms.djangoapps.instructor_analytics.csvs import format_dictlist
from lms.djangoapps.instructor_task.models import ReportStore
from lms.djangoapps.instructor_task.tasks_helper.runner import TaskProgress
from openassessment.data import OraAggregateData
from pytz import UTC
from time import time


REPORT_REQUESTED_EVENT_NAME = u'edx.instructor.report.requested'



def make_reports(_xmodule_instance_args, _entry_id, course_id, task_input, action_name):
    """
    For a given `course_id`, generate CSV files containing profile
    information, student state and ORA data for all students that are 
    enrolled, and store using a `ReportStore`.
    """
    start_time = time()
    start_date = datetime.now(UTC)
    enrolled_students = CourseEnrollment.objects.users_enrolled_in(course_id)
    task_progress = TaskProgress(action_name, enrolled_students.count(), start_time)

    current_step = {'step': 'Generating student profile...'}
    task_progress.update_task_state(extra_meta=current_step)
    if settings.UCHILEEDXLOGIN_TASK_RUN_ENABLE:
        task_input.insert(0,'run')
    query_features = task_input
    student_data = enrolled_students_features(course_id, query_features)
    header, rows = format_dictlist(student_data, query_features)
    task_progress.attempted = task_progress.succeeded = len(rows)
    task_progress.skipped = task_progress.total - task_progress.attempted
    rows.insert(0, header)

    current_step = {'step': 'Uploading student profile CSV...'}
    task_progress.update_task_state(extra_meta=current_step)
    student_profile_name = upload_csv_to_report_store(rows, 'student_profile_info', course_id, start_date)
    
    current_step = {'step': 'Generating ORA data...'}
    task_progress.update_task_state(extra_meta=current_step)
    header, datarows = OraAggregateData.collect_ora2_data(course_id)
    rows = [header] + [row for row in datarows]

    current_step = {'step': 'Uploading ORA data CSV...'}
    task_progress.update_task_state(extra_meta=current_step)
    ora_data_name = upload_csv_to_report_store(rows, 'ORA_data', course_id, start_date)

    current_step = {'step': 'Generating student state...'}
    task_progress.update_task_state(extra_meta=current_step)
    location = "block-v1:{}+type@course+block@course".format(course_id)

    current_step = {
        'step': 'Reports ready.',
        'student_profile_name': student_profile_name,
        'ora_data_name': ora_data_name,
        #'student_state_name': student_state_name,
    }
    return task_progress.update_task_state(extra_meta=current_step)


def upload_csv_to_report_store(rows, csv_name, course_id, timestamp, config_name='GRADES_DOWNLOAD'):
    """
    Upload data as a CSV using ReportStore.

    Arguments:
        rows: CSV data in the following format (first column may be a
            header):
            [
                [row1_colum1, row1_colum2, ...],
                ...
            ]
        csv_name: Name of the resulting CSV
        course_id: ID of the course

    Returns:
        report_name: string - Name of the generated report
    """
    report_store = ReportStore.from_config(config_name)
    report_name = u"{course_prefix}_{csv_name}_{timestamp_str}.csv".format(
        course_prefix=course_filename_prefix_generator(course_id),
        csv_name=csv_name,
        timestamp_str=timestamp.strftime("%Y-%m-%d-%H%M")
    )

    report_store.store_rows(course_id, report_name, rows)
    tracker_emit(csv_name)
    return report_name


def tracker_emit(report_name):
    """
    Emits a 'report.requested' event for the given report.
    """
    tracker.emit(REPORT_REQUESTED_EVENT_NAME, {"report_type": report_name, })