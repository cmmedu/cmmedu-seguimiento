from collections import defaultdict
from common.djangoapps.student.models import CourseEnrollment
from common.djangoapps.util.file import course_filename_prefix_generator
from datetime import datetime
from django.conf import settings
from django.contrib.auth import get_user_model
from eventtracking import tracker
from lms.djangoapps.course_blocks.api import get_course_blocks
from lms.djangoapps.courseware.user_state_client import DjangoXBlockUserStateClient
from lms.djangoapps.instructor_analytics.basic import enrolled_students_features, list_problem_responses
from lms.djangoapps.instructor_analytics.csvs import format_dictlist
from lms.djangoapps.instructor_task.models import ReportStore
from lms.djangoapps.instructor_task.tasks_helper.runner import TaskProgress
from opaque_keys.edx.keys import UsageKey
from openassessment.data import OraAggregateData
from pytz import UTC
from time import time
from xmodule.modulestore.django import modulestore


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
        task_input["student_features"].insert(0,'run')
    query_features = task_input["student_features"]
    student_profile = enrolled_students_features(course_id, query_features)
    header, rows = format_dictlist(student_profile, query_features)
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
    problem_locations = ["block-v1:{}+type@course+block@course".format(course_id)]
    filter_types = None
    student_data, student_data_keys = build_student_data(
        user_id=task_input["user_id"],
        course_key=course_id,
        usage_key_str_list=problem_locations,
        filter_types=filter_types,
    )
    for data in student_data:
        for key in student_data_keys:
            data.setdefault(key, '')
    header, rows = format_dictlist(student_data, student_data_keys)
    rows.insert(0, header)

    current_step = {'step': 'Uploading student state CSV...'}
    task_progress.update_task_state(extra_meta=current_step)
    student_state_name = upload_csv_to_report_store(rows, 'student_state', course_id, start_date)

    current_step = {
        'step': 'Reports ready.',
        'student_profile_name': student_profile_name,
        'ora_data_name': ora_data_name,
        'student_state_name': student_state_name
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


def build_student_data(user_id, course_key, usage_key_str_list, filter_types=None):
    """
    Generate a list of problem responses for all problem under the
    ``problem_location`` root.
    Arguments:
        user_id (int): The user id for the user generating the report
        course_key (CourseKey): The ``CourseKey`` for the course whose report
            is being generated
        usage_key_str_list (List[str]): The generated report will include these
            blocks and their child blocks.
        filter_types (List[str]): The report generator will only include data for
            block types in this list.
    Returns:
            Tuple[List[Dict], List[str]]: Returns a list of dictionaries
            containing the student data which will be included in the
            final csv, and the features/keys to include in that CSV.
    """
    usage_keys = [
        UsageKey.from_string(usage_key_str).map_into_course(course_key)
        for usage_key_str in usage_key_str_list
    ]
    user = get_user_model().objects.get(pk=user_id)

    student_data = []
    max_count = settings.FEATURES.get('MAX_PROBLEM_RESPONSES_COUNT')

    store = modulestore()
    user_state_client = DjangoXBlockUserStateClient()

    student_data_keys = set()

    printing = True

    with store.bulk_operations(course_key):
        for usage_key in usage_keys:
            if max_count is not None and max_count <= 0:
                break
            course_blocks = get_course_blocks(user, usage_key)
            base_path = build_block_base_path(store.get_item(usage_key))
            for title, path, block_key in build_problem_list(course_blocks, usage_key):
                # Chapter and sequential blocks are filtered out since they include state
                # which isn't useful for this report.
                if block_key.block_type in ('sequential', 'chapter'):
                    continue

                if filter_types is not None and block_key.block_type not in filter_types:
                    continue

                block = store.get_item(block_key)
                if printing:
                    #if block.display_name == "Iterative XBlock":
                    fields = block.fields

                    # Flag to track when we find "source_file"
                    found_source_file = False

                    # Iterate over the dictionary and print key-value pairs after "source_file"
                    print("-------------------------------------------------")
                    print(f"Block: {block.display_name}")
                    for key, value in fields.items():
                        if found_source_file:
                            print(f"{key}: {block.fields[key].read_from(block)}")
                        if key == "source_file":
                            found_source_file = True
                generated_report_data = defaultdict(list)

                # Blocks can implement the generate_report_data method to provide their own
                # human-readable formatting for user state.
                if hasattr(block, 'generate_report_data'):
                    try:
                        user_state_iterator = user_state_client.iter_all_for_block(block_key)
                        for username, state in block.generate_report_data(user_state_iterator, max_count):
                            generated_report_data[username].append(state)
                    except NotImplementedError:
                        pass

                responses = []

                for response in list_problem_responses(course_key, block_key, max_count):
                    response['title'] = title
                    # A human-readable location for the current block
                    response['location'] = ' > '.join(base_path + path)
                    # A machine-friendly location for the current block
                    response['block_key'] = str(block_key)
                    # A block that has a single state per user can contain multiple responses
                    # within the same state.
                    user_states = generated_report_data.get(response['username'])
                    if user_states:
                        # For each response in the block, copy over the basic data like the
                        # title, location, block_key and state, and add in the responses
                        for user_state in user_states:
                            user_response = response.copy()
                            user_response.update(user_state)
                            student_data_keys = student_data_keys.union(list(user_state.keys()))
                            responses.append(user_response)
                    else:
                        responses.append(response)

                student_data += responses

                if max_count is not None:
                    max_count -= len(responses)
                    if max_count <= 0:
                        break

    # Keep the keys in a useful order, starting with username, title and location,
    # then the columns returned by the xblock report generator in sorted order and
    # finally end with the more machine friendly block_key and state.
    student_data_keys_list = (
        ['username', 'title', 'location'] +
        sorted(student_data_keys) +
        ['block_key', 'state']
    )

    return student_data, student_data_keys_list

def build_problem_list(course_blocks, root, path=None):
    """
    Generate a tuple of display names, block location paths and block keys
    for all problem blocks under the ``root`` block.
    Arguments:
        course_blocks (BlockStructureBlockData): Block structure for a course.
        root (UsageKey): This block and its children will be used to generate
            the problem list
        path (List[str]): The list of display names for the parent of root block
    Yields:
        Tuple[str, List[str], UsageKey]: tuple of a block's display name, path, and
            usage key
    """
    name = course_blocks.get_xblock_field(root, 'display_name') or root.block_type
    if path is None:
        path = [name]

    yield name, path, root

    for block in course_blocks.get_children(root):
        name = course_blocks.get_xblock_field(block, 'display_name') or block.block_type
        for result in build_problem_list(course_blocks, block, path + [name]):
            yield result


def build_block_base_path(block):
    """
    Return the display names of the blocks that lie above the supplied block in hierarchy.

    Arguments:
        block: a single block

    Returns:
        List[str]: a list of display names of blocks starting from the root block (Course)
    """
    path = []
    while block.parent:
        block = block.get_parent()
        path.append(block.display_name)
    return list(reversed(path))