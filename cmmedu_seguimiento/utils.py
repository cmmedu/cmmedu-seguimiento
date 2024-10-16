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
from lms.djangoapps.instructor_task.tasks_helper.runner import TaskProgress
from opaque_keys.edx.keys import UsageKey
from openassessment.data import OraAggregateData
from pytz import UTC
from time import time
from xmodule.modulestore.django import modulestore

from .models import JsonReportStore


REPORT_REQUESTED_EVENT_NAME = u'edx.instructor.report.requested'


def make_report(_xmodule_instance_args, _entry_id, course_id, task_input, action_name):
    """
    For a given `course_id`, generate a JSON file containing profile
    information, ORA data, blocks data and student state for all students 
    that are enrolled, and store using a `JsonReportStore`.
    """
    start_time = time()
    start_date = datetime.now(UTC)
    enrolled_students = CourseEnrollment.objects.users_enrolled_in(course_id)
    problem_locations = "block-v1:{}+type@course+block@course".format(course_id)
    task_progress = TaskProgress(action_name, enrolled_students.count(), start_time)

    current_step = {'step': 'Generating report data...'}
    task_progress.update_task_state(extra_meta=current_step)
    report_data = {}

    # Student profile
    if settings.UCHILEEDXLOGIN_TASK_RUN_ENABLE:
        task_input["student_features"].insert(0,'run')
    query_features = task_input["student_features"]
    report_data["student_profile"] = enrolled_students_features(course_id, query_features)

    # ORA data
    header, datarows = OraAggregateData.collect_ora2_data(course_id)
    report_data["ora_data"] = [dict(zip(header, row)) for row in datarows]

    # Blocks and student state
    report_data["blocks"] = build_blocks_data(
        user_id=task_input["user_id"],
        course_key=course_id,
        usage_key_str=problem_locations,
    )

    current_step = {'step': 'Uploading report data JSON...'}
    task_progress.update_task_state(extra_meta=current_step)
    report_name = upload_json_to_report_store(report_data, 'report_data', course_id, start_date)

    current_step = {
        'step': 'Report ready.',
        'report_name': report_name
    }
    return task_progress.update_task_state(extra_meta=current_step)


def build_blocks_data(user_id, course_key, usage_key_str):
    blocks_data = []
    usage_key = UsageKey.from_string(usage_key_str).map_into_course(course_key)
    user = get_user_model().objects.get(pk=user_id)
    store = modulestore()
    with store.bulk_operations(course_key):
        course_blocks = get_course_blocks(user, usage_key)
        for title, path, block_key in build_problem_list(course_blocks, usage_key):

            # Course, chapter, sequential and vertical blocks are filtered out since they include state
            # which isn't useful for this report.
            if block_key.block_type in ('course', 'sequential', 'chapter', 'vertical'):
                continue

            # Store basic data from the block
            block = store.get_item(block_key)
            block_item = {
                "title": title,
                "path": path,
                "display_name": block.display_name,
                "block_type": block_key.block_type,
                "block_id": str(block_key).split('@')[-1]
            }

            # Iterate over the dictionary and store key-value pairs after "source_file", depending of the block type
            fields = block.fields
            found_source_file = False
            for key in fields.keys():
                if found_source_file:
                    block_item[key] = block.fields[key].read_from(block)
                if key == "source_file":
                    found_source_file = True

            # Add students data
            # DATA HERE

            # Append the block data to the list
            blocks_data.append(block_item)

    return blocks_data


def upload_json_to_report_store(data, json_name, course_id, timestamp, config_name='GRADES_DOWNLOAD'):
    """
    Upload data as a JSON using ReportStore.

    Arguments:
        data: JSON data
        json_name: Name of the resulting JSON
        course_id: ID of the course
    """
    report_store = JsonReportStore.from_config(config_name)
    report_name = u"{course_prefix}_{json_name}_{timestamp_str}.json".format(
        course_prefix=course_filename_prefix_generator(course_id),
        json_name=json_name,
        timestamp_str=timestamp.strftime("%Y-%m-%d-%H%M")
    )

    report_store.store_json(course_id, report_name, data)
    tracker_emit(json_name)
    return report_name


def tracker_emit(report_name):
    """
    Emits a 'report.requested' event for the given report.
    """
    tracker.emit(REPORT_REQUESTED_EVENT_NAME, {"report_type": report_name, })


def build_student_data(user_id, course_key, usage_key_str, filter_types=None):
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
    usage_key = UsageKey.from_string(usage_key_str).map_into_course(course_key)
    user = get_user_model().objects.get(pk=user_id)

    student_data = []
    max_count = settings.FEATURES.get('MAX_PROBLEM_RESPONSES_COUNT')

    store = modulestore()
    user_state_client = DjangoXBlockUserStateClient()

    student_data_keys = set()

    with store.bulk_operations(course_key):
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