from collections import defaultdict, namedtuple
from common.djangoapps.student.models import CourseEnrollment
from common.djangoapps.util.file import course_filename_prefix_generator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from django.conf import settings
from django.contrib.auth import get_user_model
from eventtracking import tracker
import json
from lms.djangoapps.course_blocks.api import get_course_blocks
from lms.djangoapps.courseware.courses import get_course_by_id
from lms.djangoapps.courseware.models import StudentModule
from lms.djangoapps.instructor_analytics.basic import enrolled_students_features, get_response_state
from lms.djangoapps.instructor_task.tasks_helper.runner import TaskProgress
import logging
from opaque_keys.edx.keys import UsageKey
from openassessment.data import OraAggregateData
from openedx.core.djangoapps.course_groups.cohorts import is_course_cohorted
from openedx.core.djangoapps.site_configuration import helpers as configuration_helpers
from pytz import UTC
from time import time
from xmodule.modulestore.django import modulestore

import sys

from .models import JsonReportStore


logger = logging.getLogger(__name__)

REPORT_REQUESTED_EVENT_NAME = u'edx.instructor.report.requested'

UserState = namedtuple('UserState', ['username', 'state'])


def make_report(_xmodule_instance_args, _entry_id, course_id, task_input, action_name):
    """
    For a given `course_id`, generate a JSON file containing profile
    information, ORA data, blocks data and student state for all students
    that are enrolled, and store using a `JsonReportStore`.
    """
    start_time = time()
    start_date = datetime.now(UTC)

    enrolled_students = CourseEnrollment.objects.users_enrolled_in(course_id)
    course = get_course_by_id(course_id)
    problem_locations = str(course.location)
    task_progress = TaskProgress(action_name, enrolled_students.count(), start_time)

    current_step = {'step': 'Generating report data...'}
    task_progress.update_task_state(extra_meta=current_step)
    logger.info("Started data generation for course %s.", course_id)

    # Student profile
    query_features = list(configuration_helpers.get_value('student_profile_download_fields', []))
    if not query_features:
        query_features = [
            'id', 'username', 'name', 'email', 'language', 'location',
            'year_of_birth', 'gender', 'level_of_education', 'mailing_address',
            'goals', 'enrollment_mode', 'verification_status',
            'last_login', 'date_joined',
        ]
    if is_course_cohorted(course.id):
        query_features.append('cohort')
    if course.teams_enabled:
        query_features.append('team')
    query_features.append('city')
    query_features.append('country')
    if settings.UCHILEEDXLOGIN_TASK_RUN_ENABLE:
        query_features.insert(0,'run')
    student_profile_data = enrolled_students_features(course_id, query_features)
    student_profile_report_name = upload_json_to_report_store(student_profile_data, 'student_profile', course_id, start_date)
    logger.info("Stored student profile data.")

    # ORA data
    header, datarows = OraAggregateData.collect_ora2_data(course_id)
    ora_data = [dict(zip(header, row)) for row in datarows]
    ora_report_name = upload_json_to_report_store(ora_data, 'ora_data', course_id, start_date)
    logger.info("Stored ORA data.")

    # Blocks and student state
    report_names = build_blocks_data(
        user_id=task_input["user_id"],
        course_key=course_id,
        usage_key_str=problem_locations,
        start_date=start_date
    )

    current_step = {
        'step': 'Report ready.',
        'course_key': student_profile_report_name.split("_student_profile")[0],
        'timestamp': student_profile_report_name.split("_")[-1].split(".")[0],
        'n_reports': len(report_names)
    }

    return task_progress.update_task_state(extra_meta=current_step)


def _iter_user_states(sm_records):
    """Yield UserState namedtuples from already-fetched StudentModule records.

    capa_module.generate_report_data accesses user_state.state as an attribute,
    so plain tuples don't work here.
    """
    for sm in sm_records:
        try:
            state = json.loads(sm.state) if sm.state else {}
        except (json.JSONDecodeError, TypeError):
            state = {}
        yield UserState(username=sm.student.username, state=state)


def build_blocks_data(user_id, course_key, usage_key_str, start_date):
    usage_key = UsageKey.from_string(usage_key_str).map_into_course(course_key)
    user = get_user_model().objects.get(pk=user_id)
    store = modulestore()
    max_count = settings.FEATURES.get('MAX_PROBLEM_RESPONSES_COUNT')

    with store.bulk_operations(course_key):
        course_blocks = get_course_blocks(user, usage_key)

        # Pass 1: group blocks by section, preserving traversal order
        sections = []
        current_section = ""
        current_items = []
        for title, path, block_key in build_problem_list(course_blocks, usage_key):
            if len(path) < 2:
                continue
            new_section = path[1]
            if new_section != current_section:
                if current_section:
                    sections.append((current_section, current_items))
                    current_items = []
                current_section = new_section
            current_items.append((title, path, block_key))
        if current_items:
            sections.append((current_section, current_items))

        reports = []
        pending_future = None

        with ThreadPoolExecutor(max_workers=1) as upload_executor:
            for section_name, section_items in sections:
                # Batch fetch all StudentModule records for this section in one query
                section_block_keys = [bk for _, _, bk in section_items]
                sm_by_block = defaultdict(list)
                for sm in StudentModule.objects.filter(
                    course_id=course_key,
                    module_state_key__in=section_block_keys,
                ).select_related('student').order_by('student'):
                    sm_by_block[sm.module_state_key].append(sm)

                blocks_data = []
                block_count = 0
                response_count = 0

                for title, path, block_key in section_items:
                    if block_key.block_type == 'course':
                        continue
                    if block_key.block_type in ('sequential', 'chapter', 'vertical'):
                        blocks_data.append({
                            "path": path,
                            "block_type": block_key.block_type,
                            "block_id": str(block_key).split('@')[-1],
                            "is_structural_item": True,
                        })
                        continue

                    block = store.get_item(block_key)
                    block_item = {
                        "title": title,
                        "path": path,
                        "display_name": block.display_name,
                        "block_type": block_key.block_type,
                        "block_id": str(block_key).split('@')[-1],
                        "is_structural_item": False,
                    }

                    found_source_file = False
                    for key in block.fields.keys():
                        if found_source_file:
                            block_item[key] = block.fields[key].read_from(block)
                        if key == "source_file":
                            found_source_file = True

                    sm_records = sm_by_block.get(block_key, [])
                    if max_count:
                        sm_records = sm_records[:max_count]

                    generated_report_data = defaultdict(list)
                    if hasattr(block, 'generate_report_data'):
                        try:
                            for username, state in block.generate_report_data(_iter_user_states(sm_records), max_count):
                                generated_report_data[username].append(state)
                        except NotImplementedError:
                            pass
                        except Exception:
                            logger.warning("Error generating report data for block %s.", block_key, exc_info=sys.exc_info())

                    responses = []
                    for sm in sm_records:
                        username = sm.student.username
                        base_response = {
                            'username': username,
                            'timestamp': sm.created,
                            'state': get_response_state(sm),
                        }
                        user_states = generated_report_data.get(username)
                        if user_states:
                            for user_state in user_states:
                                r = base_response.copy()
                                r.update(user_state)
                                responses.append(r)
                        else:
                            responses.append(base_response)
                        response_count += 1

                    block_item["responses"] = responses
                    blocks_data.append(block_item)
                    block_count += 1

                # Wait for the previous section's upload before submitting this one
                # (keeps at most one section's data in-flight, bounding peak RAM)
                if pending_future is not None:
                    reports.append(pending_future.result())

                index = len(reports) + 1
                pending_future = upload_executor.submit(
                    upload_json_to_report_store,
                    blocks_data,
                    'report_data_' + str(index),
                    course_key,
                    start_date,
                )
                blocks_data = []
                logger.info("Queued upload for section %s: %d blocks, %d responses.", section_name, block_count, response_count)

            if pending_future is not None:
                reports.append(pending_future.result())

    return reports


def upload_json_to_report_store(data, json_name, course_id, timestamp, config_name='GRADES_DOWNLOAD'):
    """
    Upload data as a JSON using ReportStore.

    Arguments:
        data: JSON data
        json_name: Name of the resulting JSON
        course_id: ID of the course
    """
    report_store = JsonReportStore.from_config(config_name)
    report_name = u"{course_prefix}_{json_name}_{timestamp_str}.tar.gz".format(
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


def list_problem_responses(course_key, problem_location, limit_responses=None):
    """
    Return responses to a given problem as a dict.

    list_problem_responses(course_key, problem_location)

    would return [
        {'username': u'user1', 'timestamp': u'...', 'state': u'...'},
        {'username': u'user2', 'timestamp': u'...', 'state': u'...'},
        {'username': u'user3', 'timestamp': u'...', 'state': u'...'},
    ]

    where `state` represents a student's response to the problem
    identified by `problem_location`.
    """
    if isinstance(problem_location, UsageKey):
        problem_key = problem_location
    else:
        problem_key = UsageKey.from_string(problem_location)
    run = problem_key.run
    if not run:
        problem_key = UsageKey.from_string(problem_location).map_into_course(course_key)
    if problem_key.course_key != course_key:
        return []

    smdat = StudentModule.objects.filter(
        course_id=course_key,
        module_state_key=problem_key
    )
    smdat = smdat.order_by('student')
    if limit_responses is not None:
        smdat = smdat[:limit_responses]

    return [
        {'username': response.student.username, 'timestamp': response.created, 'state': get_response_state(response)}
        for response in smdat
    ]
