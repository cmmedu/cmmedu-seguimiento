from django.db import transaction
from django.http import HttpResponseBadRequest, JsonResponse
from django.utils.translation import ugettext as _
from edx_rest_framework_extensions import permissions
from edx_rest_framework_extensions.auth.jwt.authentication import JwtAuthentication
from edx_rest_framework_extensions.auth.session.authentication import SessionAuthenticationAllowInactiveUser
import json
from lms.djangoapps.courseware.courses import get_course_by_id
from lms.djangoapps.instructor_task.api_helper import AlreadyRunningError
import logging
from opaque_keys import InvalidKeyError
from opaque_keys.edx.keys import CourseKey
from openedx.core.djangoapps.course_groups.cohorts import is_course_cohorted
from openedx.core.djangoapps.site_configuration import helpers as configuration_helpers
from openedx.core.lib.api.authentication import BearerAuthenticationAllowInactiveUser
from rest_framework.views import APIView

from .tasks import submit_task_make_report


logger = logging.getLogger(__name__)


class CMMEduSeguimientoMakeReport(APIView):

    authentication_classes = (
        JwtAuthentication,
        BearerAuthenticationAllowInactiveUser,
        SessionAuthenticationAllowInactiveUser,
    )

    permission_classes = (permissions.JWT_RESTRICTED_APPLICATION_OR_USER_ACCESS,)


    @transaction.non_atomic_requests
    def dispatch(self, args, **kwargs):
        return super(CMMEduSeguimientoMakeReport, self).dispatch(args, **kwargs)


    def post(self, request):
        try:
            data = json.loads(request.body)
        except json.JSONDecodeError:
            return HttpResponseBadRequest("Invalid JSON")
        course_key = data.get('course_key')
        if not course_key:
            return HttpResponseBadRequest("Missing course_key")
        try:
            key = CourseKey.from_string(course_key)
        except InvalidKeyError:
            return HttpResponseBadRequest("Invalid course_key")
        course = get_course_by_id(key)
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

        task_input = {
            'user_id': request.user.pk,
            'student_features': query_features,
        }
        try:
            task = submit_task_make_report(request, course_key, task_input)
            success_status = 'Se ha iniciado la generación del reporte.'
            return JsonResponse({"status": success_status, 'task_id': task.task_id})
        except AlreadyRunningError:
            return JsonResponse({"status": "Esta tarea ya está en progreso."})
