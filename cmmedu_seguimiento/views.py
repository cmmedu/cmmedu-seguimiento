from django.conf import settings
from django.db import transaction
from django.db.utils import IntegrityError
from django.http import HttpResponseRedirect, HttpResponseBadRequest, HttpResponse, JsonResponse
from django.views.generic.base import View
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
from xmodule.modulestore.django import modulestore

from .tasks import submit_task_make_reports


logger = logging.getLogger(__name__)


class CMMEduSeguimientoMakeReports(APIView):

    authentication_classes = (
        JwtAuthentication,
        BearerAuthenticationAllowInactiveUser,
        SessionAuthenticationAllowInactiveUser,
    )

    permission_classes = (permissions.JWT_RESTRICTED_APPLICATION_OR_USER_ACCESS,)

    @transaction.non_atomic_requests
    def dispatch(self, args, **kwargs):
        return super(CMMEduSeguimientoMakeReports, self).dispatch(args, **kwargs)

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

        query_features_names = {
            'id': _('User ID'),
            'username': _('Username'),
            'name': _('Name'),
            'email': _('Email'),
            'language': _('Language'),
            'location': _('Location'),
            'year_of_birth': _('Birth Year'),
            'gender': _('Gender'),
            'level_of_education': _('Level of Education'),
            'mailing_address': _('Mailing Address'),
            'goals': _('Goals'),
            'enrollment_mode': _('Enrollment Mode'),
            'verification_status': _('Verification Status'),
            'last_login': _('Last Login'),
            'date_joined': _('Date Joined'),
        }

        if is_course_cohorted(course.id):
            # Translators: 'Cohort' refers to a group of students within a course.
            query_features.append('cohort')
            query_features_names['cohort'] = _('Cohort')

        if course.teams_enabled:
            query_features.append('team')
            query_features_names['team'] = _('Team')

        # For compatibility reasons, city and country should always appear last.
        query_features.append('city')
        query_features_names['city'] = _('City')
        query_features.append('country')
        query_features_names['country'] = _('Country')

        try:
            task = submit_task_make_reports(request, course_key, query_features)
            success_status = 'El reporte Perfil de estudiantes está siendo creado.'
            return JsonResponse({"status": success_status, 'task_id': task.task_id})
        except AlreadyRunningError:
            return JsonResponse({"status": "Esta tarea ya está en progreso."})