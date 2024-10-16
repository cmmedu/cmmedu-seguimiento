from django.db import transaction
from django.http import HttpResponseBadRequest, JsonResponse
from edx_rest_framework_extensions import permissions
from edx_rest_framework_extensions.auth.jwt.authentication import JwtAuthentication
from edx_rest_framework_extensions.auth.session.authentication import SessionAuthenticationAllowInactiveUser
import json
from lms.djangoapps.instructor_task.api_helper import AlreadyRunningError
import logging
from opaque_keys import InvalidKeyError
from opaque_keys.edx.keys import CourseKey
from openedx.core.lib.api.authentication import BearerAuthenticationAllowInactiveUser
from rest_framework.views import APIView

from .models import JsonReportStore
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
            _ = CourseKey.from_string(course_key)
        except InvalidKeyError:
            return HttpResponseBadRequest("Invalid course_key")
        task_input = {
            'user_id': request.user.pk
        }
        try:
            task = submit_task_make_report(request, course_key, task_input)
            return JsonResponse({"status": 1, "msg": 'Se ha iniciado la generación del reporte.', 'task_id': task.task_id})
        except AlreadyRunningError:
            return JsonResponse({"status": 0, "msg": "Esta tarea ya está en progreso."})


class CMMEduSeguimientoGetReport(APIView):

    authentication_classes = (
        JwtAuthentication,
        BearerAuthenticationAllowInactiveUser,
        SessionAuthenticationAllowInactiveUser,
    )

    permission_classes = (permissions.JWT_RESTRICTED_APPLICATION_OR_USER_ACCESS,)

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
        report_store = JsonReportStore.from_config(config_name='GRADES_DOWNLOAD')
        reports = [url for name, url in report_store.links_for(key) if ".json" in name and "report_data" in name]
        if len(reports) > 0:
            url = max(reports)
            timestamp = url.split("_report_data_")[-1].split(".json")[0]
            return JsonResponse({"status": 1, "msg": "Reporte encontrado.", "course_key": course_key, "timestamp": timestamp, "report_url": max(reports)})
        else:
            return JsonResponse({"status": 0, "msg": "No se ha encontrado el reporte."})

