from django.db import transaction
from django.http import HttpResponseBadRequest, JsonResponse
from edx_rest_framework_extensions import permissions
from edx_rest_framework_extensions.auth.jwt.authentication import JwtAuthentication
from edx_rest_framework_extensions.auth.session.authentication import SessionAuthenticationAllowInactiveUser
import json
from lms.djangoapps.instructor_task.api_helper import AlreadyRunningError
from lms.djangoapps.instructor_task.models import InstructorTask
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
        course_tasks = InstructorTask.objects.filter(
            task_type='cmmedu_seguimiento_report',
            course_id=course_key
        ).order_by('-created').all()
        if not course_tasks:
            return JsonResponse({"status": 0, "msg": "No hay tareas de reportes asociadas a este curso."})
        latest_task = course_tasks[0]
        if latest_task.task_state == 'PROGRESS':
            return JsonResponse({"status": 0, "msg": "La tarea de reportes aún no está lista."})
        elif latest_task.task_state == 'FAILED':
            return JsonResponse({"status": 0, "msg": "La tarea de reportes ha fallado."})
        elif latest_task.task_state == 'SUCCESS':
            task_output = json.loads(latest_task.task_output)
            logger.info("Task output: %s", task_output)
            try:
                report_names = task_output.get('reports')
                student_profile_report_name = report_names.get('student_profile')
                ora_report_name = report_names.get('ora_data')
                block_report_names = report_names.get('blocks_data')
            except:
                return JsonResponse({"status": 0, "msg": "Formato de output de tarea inválido."})
            report_store = JsonReportStore.from_config(config_name='GRADES_DOWNLOAD')
            output = {
                'student_profile': None,
                'ora_data': None,
                'blocks_data': {},
                'task_started': latest_task.created.isoformat(),
                'task_finished': latest_task.updated.isoformat(),
                'task_duration_seconds': (latest_task.updated - latest_task.created).total_seconds()
            }
            for name, url in report_store.links_for(key):
                if name == student_profile_report_name:
                    output['student_profile'] = url
                elif name == ora_report_name:
                    output['ora_data'] = url
                elif name in block_report_names:
                    output['blocks_data'][name.split("report_data_")[1].split("_")[0]] = url
            return JsonResponse({"status": 1, "msg": "Reporte encontrado.", "course_key": course_key, "output": output})
        else:
            return JsonResponse({"status": 0, "msg": "Estado de la tarea desconocido."})
