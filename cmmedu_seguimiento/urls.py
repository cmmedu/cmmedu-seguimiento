from django.conf.urls import url
from django.views.decorators.csrf import csrf_exempt
from .views import *


urlpatterns = [
    url('cmmedu_seguimiento_student_profile/', csrf_exempt(CMMEduSeguimientoStudentProfile.as_view()), name='cmmedu_seguimiento_student_profile')
]