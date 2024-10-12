from django.conf.urls import url
from django.views.decorators.csrf import csrf_exempt
from .views import *


urlpatterns = [
    url('cmmedu_seguimiento_report/', csrf_exempt(CMMEduSeguimientoReport.as_view()), name='cmmedu_seguimiento_report')
]