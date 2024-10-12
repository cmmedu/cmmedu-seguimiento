from django.conf import settings
from django.contrib.auth import logout
from django.db.utils import IntegrityError
from django.http import HttpResponseRedirect, HttpResponseBadRequest, HttpResponse, JsonResponse
from django.views.generic.base import View
from edx_rest_framework_extensions import permissions
from edx_rest_framework_extensions.auth.jwt.authentication import JwtAuthentication
from edx_rest_framework_extensions.auth.session.authentication import SessionAuthenticationAllowInactiveUser
import json
from lms.djangoapps.certificates.queue import XQueueCertInterface
import logging
from opaque_keys.edx.keys import CourseKey
from openedx.core.djangoapps.site_configuration import helpers as configuration_helpers
from openedx.core.lib.api.authentication import BearerAuthenticationAllowInactiveUser
from rest_framework.views import APIView
from xmodule.modulestore.django import modulestore


logger = logging.getLogger(__name__)


class CMMEduSeguimientoReport(APIView):

    pass