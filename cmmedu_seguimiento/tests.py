from mock import patch, Mock
from django.test import TestCase, Client
from django.test.client import RequestFactory
from django.urls import reverse
from common.djangoapps.util.testing import UrlResetMixin
from xmodule.modulestore import ModuleStoreEnum
from xmodule.modulestore.tests.django_utils import ModuleStoreTestCase
from xmodule.modulestore.tests.factories import CourseFactory, ItemFactory
from common.djangoapps.student.tests.factories import UserFactory, CourseEnrollmentFactory
from capa.tests.response_xml_factory import StringResponseXMLFactory
from lms.djangoapps.courseware.tests.factories import StudentModuleFactory
from lms.djangoapps.grades.tasks import compute_all_grades_for_course as task_compute_all_grades_for_course
from opaque_keys.edx.keys import CourseKey
from lms.djangoapps.courseware.courses import get_course_with_access
from six import text_type
from six.moves import range

from . import views


XBLOCK_COUNT = 10

USER_COUNT = 5

class TestCMMEduSeguimiento(UrlResetMixin, ModuleStoreTestCase):

    def setUp(self):
        super(TestCMMEduSeguimiento, self).setUp()

        # Create OAuth credentials

        # Create a course
        self.course1 = CourseFactory.create(org='mss', course='100', display_name='Sample course 1')

        # Now give it some content
        with self.store.bulk_operations(self.course1.id, emit_signals=False):
            chapter = ItemFactory.create(
                parent_location=self.course1.location,
                category="sequential",
            )
            section = ItemFactory.create(
                parent_location=chapter.location,
                category="sequential",
                metadata={'graded': True, 'format': 'Homework'}
            )
            self.items = [
                ItemFactory.create(
                    parent_location=section.location,
                    category="problem",
                    data=StringResponseXMLFactory().build_xml(answer='foo'),
                    metadata={'rerandomize': 'always'}
                )
                for __ in range(XBLOCK_COUNT - 1)
            ]

        self.users = [UserFactory.create() for _ in range(USER_COUNT)]
        for user in self.users:
            CourseEnrollmentFactory.create(user=user, course_id=self.course1.id)
        for i, item in enumerate(self.items):
            for j, user in enumerate(self.users):
                StudentModuleFactory.create(
                    grade=1 if i < j else 0,
                    max_grade=1,
                    student=user,
                    course_id=self.course1.id,
                    module_state_key=item.location
                )
        