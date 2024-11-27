from django.test import Client
from django.urls import reverse
from xmodule.modulestore.tests.django_utils import ModuleStoreTestCase
from xmodule.modulestore.tests.factories import CourseFactory, ItemFactory
from common.djangoapps.student.tests.factories import UserFactory, CourseEnrollmentFactory
from capa.tests.response_xml_factory import StringResponseXMLFactory
from lms.djangoapps.courseware.tests.factories import StudentModuleFactory
from six.moves import range


XBLOCK_COUNT = 10

USER_COUNT = 5

class TestCMMEduSeguimiento(ModuleStoreTestCase):

    def setUp(self):
        super(TestCMMEduSeguimiento, self).setUp()

        # Create clients
        self.non_auth_client = Client()
        self.auth_client = Client()
        self.user_staff = UserFactory(
            username='testuser3',
            password='12345',
            email='student2@edx.org',
            is_staff=True)
        self.auth_client.login(username='testuser3', password='12345')

        # Create a course
        self.course1 = CourseFactory.create(org='mss', course='100', run='2020', display_name='Sample course 1')

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

        # Create users and enroll them in the course
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


    def test_endpoints_authentication(self):
        """
        Test that the endpoints require authentication.
        """
        response1 = self.non_auth_client.post(reverse('cmmedu_seguimiento:cmmedu_seguimiento_get_report'), data={})
        self.assertEqual(response1.status_code, 401)
        response2 = self.non_auth_client.post(reverse('cmmedu_seguimiento:cmmedu_seguimiento_make_report'), data={})
        self.assertEqual(response2.status_code, 401)


    def test_no_task_created_if_not_course_key(self):
        """
        Test that no task is created if the course key is missing.
        """
        response = self.auth_client.post(reverse('cmmedu_seguimiento:cmmedu_seguimiento_make_report'), content_type="application/json", data={})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.content, b'Missing course_key')


    def test_no_task_created_if_bad_course_key(self):
        """
        Test that no task is created if the course key is missing.
        """
        response = self.auth_client.post(reverse('cmmedu_seguimiento:cmmedu_seguimiento_make_report'), content_type="application/json", data={"course_key": "BAD_COURSE_KEY"})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.content, b'Invalid course_key')


    def test_task_created(self):
        """
        Test that a task is created when the course key is valid.
        """
        response = self.auth_client.post(reverse('cmmedu_seguimiento:cmmedu_seguimiento_make_report'), content_type="application/json", data={"course_key": "course-v1:mss+100+2020"})
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        self.assertEqual(response_json['status'], 1)
        self.assertEqual(response_json['msg'], 'Se ha iniciado la generación del reporte.')
        self.assertIn('task_id', response_json)


    def test_get_report_no_course_key(self):
        """
        Test that no report is returned if the course key is missing.
        """
        response = self.auth_client.post(reverse('cmmedu_seguimiento:cmmedu_seguimiento_get_report'), content_type="application/json", data={})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.content, b'Missing course_key')


    def test_get_report_bad_course_key(self):
        """
        Test that no report is returned if the course key is missing.
        """
        response = self.auth_client.post(reverse('cmmedu_seguimiento:cmmedu_seguimiento_get_report'), content_type="application/json", data={"course_key": "BAD_COURSE_KEY"})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.content, b'Invalid course_key')

    
    def test_get_report_no_report(self):
        """
        Test that no report is returned if no task has been created.
        """
        response = self.auth_client.post(reverse('cmmedu_seguimiento:cmmedu_seguimiento_get_report'), content_type="application/json", data={"course_key": "course-v1:mss+100+2020"})
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        self.assertEqual(response_json['status'], 0)
        self.assertEqual(response_json['msg'], 'No hay tareas de reportes asociadas a este curso.')


    def test_task_create(self):
        """
        Test that a task is created when the course key is valid.
        """
        response1 = self.auth_client.post(reverse('cmmedu_seguimiento:cmmedu_seguimiento_make_report'), content_type="application/json", data={"course_key": "course-v1:mss+100+2020"})
        self.assertEqual(response1.status_code, 200)
        response1_json = response1.json()
        self.assertEqual(response1_json['status'], 1)
        self.assertEqual(response1_json['msg'], 'Se ha iniciado la generación del reporte.')
        self.assertIn('task_id', response1_json)

