#!/bin/dash

pip install -e /openedx/requirements/cmmedu-seguimiento

cd /openedx/requirements/cmmedu-seguimiento
cp /openedx/edx-platform/setup.cfg .
mkdir test_root
cd test_root/
ln -s /openedx/staticfiles .

cd /openedx/requirements/cmmedu-seguimiento

DJANGO_SETTINGS_MODULE=lms.envs.test EDXAPP_TEST_MONGO_HOST=mongodb pytest cmmedu_seguimiento/tests.py