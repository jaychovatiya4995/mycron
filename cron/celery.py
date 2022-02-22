from __future__ import absolute_import
import os
from celery import Celery
from django.conf import settings
from celery.schedules import crontab

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'cron.settings')
app = Celery('cron')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()

# os.environ.setdefault('DJANGO_SETTINGS_MODULE','cron.settings')
# app = Celery('cron')
# app.config_from_object('django.conf:settings')
# app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)

# Celery Beat Settings
# app.conf.beat_schedule = {
#   "sample_task": {
#         "task": "atlas.tasks.process",
#         "schedule": crontab(minute="*/1"),
#     },
# }

@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))