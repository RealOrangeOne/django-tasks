import os

import dj_database_url

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "django_tasks",
    "django_tasks.backends.database",
    "tests",
]

# TEMPLATES = [
#     {
#         "BACKEND": "django.template.backends.django.DjangoTemplates",
#         "DIRS": [],
#         "APP_DIRS": True,
#         "OPTIONS": {
#             "context_processors": [
#                 "django.template.context_processors.debug",
#                 "django.template.context_processors.request",
#                 "django.contrib.auth.context_processors.auth",
#                 "django.contrib.messages.context_processors.messages",
#             ]
#         },
#     },
# ]

SECRET_KEY = "abcde12345"

ROOT_URLCONF = "tests.urls"

DEFAULT_AUTO_FIELD = "django.db.models.AutoField"

DATABASES = {
    "default": dj_database_url.config(
        default="sqlite:///" + os.path.join(BASE_DIR, "db.sqlite3")
    )
}

USE_TZ = True
