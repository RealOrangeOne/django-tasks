import os
import sys

import dj_database_url

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

IN_TEST = sys.argv[1] == "test"

ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.messages",
    "django.contrib.sessions",
    "django.contrib.staticfiles",
    "django_tasks",
    "django_tasks.backends.database",
    "tests",
]

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
                "django.template.context_processors.static",
            ]
        },
    },
]

MIDDLEWARE = [
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
]

STATIC_URL = "/static/"

SECRET_KEY = "abcde12345"

ROOT_URLCONF = "tests.urls"

DEFAULT_AUTO_FIELD = "django.db.models.AutoField"

DATABASES = {
    "default": dj_database_url.config(
        default="sqlite://:memory:"
        if IN_TEST
        else "sqlite:///" + os.path.join(BASE_DIR, "db.sqlite3")
    )
}


USE_TZ = True

if not IN_TEST:
    DEBUG = True
    TASKS = {"default": {"BACKEND": "django_tasks.backends.database.DatabaseBackend"}}
