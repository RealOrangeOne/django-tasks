from .settings import *

TASKS = {"default": {"BACKEND": "django_tasks.backends.database.DatabaseBackend"}}

# Force the test DB to be used
if "sqlite" in DATABASES["default"]["ENGINE"]:
    DATABASES["default"]["NAME"] = DATABASES["default"]["TEST"]["NAME"]
else:
    DATABASES["default"]["NAME"] = "test_" + DATABASES["default"]["NAME"]
