from .settings import *

# Unset custom test settings to use in-memory DB
if "sqlite" in DATABASES["default"]["ENGINE"]:
    del DATABASES["default"]["TEST"]
