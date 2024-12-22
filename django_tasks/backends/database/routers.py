from django.conf import settings

class DynamicDatabaseRouter:
    """
    A database router that retrieves the app-to-database map from settings.py.
    """

    def get_db(self, app_label):
        """
        Retrieves the database for the given app label from settings.
        If the app is not found in the map, returns 'default' as a fallback.
        """
        app_db_map = getattr(settings, 'APP_TO_DB_MAP', {})
        return app_db_map.get(app_label, 'default')

    def db_for_read(self, model, **hints):
        """
        Directs read operations to the appropriate database.
        """
        return self.get_db(model._meta.app_label)

    def db_for_write(self, model, **hints):
        """
        Directs write operations to the appropriate database.
        """
        return self.get_db(model._meta.app_label)

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        Ensures migrations happen only on the appropriate database.
        """
        return db == self.get_db(app_label)
