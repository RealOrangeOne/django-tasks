from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        (
            "django_tasks_database",
            "0004_dbtaskresult_started_at",
        ),
    ]

    operations = [
        migrations.AddField(
            model_name="dbtaskresult",
            name="signature",
            field=models.TextField(),
        ),
        migrations.AddField(
            model_name="dbtaskresult",
            name="salt",
            field=models.TextField(),
        ),
    ]
