# Generated by Django 5.0.7 on 2024-08-21 22:48

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("django_tasks_database", "0006_alter_dbtaskresult_args_kwargs_and_more"),
    ]

    operations = [
        migrations.RenameModel(
            old_name="DBTaskResult",
            new_name="DBTaskRun",
        ),
    ]
