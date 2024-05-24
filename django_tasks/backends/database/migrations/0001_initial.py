import uuid

from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="DBTaskResult",
            fields=[
                (
                    "id",
                    models.UUIDField(
                        default=uuid.uuid4,
                        editable=False,
                        primary_key=True,
                        serialize=False,
                    ),
                ),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("NEW", "New"),
                            ("RUNNING", "Running"),
                            ("FAILED", "Failed"),
                            ("COMPLETE", "Complete"),
                        ],
                        default="NEW",
                        max_length=8,
                    ),
                ),
                ("args_kwargs", models.JSONField()),
                ("priority", models.PositiveSmallIntegerField(null=True)),
                ("task_path", models.TextField()),
                ("queue_name", models.TextField()),
                ("backend_name", models.TextField()),
                ("run_after", models.DateTimeField(null=True)),
                ("result", models.JSONField(default=None, null=True)),
            ],
        ),
    ]
