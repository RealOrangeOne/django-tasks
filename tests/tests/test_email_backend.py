from django.core.mail import send_mail, send_mass_mail
from django.core.mail.message import EmailMessage, EmailMultiAlternatives
from django.test import SimpleTestCase, override_settings

from django_tasks import default_task_backend
from django_tasks.mail import SMTPEmailBackend
from django_tasks.utils import is_json_serializable


@override_settings(
    TASKS={"default": {"BACKEND": "django_tasks.backends.dummy.DummyBackend"}},
    EMAIL_BACKEND="django_tasks.mail.SMTPEmailBackend",
)
class EmailBackendTestCase(SimpleTestCase):
    def setUp(self) -> None:
        default_task_backend.clear()

    def test_send_mail(self) -> None:
        send_mail(
            subject="Subject",
            message="This is my email",
            recipient_list=["foo@example.com"],
            from_email=None,
        )

        self.assertEqual(len(default_task_backend.results), 1)

    def test_send_mass_email(self) -> None:
        sent_messages = send_mass_mail(
            [
                ("Subject", "This is my email", None, ["foo@example.com"]),
                ("Subject 2", "This is my second email", None, ["foo@example.com"]),
            ]
        )

        self.assertEqual(sent_messages, 2)

        self.assertEqual(len(default_task_backend.results), 1)

        result = default_task_backend.results[0]

        self.assertEqual(len(result.args[0]), 2)

    def test_deserialize_email_message(self) -> None:
        serialized_message = SMTPEmailBackend.serialize_message(
            EmailMessage(
                subject="Subject", body="Body", from_email=None, to=["foo@example.com"]
            )
        )
        SMTPEmailBackend.deserialize_message(serialized_message)

    def test_deserialize_multi_message(self) -> None:
        serialized_message = SMTPEmailBackend.serialize_message(
            EmailMultiAlternatives(
                subject="Subject", body="Body", from_email=None, to=["foo@example.com"]
            )
        )
        SMTPEmailBackend.deserialize_message(serialized_message)

    def test_serialized_message(self) -> None:
        serialized_email = SMTPEmailBackend.serialize_message(
            EmailMessage(
                subject="Subject", body="Body", from_email=None, to=["foo@example.com"]
            )
        )

        self.assertTrue(is_json_serializable(serialized_email))

    def test_serialized_multi_message(self) -> None:
        serialized_email = SMTPEmailBackend.serialize_message(
            EmailMultiAlternatives(
                subject="Subject", body="Body", from_email=None, to=["foo@example.com"]
            )
        )

        self.assertTrue(is_json_serializable(serialized_email))

    def test_serialized_message_with_attachments(self) -> None:
        email = EmailMessage(
            subject="Subject", body="Body", from_email=None, to=["foo@example.com"]
        )
        email.attach("example.txt", "Some text for an attachment", "text/plain")

        serialized_email = SMTPEmailBackend.serialize_message(email)

        self.assertTrue(is_json_serializable(serialized_email))

        recreated_message = SMTPEmailBackend.deserialize_message(serialized_email)

        self.assertEqual(
            recreated_message.attachments[0],
            ("example.txt", "Some text for an attachment", "text/plain"),
        )
