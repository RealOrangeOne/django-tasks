from typing import List, Sequence

from django.core.mail.backends import smtp
from django.core.mail.backends.base import BaseEmailBackend
from django.core.mail.message import EmailMessage, EmailMultiAlternatives

from django_tasks import task

__all__ = ["SMTPEmailBackend"]


@task()
def send_emails(email_messages: List[dict]) -> int:
    """
    Send emails using the real SMTP backend.
    """
    return smtp.EmailBackend().send_messages(
        [SMTPEmailBackend.deserialize_message(message) for message in email_messages]
    )


class SMTPEmailBackend(BaseEmailBackend):
    """
    An email backend to send emails as background tasks
    """

    @staticmethod
    def serialize_message(message: EmailMessage) -> dict:
        data = {
            "subject": message.subject,
            "body": message.body,
            "from_email": message.from_email,
            "to": message.to,
            "bcc": message.bcc,
            "attachments": message.attachments,
            "headers": message.extra_headers,
            "cc": message.cc,
            "reply_to": message.reply_to,
        }

        if isinstance(message, EmailMultiAlternatives):
            data["alternatives"] = message.alternatives

        return data

    @staticmethod
    def deserialize_message(message_data: dict) -> EmailMessage:
        if "alternatives" in message_data:
            return EmailMultiAlternatives(**message_data)
        return EmailMessage(**message_data)

    def send_messages(self, email_messages: Sequence[EmailMessage]) -> int:
        if not email_messages:
            return 0

        send_emails.enqueue(
            [self.serialize_message(message) for message in email_messages]
        )

        return len(email_messages)
