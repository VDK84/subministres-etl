# pyright: reportMissingImports=false
import email
import email.message
import imaplib
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from email.header import decode_header
from email.utils import parsedate_to_datetime
from typing import Any

from src.config import settings

logger = logging.getLogger("email_client")


@dataclass
class EmailAttachment:
    filename: str
    content_type: str
    data: bytes


@dataclass
class ParsedEmail:
    msg_id: str
    sender: str
    subject: str
    date: datetime | None
    body_text: str
    attachments: list[EmailAttachment] = field(default_factory=list)


def decode_str(header_value: Any) -> str:
    """Decodifica capçaleres de correu codificades en RFC 2047."""
    if not header_value:
        return ""
    decoded_fragments = decode_header(header_value)
    result = ""
    for frag, enc in decoded_fragments:
        if isinstance(frag, bytes):
            result += frag.decode(enc or "utf-8", errors="replace")
        else:
            result += str(frag)
    return result


class ImapMailboxClient:
    """Client IMAP per llegir la bústia dedicada a subministraments."""

    def __init__(self) -> None:
        self.host = settings.IMAP_HOST
        self.port = settings.IMAP_PORT
        self.user = settings.IMAP_USER
        self.password = settings.IMAP_PASSWORD
        self.folder = settings.IMAP_FOLDER

    def is_configured(self) -> bool:
        return bool(self.host and self.user and self.password)

    def fetch_recent_emails(
        self,
        sender_filters: list[str] | None = None,
        days_back: int | None = None,
    ) -> list[ParsedEmail]:
        """Cerca i extreu correus recents de la bústia segons filtre de remitent."""
        if not self.is_configured():
            logger.warning("IMAP no està configurat (host, user o password buits). Ometent connexió.")
            return []

        limit_days = days_back or settings.DAYS_BACK
        since_date = (datetime.now() - timedelta(days=limit_days)).strftime("%d-%b-%Y")

        emails: list[ParsedEmail] = []
        try:
            with imaplib.IMAP4_SSL(self.host, self.port) as client:
                client.login(self.user, self.password)
                client.select(self.folder, readonly=not settings.IMAP_MARK_SEEN)

                search_criteria = f'(SINCE "{since_date}")'
                _, data = client.search(None, search_criteria)
                mail_ids = data[0].split()

                logger.info(f"Trobats {len(mail_ids)} correus des de {since_date} a {self.folder}")

                for mail_id in mail_ids:
                    _, msg_data = client.fetch(mail_id, "(RFC822)")
                    if not msg_data or not msg_data[0]:
                        continue

                    raw_email = msg_data[0][1]
                    if not isinstance(raw_email, bytes):
                        continue

                    msg = email.message_from_bytes(raw_email)
                    sender = decode_str(msg.get("From", ""))
                    subject = decode_str(msg.get("Subject", ""))

                    # Filtratge per remitent o assumpte (suporta correus reenviats Fwd:)
                    if sender_filters:
                        matches_sender = any(f.lower() in sender.lower() for f in sender_filters)
                        matches_subject = any(f.lower() in subject.lower() for f in sender_filters)
                        if not matches_sender and not matches_subject:
                            continue

                    parsed = self._parse_message(mail_id.decode("utf-8"), msg, sender, subject)
                    emails.append(parsed)

        except (imaplib.IMAP4.error, OSError) as e:
            logger.error(f"Error connectant o llegint del servidor IMAP: {e}")

        return emails

    def _parse_message(
        self,
        msg_id: str,
        msg: email.message.Message,
        sender: str,
        subject: str,
    ) -> ParsedEmail:
        body_text_parts: list[str] = []
        attachments: list[EmailAttachment] = []

        date_hdr = msg.get("Date")
        date_obj: datetime | None = None
        if date_hdr:
            try:
                date_obj = parsedate_to_datetime(date_hdr)
            except (TypeError, ValueError) as err:
                logger.debug(f"No s'ha pogut parsejar la data {date_hdr}: {err}")

        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get("Content-Disposition", ""))

                filename = part.get_filename()
                if filename:
                    filename = decode_str(filename)

                if "attachment" in content_disposition.lower() or (filename and filename.lower().endswith(".pdf")):
                    payload = part.get_payload(decode=True)
                    if isinstance(payload, bytes):
                        attachments.append(
                            EmailAttachment(
                                filename=filename or f"adjunt_{msg_id}.pdf",
                                content_type=content_type,
                                data=payload,
                            )
                        )
                elif content_type in ("text/plain", "text/html") and "attachment" not in content_disposition.lower():
                    payload = part.get_payload(decode=True)
                    if isinstance(payload, bytes):
                        body_text_parts.append(payload.decode("utf-8", errors="replace"))
        else:
            payload = msg.get_payload(decode=True)
            if isinstance(payload, bytes):
                body_text_parts.append(payload.decode("utf-8", errors="replace"))

        body_text = "\n".join(body_text_parts)
        # Neteja de tags HTML bàsics si cal
        body_text = re.sub(r"<[^>]+>", " ", body_text)

        return ParsedEmail(
            msg_id=msg_id,
            sender=sender,
            subject=subject,
            date=date_obj,
            body_text=body_text,
            attachments=attachments,
        )


imap_client = ImapMailboxClient()
