#!/usr/bin/env python3
"""
Custom MCP Server for Readdle Spark Desktop on macOS.
Interacts with Spark Desktop local SQLite databases in read-only mode,
provides search, attachment management, subscriptions, contact analytics,
email export, signatures, digests, meeting/calendar invite parsing,
link extraction, invoices, package tracking, thread browsing,
and Spark UI composition.

Copyright (C) 2026 Vikentiy Pukhaev
SPDX-License-Identifier: GPL-3.0-or-later
"""

import sys
import os
import re
import json
import base64
import sqlite3
import shutil
import time
import tempfile
import subprocess
import urllib.parse
from datetime import datetime
from email.message import EmailMessage
from email.utils import formatdate
from html.parser import HTMLParser
import html

SPARK_CORE_DATA = os.path.expanduser("~/Library/Application Support/Spark Desktop/core-data")
SPARK_CACHE_DIR = os.path.expanduser("~/Library/Caches/Spark Desktop/messagesData")
MESSAGES_DB = os.path.join(SPARK_CORE_DATA, "messages.sqlite")
CACHE_DB = os.path.join(SPARK_CORE_DATA, "cache.sqlite")
SEARCH_DB = os.path.join(SPARK_CORE_DATA, "search_fts5.sqlite")
CONTACTS_DB = os.path.join(SPARK_CORE_DATA, "contactsDictionary4.sqlite")
CALENDAR_DB = os.path.join(SPARK_CORE_DATA, "calendarsapi.sqlite")
SETTINGS_DB = os.path.join(SPARK_CORE_DATA, "settings.sqlite")
# Spark CLI. "Setup CLI" in Spark Settings > AI Agents creates /usr/local/bin/spark; Spark only
# authorizes calls made through it, not the SparklyRemote binary inside the .app.
SPARK_CLI = next((p for p in ("/usr/local/bin/spark", "/opt/homebrew/bin/spark") if os.path.exists(p)),
                 "/usr/local/bin/spark")

CATEGORY_MAP = {
    0: "other",
    1: "personal",
    2: "notifications",
    3: "newsletters",
    5: "pinned",
    6: "system"
}
REVERSE_CATEGORY_MAP = {v: k for k, v in CATEGORY_MAP.items()}


def get_ro_conn(db_path):
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found: {db_path}")
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    # Spark stores raw folded headers ("Name\r\n <addr>"); unfold them for every TEXT column.
    conn.text_factory = lambda b: b.decode("utf-8", errors="replace").replace("\r\n ", " ").replace("\r\n\t", " ")
    return conn


class HTMLToTextParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.text_parts = []
        self.hide = False

    def handle_starttag(self, tag, attrs):
        tag_lower = tag.lower()
        if tag_lower in ("script", "style", "head", "title", "meta"):
            self.hide = True
        elif tag_lower in ("p", "br", "div", "tr", "h1", "h2", "h3", "h4", "h5", "h6", "li"):
            self.text_parts.append("\n")

    def handle_endtag(self, tag):
        tag_lower = tag.lower()
        if tag_lower in ("script", "style", "head", "title", "meta"):
            self.hide = False
        elif tag_lower in ("p", "div", "tr"):
            self.text_parts.append("\n")

    def handle_data(self, data):
        if not self.hide:
            self.text_parts.append(data)

    def get_text(self):
        raw = "".join(self.text_parts)
        lines = [line.strip() for line in raw.splitlines()]
        cleaned = []
        for line in lines:
            if line:
                cleaned.append(line)
            elif cleaned and cleaned[-1] != "":
                cleaned.append("")
        return "\n".join(cleaned).strip()


def format_timestamp(ts):
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts)


def get_message_body(message_id, format="text"):
    if not os.path.exists(CACHE_DB):
        return None
    cache_conn = get_ro_conn(CACHE_DB)
    try:
        cc = cache_conn.cursor()
        cc.execute("SELECT data FROM messageBodyHtml WHERE messagePk = ?", (message_id,))
        row = cc.fetchone()
        if row and row["data"]:
            html_raw = row["data"].decode("utf-8", errors="ignore")
            if format == "html":
                return html_raw
            parser = HTMLToTextParser()
            parser.feed(html_raw)
            return parser.get_text()
        return None
    finally:
        cache_conn.close()


def get_message_parsed_info(message_id):
    if not os.path.exists(CACHE_DB):
        return None, None
    cache_conn = get_ro_conn(CACHE_DB)
    try:
        cc = cache_conn.cursor()
        cc.execute("SELECT data, sourceLanguage FROM messageBodyParsedData WHERE messagePk = ?", (message_id,))
        row = cc.fetchone()
        if not row:
            return None, None

        lang = row["sourceLanguage"]
        clean_text = None
        if row["data"]:
            try:
                d = json.loads(row["data"].decode("utf-8", errors="ignore"))
                parts = d.get("parts", {})
                if isinstance(parts, dict) and "value" in parts:
                    text_parts = []
                    for item in parts["value"]:
                        if item.get("typeName") == "RSMMessageBodyTextPart":
                            val = item.get("value", {})
                            attr = val.get("attributedText", {})
                            b64_str = attr.get("stringData")
                            if b64_str:
                                decoded = base64.b64decode(b64_str).decode("utf-8", errors="ignore")
                                text_parts.append(decoded.strip())
                    if text_parts:
                        clean_text = "\n\n".join(text_parts)
            except Exception:
                pass
        return clean_text, lang
    finally:
        cache_conn.close()


def find_cached_attachment_file(account_pk, msg_pk, att_name, att_url=None):
    if att_url and att_url.startswith("file://"):
        candidate = urllib.parse.unquote(att_url[7:])
        if os.path.exists(candidate):
            return candidate

    # att_name is sender-controlled: strip path parts and refuse anything resolving outside the cache.
    direct_candidate = os.path.join(SPARK_CACHE_DIR, str(account_pk), str(msg_pk), safe_filename(att_name, ""))
    cache_root = os.path.realpath(SPARK_CACHE_DIR) + os.sep
    if os.path.isfile(direct_candidate) and os.path.realpath(direct_candidate).startswith(cache_root):
        return direct_candidate

    # No name-only fallback: names like image001.png or invite.ics repeat across messages,
    # so matching by filename alone returns another message's file.
    return None


def run_spark_cli(args, timeout=120):
    """Run one subcommand of Spark Desktop's bundled CLI, return stdout.

    Needs Spark running and the agent connected in Settings -> AI Agents.
    Only subcommands from Spark's own tool catalog are allowed.
    """
    if args[0] not in {"attachment", "tools"} | {s["command"] for s in CLI_CATALOG.values()}:
        raise ValueError(f"Spark CLI subcommand not allowed: {args[0]}")
    if not os.path.exists(SPARK_CLI):
        raise RuntimeError(f"Spark CLI not found: {SPARK_CLI}")
    res = subprocess.run([SPARK_CLI] + [str(a) for a in args],
                         capture_output=True, text=True, timeout=timeout)
    if res.returncode != 0:
        raise RuntimeError(((res.stdout or "") + (res.stderr or "")).strip() or f"Spark CLI exit code {res.returncode}")
    return res.stdout


def download_attachment_via_cli(attachment_pk):
    """Ask Spark Desktop to download an uncached attachment. Returns the local path."""
    out = run_spark_cli(["attachment", int(attachment_pk)])
    m = re.search(r"^\s*Path:\s*(.+?)\s*$", out, re.M)
    path = m.group(1) if m else None
    if not path or not os.path.isfile(path):
        raise RuntimeError(f"Spark CLI gave no usable path: {out.strip()[:300]}")
    return path


def safe_filename(name, fallback):
    """Strip directories and path-hostile chars from a sender-controlled attachment name."""
    base = os.path.basename(name or "") or fallback
    return re.sub(r'[\\/*?:"<>|]', '_', base).lstrip(".") or fallback


# Tool implementations
def spark_list_accounts():
    conn = get_ro_conn(MESSAGES_DB)
    try:
        c = conn.cursor()
        c.execute("""
            SELECT pk, accountType, accountTitle, ownerFullName, orderNumber,
                   json_extract(additionalInfo, '$.accountAddress') AS address
            FROM accounts
            ORDER BY orderNumber ASC, pk ASC
        """)
        rows = c.fetchall()
        accounts = []
        for r in rows:
            accounts.append({
                "account_id": r["pk"],
                # accountTitle is the display label; the real address lives in additionalInfo JSON.
                "email": r["address"],
                "title": r["accountTitle"],
                "name": r["ownerFullName"],
                "type": r["accountType"]
            })
        return accounts
    finally:
        conn.close()


def spark_get_unread_summary():
    conn = get_ro_conn(MESSAGES_DB)
    try:
        c = conn.cursor()
        c.execute("""
            SELECT a.pk as account_id, a.accountTitle, a.ownerFullName,
                   SUM(CASE WHEN m.unseen = 1 THEN 1 ELSE 0 END) as total_unseen,
                   SUM(CASE WHEN m.unseen = 1 AND m.inInbox = 1 THEN 1 ELSE 0 END) as inbox_unseen,
                   COUNT(m.pk) as total_messages
            FROM accounts a
            LEFT JOIN messages m ON a.pk = m.accountPk
            GROUP BY a.pk, a.accountTitle, a.ownerFullName
            ORDER BY inbox_unseen DESC, total_unseen DESC
        """)
        rows = c.fetchall()
        summary = []
        for r in rows:
            summary.append({
                "account_id": r["account_id"],
                "account": r["accountTitle"],
                "owner": r["ownerFullName"] or "",
                "inbox_unseen": r["inbox_unseen"] or 0,
                "total_unseen": r["total_unseen"] or 0,
                "total_messages": r["total_messages"] or 0
            })
        return summary
    finally:
        conn.close()


def spark_find_unreplied_emails(older_than_days=0, account_id=None, limit=10):
    limit = max(1, min(int(limit), 50))
    now_ts = int(datetime.now().timestamp())
    cutoff_ts = now_ts - (int(older_than_days) * 86400)

    conn = get_ro_conn(MESSAGES_DB)
    try:
        c = conn.cursor()
        where_clauses = [
            "m.inInbox = 1",
            "m.category = 1",
            "m.receivedDate <= ?",
            """NOT EXISTS (
                SELECT 1 FROM messages s
                WHERE s.conversationPk = m.conversationPk
                  AND s.inSent = 1
                  AND s.receivedDate >= m.receivedDate
            )"""
        ]
        params = [cutoff_ts]

        if account_id:
            where_clauses.append("m.accountPk = ?")
            params.append(account_id)

        sql = f"""
            SELECT m.pk, m.accountPk, m.conversationPk, m.subject, m.messageFrom,
                   m.receivedDate, m.shortBody, m.unseen, m.starred
            FROM messages m
            WHERE {' AND '.join(where_clauses)}
            ORDER BY m.receivedDate DESC
            LIMIT ?
        """
        params.append(limit)
        c.execute(sql, params)
        rows = c.fetchall()

        results = []
        for r in rows:
            elapsed_hours = round((now_ts - r["receivedDate"]) / 3600.0, 1)
            elapsed_days = round(elapsed_hours / 24.0, 1)
            results.append({
                "message_id": r["pk"],
                "account_id": r["accountPk"],
                "conversation_id": r["conversationPk"],
                "from": r["messageFrom"],
                "subject": r["subject"] or "",
                "date": format_timestamp(r["receivedDate"]),
                "waiting_hours": elapsed_hours,
                "waiting_days": elapsed_days,
                "snippet": r["shortBody"] or "",
                "unseen": bool(r["unseen"])
            })
        return results
    finally:
        conn.close()


def spark_list_folders(account_id=None):
    conn = get_ro_conn(MESSAGES_DB)
    try:
        c = conn.cursor()
        c.execute("""
            SELECT pk, accountPk, folderName, folderPath, imapMessageCount, imapMessageUnseenCount
            FROM folders
            WHERE ? IS NULL OR accountPk = ?
            ORDER BY accountPk ASC, folderName ASC
        """, (account_id or None, account_id or None))
        rows = c.fetchall()
        folders = []
        for r in rows:
            folders.append({
                "folder_id": r["pk"],
                "account_id": r["accountPk"],
                "name": r["folderName"],
                "path": r["folderPath"],
                "total_count": r["imapMessageCount"],
                "unseen_count": r["imapMessageUnseenCount"]
            })
        return folders
    finally:
        conn.close()


def spark_list_threads(account_id=None, only_inbox=False, only_unseen=False, category=None, limit=20, offset=0):
    limit = max(1, min(int(limit), 100))
    offset = max(0, int(offset))

    conn = get_ro_conn(MESSAGES_DB)
    try:
        c = conn.cursor()
        where_clauses = []
        params = []

        if account_id:
            where_clauses.append("c.accountPk = ?")
            params.append(account_id)
        if only_inbox:
            where_clauses.append("c.inInbox = 1")
        if only_unseen:
            where_clauses.append("c.unseenMessages > 0")
        if category and category.lower() in REVERSE_CATEGORY_MAP:
            where_clauses.append("c.category = ?")
            params.append(REVERSE_CATEGORY_MAP[category.lower()])

        sql = """
            SELECT c.pk as conversation_id, c.accountPk, c.subject, c.sender, c.otherSenders,
                   c.totalMessages, c.unseenMessages, c.inInbox, c.inArchive, c.category,
                   c.inboxOrSnoozeDate, c.updateDate
            FROM conversations c
        """
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)

        sql += " ORDER BY c.inboxOrSnoozeDate DESC, c.updateDate DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        c.execute(sql, params)
        rows = c.fetchall()

        threads = []
        for r in rows:
            cat_id = r["category"]
            threads.append({
                "conversation_id": r["conversation_id"],
                "account_id": r["accountPk"],
                "subject": r["subject"] or "",
                "sender": r["sender"] or "",
                "other_senders": r["otherSenders"] or "",
                "total_messages": r["totalMessages"],
                "unseen_messages": r["unseenMessages"],
                "in_inbox": bool(r["inInbox"]),
                "in_archive": bool(r["inArchive"]),
                "category": CATEGORY_MAP.get(cat_id, "other"),
                "date": format_timestamp(r["inboxOrSnoozeDate"] or r["updateDate"])
            })
        return threads
    finally:
        conn.close()


def spark_list_messages(account_id=None, folder_id=None, category=None, only_inbox=False, only_unseen=False, only_starred=False, limit=10, offset=0):
    limit = max(1, min(int(limit), 100))
    offset = max(0, int(offset))

    conn = get_ro_conn(MESSAGES_DB)
    try:
        c = conn.cursor()
        where_clauses = []
        params = []

        query = """
            SELECT m.pk, m.accountPk, m.receivedDate, m.messageFrom, m.messageTo,
                   m.subject, m.shortBody, m.unseen, m.starred, m.inInbox,
                   m.numberOfFileAttachments, m.conversationPk, m.category
            FROM messages m
        """
        if folder_id:
            query += " JOIN messageFoldersInfo f ON m.pk = f.messagePk WHERE f.folderPk = ?"
            params.append(folder_id)
        else:
            query += " WHERE 1=1"

        if account_id:
            where_clauses.append("m.accountPk = ?")
            params.append(account_id)
        if category and category.lower() in REVERSE_CATEGORY_MAP:
            where_clauses.append("m.category = ?")
            params.append(REVERSE_CATEGORY_MAP[category.lower()])
        if only_inbox:
            where_clauses.append("m.inInbox = 1")
        if only_unseen:
            where_clauses.append("m.unseen = 1")
        if only_starred:
            where_clauses.append("m.starred = 1")

        if where_clauses:
            query += " AND " + " AND ".join(where_clauses)

        query += " ORDER BY m.receivedDate DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        c.execute(query, params)
        rows = c.fetchall()

        messages = []
        for r in rows:
            cat_val = r["category"]
            messages.append({
                "message_id": r["pk"],
                "conversation_id": r["conversationPk"],
                "account_id": r["accountPk"],
                "date": format_timestamp(r["receivedDate"]),
                "from": r["messageFrom"],
                "to": r["messageTo"],
                "subject": r["subject"] or "",
                "snippet": r["shortBody"] or "",
                "category": CATEGORY_MAP.get(cat_val, "other"),
                "unseen": bool(r["unseen"]),
                "starred": bool(r["starred"]),
                "in_inbox": bool(r["inInbox"]),
                "attachments_count": r["numberOfFileAttachments"] or 0
            })
        return messages
    finally:
        conn.close()


def spark_get_message(message_id, format="text", exclude_quoted_history=False):
    message_id = int(message_id)

    meta_conn = get_ro_conn(MESSAGES_DB)
    try:
        mc = meta_conn.cursor()
        mc.execute("""
            SELECT pk, accountPk, conversationPk, receivedDate, creationDate, messageFrom, messageTo,
                   messageCc, messageBcc, subject, shortBody, unseen, starred,
                   numberOfFileAttachments, messageId, category, inSent
            FROM messages
            WHERE pk = ?
        """, (message_id,))
        msg = mc.fetchone()
        if not msg:
            raise ValueError(f"Message with ID {message_id} not found")

        mc.execute("""
            SELECT pk, attachmentName, attachmentSize, attachmentMIMEType, attachmentURL
            FROM messageAttachment
            WHERE messagePk = ?
        """, (message_id,))
        att_rows = mc.fetchall()
        attachments = []
        for a in att_rows:
            cached_path = find_cached_attachment_file(
                msg["accountPk"], msg["pk"], a["attachmentName"], a["attachmentURL"]
            )
            attachments.append({
                "attachment_id": a["pk"],
                "name": a["attachmentName"],
                "size_bytes": a["attachmentSize"],
                "mime_type": a["attachmentMIMEType"],
                "cached_path": cached_path
            })
    finally:
        meta_conn.close()

    clean_unquoted, detected_lang = get_message_parsed_info(message_id)

    if exclude_quoted_history and clean_unquoted:
        body = clean_unquoted
    else:
        body = get_message_body(message_id, format=format)
        if not body:
            body = clean_unquoted or msg["shortBody"] or ""

    return {
        "message_id": msg["pk"],
        "conversation_id": msg["conversationPk"],
        "account_id": msg["accountPk"],
        "received_date": format_timestamp(msg["receivedDate"]),
        "from": msg["messageFrom"],
        "to": msg["messageTo"],
        "cc": msg["messageCc"],
        "bcc": msg["messageBcc"],
        "subject": msg["subject"] or "",
        "category": CATEGORY_MAP.get(msg["category"], "other"),
        "detected_language": detected_lang,
        "body": body,
        "unseen": bool(msg["unseen"]),
        "starred": bool(msg["starred"]),
        "in_sent": bool(msg["inSent"]),
        "rfc_message_id": msg["messageId"],
        "attachments": attachments
    }


def spark_get_thread(conversation_id=None, message_id=None, format="text", exclude_quoted_history=True):
    if not conversation_id and not message_id:
        raise ValueError("Either conversation_id or message_id must be provided")

    conn = get_ro_conn(MESSAGES_DB)
    try:
        c = conn.cursor()
        if not conversation_id:
            c.execute("SELECT conversationPk FROM messages WHERE pk = ?", (int(message_id),))
            row = c.fetchone()
            if not row or not row["conversationPk"]:
                raise ValueError(f"Message {message_id} does not belong to any conversation or was not found")
            conv_pk = row["conversationPk"]
        else:
            conv_pk = int(conversation_id)

        c.execute("""
            SELECT pk, accountPk, receivedDate, messageFrom, messageTo, messageCc,
                   subject, shortBody, unseen, starred, numberOfFileAttachments, category
            FROM messages
            WHERE conversationPk = ?
            ORDER BY receivedDate ASC
        """, (conv_pk,))
        msg_rows = c.fetchall()
        if not msg_rows:
            return {"conversation_id": conv_pk, "subject": "", "messages_count": 0, "messages": []}

        subject = msg_rows[0]["subject"] or ""
        messages = []
        for r in msg_rows:
            m_pk = r["pk"]
            clean_unquoted, lang = get_message_parsed_info(m_pk)
            if exclude_quoted_history and clean_unquoted:
                body = clean_unquoted
            else:
                body = get_message_body(m_pk, format=format) or clean_unquoted or r["shortBody"] or ""

            messages.append({
                "message_id": m_pk,
                "account_id": r["accountPk"],
                "date": format_timestamp(r["receivedDate"]),
                "from": r["messageFrom"],
                "to": r["messageTo"],
                "cc": r["messageCc"],
                "subject": r["subject"] or "",
                "category": CATEGORY_MAP.get(r["category"], "other"),
                "detected_language": lang,
                "body": body,
                "unseen": bool(r["unseen"]),
                "attachments_count": r["numberOfFileAttachments"] or 0
            })

        return {
            "conversation_id": conv_pk,
            "subject": subject,
            "messages_count": len(messages),
            "messages": messages
        }
    finally:
        conn.close()


def spark_get_attachment(attachment_id=None, message_id=None, filename=None, download=True):
    conn = get_ro_conn(MESSAGES_DB)
    try:
        c = conn.cursor()
        if attachment_id:
            c.execute("""
                SELECT a.pk, a.messagePk, a.attachmentName, a.attachmentSize, a.attachmentMIMEType,
                       a.attachmentURL, m.accountPk
                FROM messageAttachment a
                JOIN messages m ON a.messagePk = m.pk
                WHERE a.pk = ?
            """, (int(attachment_id),))
            att = c.fetchone()
        elif message_id and filename:
            c.execute("""
                SELECT a.pk, a.messagePk, a.attachmentName, a.attachmentSize, a.attachmentMIMEType,
                       a.attachmentURL, m.accountPk
                FROM messageAttachment a
                JOIN messages m ON a.messagePk = m.pk
                WHERE a.messagePk = ? AND a.attachmentName LIKE ?
                LIMIT 1
            """, (int(message_id), f"%{filename}%"))
            att = c.fetchone()
        elif message_id:
            c.execute("""
                SELECT a.pk, a.messagePk, a.attachmentName, a.attachmentSize, a.attachmentMIMEType,
                       a.attachmentURL, m.accountPk
                FROM messageAttachment a
                JOIN messages m ON a.messagePk = m.pk
                WHERE a.messagePk = ?
                LIMIT 1
            """, (int(message_id),))
            att = c.fetchone()
        else:
            raise ValueError("Provide either attachment_id, or message_id (+ optional filename)")

        if not att:
            raise ValueError("Attachment not found in database")

        att_name = att["attachmentName"]
        account_pk = att["accountPk"]
        msg_pk = att["messagePk"]
        att_url = att["attachmentURL"]

        found_path = find_cached_attachment_file(account_pk, msg_pk, att_name, att_url)
        downloaded, download_error = False, None
        if not found_path and download:
            try:
                found_path = download_attachment_via_cli(att["pk"])
                downloaded = True
            except Exception as e:
                download_error = str(e)

        result = {
            "attachment_id": att["pk"],
            "message_id": msg_pk,
            "filename": att_name,
            "size_bytes": att["attachmentSize"],
            "mime_type": att["attachmentMIMEType"],
            "cached_locally": found_path is not None,
            "downloaded_now": downloaded,
            "file_path": found_path
        }
        if download_error:
            result["download_error"] = download_error
        return result
    finally:
        conn.close()


def spark_search_attachments(query=None, mime_type=None, limit=20):
    limit = max(1, min(int(limit), 50))
    conn = get_ro_conn(MESSAGES_DB)
    try:
        c = conn.cursor()
        where_clauses = []
        params = []

        if query:
            where_clauses.append("a.attachmentName LIKE ?")
            params.append(f"%{query}%")
        if mime_type:
            where_clauses.append("a.attachmentMIMEType LIKE ?")
            params.append(f"%{mime_type}%")

        sql = """
            SELECT a.pk, a.messagePk, a.attachmentName, a.attachmentSize, a.attachmentMIMEType,
                   a.attachmentURL, m.accountPk, m.subject, m.receivedDate, m.messageFrom
            FROM messageAttachment a
            JOIN messages m ON a.messagePk = m.pk
        """
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        sql += " ORDER BY m.receivedDate DESC LIMIT ?"
        params.append(limit)

        c.execute(sql, params)
        rows = c.fetchall()
        results = []
        for r in rows:
            cached_path = find_cached_attachment_file(
                r["accountPk"], r["messagePk"], r["attachmentName"], r["attachmentURL"]
            )
            results.append({
                "attachment_id": r["pk"],
                "message_id": r["messagePk"],
                "filename": r["attachmentName"],
                "size_bytes": r["attachmentSize"],
                "mime_type": r["attachmentMIMEType"],
                "message_subject": r["subject"] or "",
                "message_from": r["messageFrom"],
                "date": format_timestamp(r["receivedDate"]),
                "cached_locally": cached_path is not None,
                "cached_path": cached_path
            })
        return results
    finally:
        conn.close()


def spark_find_invoices(query=None, limit=20):
    limit = max(1, min(int(limit), 50))
    conn = get_ro_conn(MESSAGES_DB)
    try:
        c = conn.cursor()
        where_clauses = [
            """(
                m.subject LIKE '%rechnung%' OR m.subject LIKE '%invoice%' OR m.subject LIKE '%receipt%'
                OR m.subject LIKE '%beleg%' OR m.subject LIKE '%quittung%' OR m.subject LIKE '%abrechnung%'
                OR a.attachmentName LIKE '%invoice%' OR a.attachmentName LIKE '%rechnung%' OR a.attachmentName LIKE '%beleg%'
            )"""
        ]
        params = []

        if query:
            where_clauses.append("(m.subject LIKE ? OR a.attachmentName LIKE ? OR m.messageFrom LIKE ?)")
            pat = f"%{query}%"
            params.extend([pat, pat, pat])

        sql = f"""
            SELECT m.pk as message_id, m.accountPk, m.subject, m.messageFrom, m.receivedDate,
                   a.pk as attachment_id, a.attachmentName, a.attachmentSize, a.attachmentMIMEType, a.attachmentURL
            FROM messages m
            JOIN messageAttachment a ON m.pk = a.messagePk
            WHERE {' AND '.join(where_clauses)}
            ORDER BY m.receivedDate DESC
            LIMIT ?
        """
        params.append(limit)
        c.execute(sql, params)
        rows = c.fetchall()

        invoices = []
        for r in rows:
            cached_path = find_cached_attachment_file(
                r["accountPk"], r["message_id"], r["attachmentName"], r["attachmentURL"]
            )
            invoices.append({
                "message_id": r["message_id"],
                "subject": r["subject"] or "",
                "from": r["messageFrom"],
                "date": format_timestamp(r["receivedDate"]),
                "attachment_id": r["attachment_id"],
                "filename": r["attachmentName"],
                "size_bytes": r["attachmentSize"],
                "mime_type": r["attachmentMIMEType"],
                "cached_locally": cached_path is not None,
                "file_path": cached_path
            })

        return invoices
    finally:
        conn.close()


def spark_find_deliveries(days=30, limit=20):
    limit = max(1, min(int(limit), 50))
    since_ts = int(datetime.now().timestamp()) - (int(days) * 86400)

    conn = get_ro_conn(MESSAGES_DB)
    try:
        c = conn.cursor()
        sql = """
            SELECT pk as message_id, accountPk, subject, messageFrom, receivedDate, shortBody
            FROM messages
            WHERE (
                subject LIKE '%tracking%' OR subject LIKE '%delivery%' OR subject LIKE '%sendung%'
                OR subject LIKE '%paket%' OR subject LIKE '%versand%' OR subject LIKE '%zugestellt%'
                OR subject LIKE '%order%' OR subject LIKE '%bestellung%' OR subject LIKE '%shipped%'
                OR subject LIKE '%dpd%' OR subject LIKE '%dhl%' OR subject LIKE '%post.at%'
            )
            AND receivedDate >= ?
            ORDER BY receivedDate DESC
            LIMIT ?
        """
        c.execute(sql, (since_ts, limit))
        rows = c.fetchall()

        deliveries = []
        for r in rows:
            m_id = r["message_id"]
            link_info = spark_extract_links(m_id)
            tracking_links = link_info.get("action_links", [])

            # Extract order number patterns
            comb = f"{r['subject']} {r['shortBody']}"
            order_nums = re.findall(r'\b[A-Z0-9]{2,5}[-_][0-9]{4,15}\b|\b[A-Z]{0,3}\d{7,16}\b', comb)

            deliveries.append({
                "message_id": m_id,
                "subject": r["subject"],
                "from": r["messageFrom"],
                "date": format_timestamp(r["receivedDate"]),
                "snippet": r["shortBody"] or "",
                "detected_numbers": list(dict.fromkeys(order_nums))[:3],
                "tracking_links": [l["url"] for l in tracking_links][:3]
            })
        return deliveries
    finally:
        conn.close()


def spark_list_subscriptions(limit=25):
    limit = max(1, min(int(limit), 100))
    conn = get_ro_conn(MESSAGES_DB)
    try:
        c = conn.cursor()
        c.execute("""
            SELECT messageFrom, listUnsubscribeURL, count(*) as count, max(receivedDate) as last_received
            FROM messages
            WHERE listUnsubscribeURL IS NOT NULL AND listUnsubscribeURL != ''
            GROUP BY messageFrom, listUnsubscribeURL
            ORDER BY count DESC
            LIMIT ?
        """, (limit,))
        rows = c.fetchall()
        subs = []
        for r in rows:
            raw_url = r["listUnsubscribeURL"] or ""
            # List-Unsubscribe: "<https://...>, <mailto:...>"; may be an empty "<>". Prefer the web link.
            urls = [u.strip() for u in re.findall(r'<([^>]*)>', raw_url) if u.strip()]
            if not urls and raw_url.strip() and "<" not in raw_url:
                urls = [raw_url.strip()]
            unsub_link = next((u for u in urls if u.startswith("http")), urls[0] if urls else None)

            subs.append({
                "sender": r["messageFrom"],
                "emails_count": r["count"],
                "last_received": format_timestamp(r["last_received"]),
                "unsubscribe_url": unsub_link
            })
        return subs
    finally:
        conn.close()


def spark_get_contact_history(email, limit=20):
    limit = max(1, min(int(limit), 50))
    conn = get_ro_conn(MESSAGES_DB)
    try:
        c = conn.cursor()
        pattern = f"%{email}%"

        c.execute("""
            SELECT count(*) as total,
                   min(receivedDate) as first_date,
                   max(receivedDate) as last_date
            FROM messages
            WHERE messageFrom LIKE ? OR messageTo LIKE ? OR messageCc LIKE ?
        """, (pattern, pattern, pattern))
        stats = c.fetchone()

        c.execute("""
            SELECT pk, conversationPk, accountPk, receivedDate, messageFrom, messageTo,
                   subject, shortBody, unseen, inSent
            FROM messages
            WHERE messageFrom LIKE ? OR messageTo LIKE ? OR messageCc LIKE ?
            ORDER BY receivedDate DESC
            LIMIT ?
        """, (pattern, pattern, pattern, limit))
        rows = c.fetchall()

        messages = []
        for r in rows:
            messages.append({
                "message_id": r["pk"],
                "conversation_id": r["conversationPk"],
                "date": format_timestamp(r["receivedDate"]),
                "direction": "sent" if r["inSent"] else "received",
                "from": r["messageFrom"],
                "to": r["messageTo"],
                "subject": r["subject"] or "",
                "snippet": r["shortBody"] or ""
            })

        return {
            "contact_email": email,
            "total_messages": stats["total"] if stats else 0,
            "first_contact": format_timestamp(stats["first_date"]) if stats else None,
            "last_contact": format_timestamp(stats["last_date"]) if stats else None,
            "recent_messages": messages
        }
    finally:
        conn.close()


def spark_search_contacts(query, limit=20):
    limit = max(1, min(int(limit), 50))
    if not os.path.exists(CONTACTS_DB):
        return []

    conn = get_ro_conn(CONTACTS_DB)
    try:
        c = conn.cursor()
        pattern = f"%{query}%"
        c.execute("""
            SELECT DISTINCT e.email, n.name, e.quality
            FROM ContactEmails e
            LEFT JOIN ContactNames n ON e.contactPk = n.contactPk
            WHERE e.email LIKE ? OR n.name LIKE ?
            ORDER BY e.quality DESC, n.name ASC
            LIMIT ?
        """, (pattern, pattern, limit))
        rows = c.fetchall()
        contacts = []
        for r in rows:
            contacts.append({
                "email": r["email"],
                "name": r["name"] or "",
                "quality": r["quality"]
            })
        return contacts
    finally:
        conn.close()


def spark_list_signatures():
    if not os.path.exists(SETTINGS_DB):
        return []

    conn = get_ro_conn(SETTINGS_DB)
    try:
        c = conn.cursor()
        c.execute("""
            SELECT itemKey, itemValue FROM settings
            WHERE itemGroup = 'SignaturesSettingsItemsGroup'
        """)
        rows = c.fetchall()
        signatures = []
        for r in rows:
            val = r["itemValue"]
            if not val:
                continue
            try:
                d = json.loads(val.decode("utf-8", errors="ignore"))
                if not d.get("deleted"):
                    html = d.get("htmlContent") or ""
                    parser = HTMLToTextParser()
                    parser.feed(html)
                    plain = parser.get_text()
                    signatures.append({
                        "id": d.get("identifier"),
                        "text": plain,
                        "html": html
                    })
            except Exception:
                pass
        return signatures
    finally:
        conn.close()


def spark_get_digest(days=1, account_id=None, limit=50):
    days = max(1, min(int(days), 30))
    limit = max(1, min(int(limit), 200))
    since_ts = int(datetime.now().timestamp()) - (days * 86400)

    conn = get_ro_conn(MESSAGES_DB)
    try:
        c = conn.cursor()
        where_clauses = ["receivedDate >= ?"]
        params = [since_ts]

        if account_id:
            where_clauses.append("accountPk = ?")
            params.append(account_id)

        sql = f"""
            SELECT pk, accountPk, receivedDate, messageFrom, subject, shortBody,
                   unseen, starred, category, numberOfFileAttachments
            FROM messages
            WHERE {' AND '.join(where_clauses)}
            ORDER BY receivedDate DESC
        """
        c.execute(sql, params)
        rows = c.fetchall()

        digest = {
            "period_days": days,
            "since": format_timestamp(since_ts),
            "total_received": len(rows),
            "total_unseen": 0,
            "personal": [],
            "notifications": [],
            "newsletters": [],
            "other": []
        }

        digest["total_unseen"] = sum(1 for r in rows if r["unseen"])
        digest["truncated"] = len(rows) > limit
        # Totals cover the whole window; only the newest `limit` messages are listed.
        for r in rows[:limit]:

            cat_id = r["category"]
            cat_name = CATEGORY_MAP.get(cat_id, "other")

            item = {
                "message_id": r["pk"],
                "account_id": r["accountPk"],
                "date": format_timestamp(r["receivedDate"]),
                "from": r["messageFrom"],
                "subject": r["subject"] or "",
                "snippet": r["shortBody"] or "",
                "unseen": bool(r["unseen"]),
                "attachments": r["numberOfFileAttachments"] or 0
            }

            digest[cat_name if cat_name in ("personal", "notifications", "newsletters") else "other"].append(item)

        return digest
    finally:
        conn.close()


CHROME_PATHS = (
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
)


def html_to_pdf(full_html, out_file):
    """Print HTML to PDF with headless Chrome. Returns False if no Chrome is installed."""
    chrome_bin = next((p for p in CHROME_PATHS if os.path.exists(p)), None)
    if not chrome_bin:
        return False
    with tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8") as tmp:
        tmp.write(full_html)
    try:
        res = subprocess.run([chrome_bin, "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
                              f"--print-to-pdf={out_file}", tmp.name], capture_output=True, text=True)
        if res.returncode != 0:
            raise RuntimeError(f"Chrome PDF export failed: {res.stderr}")
        return True
    finally:
        os.remove(tmp.name)


def spark_export_email(message_id, output_path, format="pdf"):
    format_lower = format.lower()
    msg = spark_get_message(message_id, format="html" if format_lower in ("pdf", "html") else "text")
    out_file = os.path.expanduser(output_path)
    os.makedirs(os.path.dirname(os.path.abspath(out_file)), exist_ok=True)

    if format_lower == "txt":
        content = f"From: {msg['from']}\nTo: {msg['to']}\nDate: {msg['received_date']}\nSubject: {msg['subject']}\n"
        if msg.get("attachments"):
            att_names = [a["name"] for a in msg["attachments"]]
            content += f"Attachments: {', '.join(att_names)}\n"
        content += f"\n----------------------------------------\n\n{msg['body']}\n"
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(content)

    elif format_lower == "eml":
        eml = EmailMessage()
        eml["Subject"] = msg["subject"]
        eml["From"] = msg["from"]
        eml["To"] = msg["to"]
        if msg.get("cc"):
            eml["Cc"] = msg["cc"]
        if msg["received_date"]:
            eml["Date"] = formatdate(datetime.strptime(msg["received_date"], "%Y-%m-%d %H:%M:%S").timestamp(), localtime=True)
        eml.set_content(msg["body"])
        with open(out_file, "wb") as f:
            f.write(eml.as_bytes())

    elif format_lower in ("html", "pdf"):
        full_html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 40px; color: #222; }}
.header {{ border-bottom: 2px solid #e0e0e0; padding-bottom: 16px; margin-bottom: 24px; }}
.header h1 {{ margin: 0 0 12px 0; font-size: 22px; }}
.meta-row {{ margin: 4px 0; color: #555; font-size: 14px; }}
.meta-row strong {{ color: #222; }}
.body {{ line-height: 1.5; font-size: 15px; }}
</style>
</head>
<body>
<div class="header">
  <h1>{msg['subject']}</h1>
  <div class="meta-row"><strong>From:</strong> {msg['from']}</div>
  <div class="meta-row"><strong>To:</strong> {msg['to']}</div>
  <div class="meta-row"><strong>Date:</strong> {msg['received_date']}</div>
</div>
<div class="body">
{msg['body']}
</div>
</body>
</html>"""

        if format_lower == "html":
            with open(out_file, "w", encoding="utf-8") as f:
                f.write(full_html)
        else:
            if not html_to_pdf(full_html, out_file):
                raise RuntimeError("Google Chrome not found for PDF rendering. Export to .html or .txt instead.")

    else:
        raise ValueError(f"Unsupported format: {format}. Supported: pdf, html, txt, eml")

    return {
        "status": "exported",
        "format": format_lower,
        "output_path": out_file,
        "message_id": message_id
    }


def parse_ics_date(val):
    clean = re.sub(r'[^0-9T]', '', val.strip())
    try:
        if 'T' in clean:
            dt = datetime.strptime(clean[:15], '%Y%m%dT%H%M%S')
            # Only a trailing Z means UTC; otherwise it is TZID-local or floating time.
            return dt.strftime('%Y-%m-%d %H:%M:%S') + (' (UTC)' if val.strip().endswith('Z') else '')
        else:
            dt = datetime.strptime(clean[:8], '%Y%m%d')
            return dt.strftime('%Y-%m-%d')
    except Exception:
        return val


def spark_parse_calendar_invites(message_id=None, limit=10):
    limit = max(1, min(int(limit), 50))
    conn = get_ro_conn(MESSAGES_DB)
    try:
        c = conn.cursor()
        where_clauses = ["a.attachmentName LIKE '%.ics'"]
        params = []

        if message_id:
            where_clauses.append("a.messagePk = ?")
            params.append(message_id)

        sql = f"""
            SELECT a.pk, a.messagePk, a.attachmentName, a.attachmentURL, m.accountPk, m.subject, m.receivedDate, m.messageFrom
            FROM messageAttachment a
            JOIN messages m ON a.messagePk = m.pk
            WHERE {' AND '.join(where_clauses)}
            ORDER BY m.receivedDate DESC
            LIMIT ?
        """
        params.append(limit)
        c.execute(sql, params)
        rows = c.fetchall()

        events = []
        for r in rows:
            file_path = find_cached_attachment_file(r["accountPk"], r["messagePk"], r["attachmentName"], r["attachmentURL"])
            if not file_path or not os.path.exists(file_path):
                continue

            try:
                with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()

                # Unfold continuation lines and look only inside VEVENT (VTIMEZONE has its own DTSTART).
                unfolded = re.sub(r'\r?\n[ \t]', '', content)
                ev = re.search(r'BEGIN:VEVENT(.*?)(?:END:VEVENT|$)', unfolded, re.DOTALL)
                event_block = ev.group(1) if ev else unfolded

                def extract_val(name):
                    # NAME[;PARAM=...]:value — params may contain ':' only inside quotes.
                    m = re.search(rf'^{name}(?:;(?:"[^"]*"|[^:\r\n])*)?:([^\r\n]*)', event_block, re.MULTILINE)
                    # RFC 5545 TEXT escapes: \n \N \, \; \\
                    return re.sub(r'\\([\\;,nN])', lambda e: "\n" if e.group(1) in "nN" else e.group(1), m.group(1)).strip() if m else None

                summary = extract_val('SUMMARY')
                dtstart = extract_val('DTSTART')
                dtend = extract_val('DTEND')
                location = extract_val('LOCATION')
                desc = extract_val('DESCRIPTION')
                status = extract_val('STATUS')

                events.append({
                    "message_id": r["messagePk"],
                    "email_subject": r["subject"],
                    "from": r["messageFrom"],
                    "event_title": summary or r["attachmentName"],
                    "start": parse_ics_date(dtstart) if dtstart else None,
                    "end": parse_ics_date(dtend) if dtend else None,
                    "location": location or "",
                    "description": desc or "",
                    "status": status or "CONFIRMED",
                    "ics_file": file_path
                })
            except Exception:
                pass

        return events
    finally:
        conn.close()


def spark_extract_links(message_id):
    msg = spark_get_message(message_id, format="html")
    html_content = msg.get("body", "")

    link_items = re.findall(r'<a\s+(?:[^>]*?\s+)?href=["\'](https?://[^"\'>]+)["\'][^>]*>(.*?)</a>', html_content, re.IGNORECASE | re.DOTALL)
    seen = set()
    links = []

    for url, text_html in link_items:
        clean_url = html.unescape(url.strip())
        if clean_url in seen:
            continue
        seen.add(clean_url)
        label = html.unescape(re.sub(r'<[^>]+>', '', text_html)).strip()
        links.append({"url": clean_url, "label": label})

    # Bare URLs only from visible text: scanning raw HTML picks up img src, fonts and DTDs.
    parser = HTMLToTextParser()
    parser.feed(html_content)
    for u in re.findall(r'https?://[^\s<>"\']+', parser.get_text()):
        u_clean = u.rstrip(".,;:")
        while u_clean.endswith(")") and u_clean.count(")") > u_clean.count("("):
            u_clean = u_clean[:-1]
        if u_clean not in seen:
            seen.add(u_clean)
            links.append({"url": u_clean, "label": ""})

    # Short carrier names need word boundaries ("ups" in "upscale"); stems need only a leading one.
    action_re = re.compile(r'\b(?:dhl|ups|dpd)\b|post\.at|\b(?:confirm|verif|track|booking|download|pay|order|login|view|ticket|invoice)')
    unsub_keywords = ("unsubscribe", "optout", "opt-out", "abmelden", "subscription")

    action_links = []
    unsubscribe_links = []
    general_links = []

    for l in links:
        comb = f"{l['url'].lower()} {l['label'].lower()}"
        if any(k in comb for k in unsub_keywords):
            unsubscribe_links.append(l)
        elif action_re.search(comb):
            action_links.append(l)
        else:
            general_links.append(l)

    return {
        "message_id": message_id,
        "subject": msg.get("subject"),
        "total_unique_links": len(links),
        "action_links": action_links,
        "unsubscribe_links": unsubscribe_links,
        "general_links": general_links
    }


def spark_reply_to_email(message_id, body, cc=None, bcc=None, auto_signature=True):
    orig = spark_get_message(message_id, format="text", exclude_quoted_history=True)
    conn = get_ro_conn(MESSAGES_DB)
    try:
        row = conn.execute("""
            SELECT m.messageReplyToMailbox AS reply_to, a.ownerFullName AS owner
            FROM messages m LEFT JOIN accounts a ON a.pk = m.accountPk
            WHERE m.pk = ?
        """, (int(message_id),)).fetchone()
    finally:
        conn.close()
    # Replying to our own sent message should go to its recipients, not back to us.
    if orig.get("in_sent"):
        to_addr = orig["to"] or orig["from"]
    else:
        to_addr = (row and row["reply_to"]) or orig["from"]
    orig_subj = orig["subject"] or ""
    if not orig_subj.lower().startswith("re:"):
        subject = f"Re: {orig_subj}"
    else:
        subject = orig_subj

    final_body = body.strip()

    if auto_signature:
        lang = orig.get("detected_language") or "en"
        sigs = spark_list_signatures()
        # Spark doesn't link signatures to accounts. Sign only when some signature names the
        # account owner, so a reply from someone else's mailbox doesn't carry our name.
        # ponytail: name-match heuristic; switch to a real account->signature map if Spark exposes one.
        owner_words = [w for w in ((row["owner"] if row else "") or "").split() if len(w) >= 3]
        if not any(w in s.get("text", "") for s in sigs for w in owner_words):
            sigs = []
        matched_sig = None
        for s in sigs:
            stext = s.get("text", "")
            if lang == "ru" and any(w in stext for w in ("уважением", "С уважением")):
                matched_sig = stext
                break
            elif lang == "de" and any(w in stext for w in ("Grüßen", "freundlichen")):
                matched_sig = stext
                break
            elif lang == "en" and any(w in stext for w in ("regards", "Regards", "Best")):
                matched_sig = stext
                break

        if not matched_sig and sigs:
            matched_sig = sigs[0].get("text")

        if matched_sig:
            final_body += f"\n\n--\n{matched_sig}"

    return spark_compose_email(to=to_addr, subject=subject, body=final_body, cc=cc or "", bcc=bcc or "")


def spark_list_calendar_events(start_timestamp=None, end_timestamp=None, query=None, limit=20):
    limit = max(1, min(int(limit), 50))
    if not os.path.exists(CALENDAR_DB):
        return []

    conn = get_ro_conn(CALENDAR_DB)
    try:
        c = conn.cursor()
        where_clauses = []
        params = []

        if start_timestamp:
            where_clauses.append("dstart >= ?")
            params.append(int(start_timestamp))
        if end_timestamp:
            where_clauses.append("dend <= ?")
            params.append(int(end_timestamp))
        if query:
            where_clauses.append("(summary LIKE ? OR descriptionProperty LIKE ? OR location LIKE ?)")
            pat = f"%{query}%"
            params.extend([pat, pat, pat])

        sql = """
            SELECT pk, summary, descriptionProperty, location, dstart, dend, allDay, status, rrule
            FROM RDCALAPIEvent
        """
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        sql += " ORDER BY dstart ASC LIMIT ?"
        params.append(limit)

        c.execute(sql, params)
        rows = c.fetchall()
        events = []
        for r in rows:
            events.append({
                "event_id": r["pk"],
                "title": r["summary"] or "",
                "start": format_timestamp(r["dstart"]),
                "end": format_timestamp(r["dend"]),
                "all_day": bool(r["allDay"]),
                "location": r["location"] or "",
                "description": r["descriptionProperty"] or "",
                "status": r["status"]
            })
        return events
    finally:
        conn.close()


def spark_search_messages(query, limit=20):
    if not (query or "").strip():
        raise ValueError("query argument is required")
    limit = max(1, min(int(limit), 50))
    results = []

    if os.path.exists(SEARCH_DB):
        try:
            fts_conn = get_ro_conn(SEARCH_DB)
            try:
                fc = fts_conn.cursor()
                clean_query = "".join(c if c.isalnum() or c.isspace() else " " for c in query).strip()
                if clean_query:
                    match_expr = " ".join(f'"{word}"*' for word in clean_query.split())
                    fts_conn.execute(f'ATTACH DATABASE "file:{MESSAGES_DB}?mode=ro" AS msg_db')
                    fc.execute("""
                        SELECT f.messagePk, f.messageFrom, f.messageTo, f.subject, f.searchBody, m.receivedDate
                        FROM messagesfts f
                        LEFT JOIN msg_db.messages m ON m.pk = f.messagePk
                        WHERE messagesfts MATCH ?
                        ORDER BY m.receivedDate DESC
                        LIMIT ?
                    """, (match_expr, limit))
                    rows = fc.fetchall()
                    for r in rows:
                        results.append({
                            "message_id": r["messagePk"],
                            "from": r["messageFrom"],
                            "to": r["messageTo"],
                            "subject": r["subject"] or "",
                            "snippet": (r["searchBody"] or "")[:200],
                            "date": format_timestamp(r["receivedDate"])
                        })
            finally:
                fts_conn.close()
        except Exception as e:
            sys.stderr.write(f"FTS search fallback due to: {e}\n")

    if not results:
        conn = get_ro_conn(MESSAGES_DB)
        try:
            c = conn.cursor()
            like_pattern = f"%{query}%"
            c.execute("""
                SELECT pk, accountPk, receivedDate, messageFrom, messageTo, subject, shortBody
                FROM messages
                WHERE subject LIKE ? OR messageFrom LIKE ? OR shortBody LIKE ?
                ORDER BY receivedDate DESC
                LIMIT ?
            """, (like_pattern, like_pattern, like_pattern, limit))
            rows = c.fetchall()
            for r in rows:
                results.append({
                    "message_id": r["pk"],
                    "from": r["messageFrom"],
                    "to": r["messageTo"],
                    "subject": r["subject"] or "",
                    "snippet": r["shortBody"] or "",
                    "date": format_timestamp(r["receivedDate"])
                })
        finally:
            conn.close()

    return results


def spark_compose_email(to, subject="", body="", cc="", bcc=""):
    params = {}
    if subject:
        params["subject"] = subject
    if body:
        params["body"] = body
    if cc:
        params["cc"] = cc
    if bcc:
        params["bcc"] = bcc

    query_str = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
    mailto_url = f"mailto:{to}?{query_str}" if query_str else f"mailto:{to}"

    res = subprocess.run(["open", "-a", "Spark Desktop", mailto_url], capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"Failed to open Spark Desktop composer: {res.stderr}")

    return {
        "status": "opened",
        "to": to,
        "subject": subject,
        "message": "Composer opened in Spark Desktop with prefilled content"
    }


def spark_search_attachment_content(query, limit=20):
    """
    Search INSIDE attachments (PDFs, docs, text) using Spark's indexed attachmentsfts FTS5 database.
    Returns matched snippets, attachment details, email context, and local cached file paths.
    """
    if not query:
        raise ValueError("query argument is required")
    limit = max(1, min(int(limit), 50))

    if not os.path.exists(SEARCH_DB):
        raise FileNotFoundError(f"Search database not found: {SEARCH_DB}")
    if not os.path.exists(MESSAGES_DB):
        raise FileNotFoundError(f"Messages database not found: {MESSAGES_DB}")

    clean_query = query.strip()
    conn = get_ro_conn(SEARCH_DB)
    try:
        conn.execute(f'ATTACH DATABASE "file:{MESSAGES_DB}?mode=ro" AS msg_db')
        c = conn.cursor()

        sql = """
            SELECT f.messagePk, f.attachmentPk, f.chunkIndex, f.text,
                   a.attachmentName, a.attachmentSize, a.attachmentMIMEType, a.attachmentURL,
                   m.accountPk, m.subject, m.messageFrom, m.receivedDate
            FROM attachmentsfts f
            JOIN msg_db.messageAttachment a ON f.attachmentPk = a.pk
            JOIN msg_db.messages m ON f.messagePk = m.pk
            WHERE attachmentsfts MATCH ?
            ORDER BY rank
            LIMIT ?
        """
        try:
            c.execute(sql, (clean_query, limit))
            rows = c.fetchall()
        except sqlite3.OperationalError:
            # FTS5 string literal: a quote inside is escaped by doubling it.
            escaped = ' '.join('"' + t.replace('"', '""') + '"' for t in clean_query.split())
            c.execute(sql, (escaped, limit))
            rows = c.fetchall()

        results = []
        for r in rows:
            cached_file = find_cached_attachment_file(
                r["accountPk"], r["messagePk"], r["attachmentName"], r["attachmentURL"]
            )
            raw_text = r["text"] or ""
            snippet = raw_text[:250] + ("..." if len(raw_text) > 250 else "")

            results.append({
                "message_id": r["messagePk"],
                "attachment_id": r["attachmentPk"],
                "filename": r["attachmentName"],
                "mime_type": r["attachmentMIMEType"] or "application/octet-stream",
                "size_bytes": r["attachmentSize"] or 0,
                "email_subject": r["subject"],
                "email_from": r["messageFrom"],
                "received_date": format_timestamp(r["receivedDate"]),
                "matched_snippet": snippet,
                "cached_file_path": cached_file
            })
        return results
    finally:
        conn.close()


def spark_get_latest_otp(service=None, max_age_hours=24):
    """
    Find recent one-time verification codes (2FA / OTP), password reset tokens, or activation emails.
    Extracts authentication codes and verification URLs.
    """
    max_age_hours = max(1, min(int(max_age_hours), 168))
    since_ts = int(time.time() - (max_age_hours * 3600))

    conn = get_ro_conn(MESSAGES_DB)
    try:
        c = conn.cursor()
        query_clauses = [
            "m.receivedDate >= ?",
            """(
                m.subject LIKE '%code%' OR m.subject LIKE '%verification%' OR m.subject LIKE '%kod%'
                OR m.subject LIKE '%код%' OR m.subject LIKE '%passcode%' OR m.subject LIKE '%token%'
                OR m.subject LIKE '%security%' OR m.subject LIKE '%bestätigung%' OR m.subject LIKE '% pin%'
                OR m.subject LIKE 'pin%' OR m.subject LIKE '%one-time%' OR m.subject LIKE '%otp%'
                OR m.subject LIKE '%authenticat%' OR m.subject LIKE '%authoriz%'
                OR m.subject LIKE '%password%' OR m.subject LIKE '%парол%'
            )"""
        ]
        params = [since_ts]
        if service:
            query_clauses.append("(m.messageFrom LIKE ? OR m.subject LIKE ?)")
            srv_pat = f"%{service}%"
            params.extend([srv_pat, srv_pat])

        sql = f"""
            SELECT m.pk, m.subject, m.messageFrom, m.receivedDate, m.shortBody
            FROM messages m
            WHERE {' AND '.join(query_clauses)}
            ORDER BY m.receivedDate DESC
            LIMIT 10
        """
        c.execute(sql, params)
        candidates = c.fetchall()
    finally:
        conn.close()

    if not candidates:
        return {"status": "not_found", "message": f"No verification/OTP emails found in past {max_age_hours} hours"}

    extracted_items = []
    for cand in candidates:
        msg_pk = cand["pk"]
        subj = cand["subject"] or ""

        # Text body drops <style>/<script>, so CSS colors like #333333 don't look like codes.
        body_text = ' '.join((get_message_body(msg_pk) or cand["shortBody"] or "").split())

        found_code = None
        subj_m = re.search(r'\b((?=[A-Z0-9]*\d)[A-Z0-9]{4,10})\s+is your (?:verification|security|access|login) code', subj, re.IGNORECASE)
        if not subj_m:
            subj_m = re.search(r'(?:code|kod|код|pin)[:\s\t#]+((?=[A-Z0-9]*\d)[A-Z0-9]{4,10})\b', subj, re.IGNORECASE)
        if subj_m:
            found_code = subj_m.group(1)

        if not found_code:
            body_m = re.search(r'(?:verification code|security code|login code|passcode|one-time code|kod|код подтверждения|код авторизации)[^\w]{1,15}((?=[A-Z0-9]*\d)[A-Z0-9]{4,10})\b', body_text, re.IGNORECASE)
            if body_m:
                cand_c = body_m.group(1).strip()
                if cand_c.lower() not in ("please", "enter", "valid", "below", "thank", "your", "only", "code", "mail"):
                    found_code = cand_c

        if not found_code:
            # Bare number only when a code keyword precedes it closely.
            num_m = re.search(r'(?:code|kod|код|pin|otp|passcode|пароль)\D{0,40}?\b([0-9]{4,8})\b', body_text, re.IGNORECASE)
            if num_m:
                found_code = num_m.group(1)

        action_url = None
        for u in re.findall(r'https?://[^\s<>"\'\)]+', body_text):
            if any(k in u.lower() for k in ("verify", "confirm", "activate", "token=", "auth=")):
                action_url = u.rstrip(".,;:)")
                break

        extracted_items.append({
            "message_id": msg_pk,
            "code": found_code,
            "subject": subj,
            "from": cand["messageFrom"] or "",
            "received_date": format_timestamp(cand["receivedDate"]),
            "verification_url": action_url,
            "snippet": body_text[:200]
        })

    with_code = [item for item in extracted_items if item["code"]]
    latest = with_code[0] if with_code else extracted_items[0]

    return {
        "status": "success",
        "latest": latest,
        "recent_verification_emails": extracted_items[:5]
    }


def spark_batch_export_attachments(target_dir, file_extension=None, query=None, sender=None, limit=50, download=True):
    """
    Batch export attachments matching filters to a local directory.
    Uncached ones are downloaded through Spark's CLI when download is true.
    """
    limit = max(1, min(int(limit), 100))
    dest_dir = os.path.expanduser(target_dir)
    os.makedirs(dest_dir, exist_ok=True)

    conn = get_ro_conn(MESSAGES_DB)
    try:
        c = conn.cursor()
        clauses = []
        params = []

        if file_extension:
            ext = file_extension.lstrip(".").lower()
            clauses.append("LOWER(a.attachmentName) LIKE ?")
            params.append(f"%.{ext}")
        if query:
            clauses.append("(LOWER(a.attachmentName) LIKE ? OR LOWER(m.subject) LIKE ?)")
            params.extend([f"%{query.lower()}%", f"%{query.lower()}%"])
        if sender:
            clauses.append("LOWER(m.messageFrom) LIKE ?")
            params.append(f"%{sender.lower()}%")

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"""
            SELECT a.pk as attachmentPk, a.attachmentName, a.attachmentSize, a.attachmentURL,
                   m.pk as messagePk, m.accountPk, m.subject, m.messageFrom, m.receivedDate
            FROM messageAttachment a
            JOIN messages m ON a.messagePk = m.pk
            {where_sql}
            ORDER BY m.receivedDate DESC
            LIMIT ?
        """
        params.append(limit)
        c.execute(sql, params)
        rows = c.fetchall()

        exported = []
        skipped = 0
        download_errors = set()
        for r in rows:
            cached_path = find_cached_attachment_file(r["accountPk"], r["messagePk"], r["attachmentName"], r["attachmentURL"])
            if not cached_path and download:
                try:
                    cached_path = download_attachment_via_cli(r["attachmentPk"])
                except Exception as e:
                    download_errors.add(str(e))
            if not cached_path or not os.path.exists(cached_path):
                skipped += 1
                continue

            safe_name = safe_filename(r["attachmentName"], f"file_{r['attachmentPk']}")
            target_file = os.path.join(dest_dir, f"{r['messagePk']}_{safe_name}")

            shutil.copy2(cached_path, target_file)
            exported.append({
                "attachment_id": r["attachmentPk"],
                "filename": r["attachmentName"],
                "saved_to": target_file,
                "size_bytes": os.path.getsize(target_file),
                "message_id": r["messagePk"],
                "email_subject": r["subject"],
                "from": r["messageFrom"]
            })

        return {
            "status": "success",
            "target_dir": dest_dir,
            "exported_count": len(exported),
            "skipped_count": skipped,
            "download_errors": sorted(download_errors),
            "files": exported
        }
    finally:
        conn.close()


def spark_export_thread(conversation_id=None, message_id=None, output_path=None, format="markdown"):
    """
    Export an entire conversation thread into a structured Markdown or PDF document.
    """
    thread = spark_get_thread(conversation_id=conversation_id, message_id=message_id, format="text", exclude_quoted_history=True)
    if not thread or not thread.get("messages"):
        raise ValueError(f"No thread found for conversation_id={conversation_id}, message_id={message_id}")

    conv_id = thread.get("conversation_id")
    subject = thread.get("subject", "Conversation Thread")
    messages = thread.get("messages", [])

    # spark_get_thread returns only counts; fetch names for the export.
    conn = get_ro_conn(MESSAGES_DB)
    try:
        for m in messages:
            if m.get("attachments_count"):
                rows = conn.execute("SELECT attachmentName FROM messageAttachment WHERE messagePk = ?", (m["message_id"],)).fetchall()
                m["attachments"] = [{"filename": a["attachmentName"] or ""} for a in rows]
    finally:
        conn.close()

    safe_subject = re.sub(r'[\\/*?:"<>|]', '_', subject)[:50]
    fmt = format.lower().strip()

    if not output_path:
        ext = "pdf" if fmt == "pdf" else "md"
        output_path = os.path.expanduser(f"~/Downloads/Thread_{conv_id}_{safe_subject}.{ext}")
    else:
        output_path = os.path.expanduser(output_path)

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    if fmt == "markdown":
        lines = [
            f"# Thread: {subject}",
            f"- **Conversation ID**: `{conv_id}`",
            f"- **Total Messages**: {len(messages)}",
            f"- **Export Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "---",
            ""
        ]
        for idx, m in enumerate(messages, 1):
            lines.append(f"## Message #{idx}")
            lines.append(f"- **From**: {m.get('from', '')}")
            lines.append(f"- **To**: {m.get('to', '')}")
            if m.get("cc"):
                lines.append(f"- **Cc**: {m.get('cc', '')}")
            lines.append(f"- **Date**: {m.get('date', '')}")
            lines.append(f"- **Subject**: {m.get('subject', '')}")
            if m.get("attachments"):
                att_names = ", ".join(f"`{a.get('filename')}`" for a in m.get("attachments"))
                lines.append(f"- **Attachments**: {att_names}")
            lines.append("")
            lines.append("### Content:")
            lines.append("```")
            lines.append(m.get("body", "").strip())
            lines.append("```")
            lines.append("")
            lines.append("---")
            lines.append("")

        with open(output_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        return {
            "status": "success",
            "format": "markdown",
            "file_path": output_path,
            "conversation_id": conv_id,
            "message_count": len(messages),
            "file_size_bytes": os.path.getsize(output_path)
        }

    elif fmt == "pdf":
        html_parts = [
            "<!DOCTYPE html><html><head><meta charset='utf-8'>",
            f"<title>{html.escape(subject)}</title>",
            "<style>",
            "body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 30px; color: #222; line-height: 1.5; }",
            ".header { border-bottom: 2px solid #0066cc; padding-bottom: 12px; margin-bottom: 24px; }",
            ".msg { margin-bottom: 30px; padding: 18px; border: 1px solid #e1e4e8; border-radius: 8px; background: #fff; page-break-inside: avoid; }",
            ".meta { font-size: 13px; color: #586069; margin-bottom: 12px; }",
            ".meta strong { color: #24292e; }",
            ".body { white-space: pre-wrap; font-size: 14px; color: #24292e; }",
            "</style></head><body>",
            f"<div class='header'><h2>{html.escape(subject)}</h2>",
            f"<p>Conversation ID: <code>{conv_id}</code> | Messages: {len(messages)} | Exported: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p></div>"
        ]
        for idx, m in enumerate(messages, 1):
            att_html = ""
            if m.get("attachments"):
                att_list = ", ".join(html.escape(a.get("filename", "")) for a in m.get("attachments"))
                att_html = f"<div><strong>Attachments:</strong> {att_list}</div>"
            html_parts.append(
                f"<div class='msg'>"
                f"<h4>#{idx} - {html.escape(m.get('subject') or '')}</h4>"
                f"<div class='meta'>"
                f"<div><strong>From:</strong> {html.escape(m.get('from') or '')}</div>"
                f"<div><strong>To:</strong> {html.escape(m.get('to') or '')}</div>"
                f"<div><strong>Date:</strong> {html.escape(m.get('date') or '')}</div>"
                f"{att_html}"
                f"</div>"
                f"<div class='body'>{html.escape((m.get('body') or '').strip())}</div>"
                f"</div>"
            )
        html_parts.append("</body></html>")
        full_html = "".join(html_parts)

        if not html_to_pdf(full_html, output_path):
            html_path = output_path.replace(".pdf", ".html")
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(full_html)
            return {
                "status": "success",
                "format": "html",
                "file_path": html_path,
                "conversation_id": conv_id,
                "message_count": len(messages),
                "note": "Chrome not found for PDF printing; saved as HTML."
            }

        return {
            "status": "success",
            "format": "pdf",
            "file_path": output_path,
            "conversation_id": conv_id,
            "message_count": len(messages),
            "file_size_bytes": os.path.getsize(output_path)
        }
    else:
        raise ValueError(f"Unsupported format: {format}. Choose 'markdown' or 'pdf'")


# MCP Server Definitions
TOOLS_SCHEMA = [
    {
        "name": "spark_list_accounts",
        "description": "Get all email accounts configured in Spark Desktop.",
        "inputSchema": {
            "type": "object",
            "properties": {}
        }
    },
    {
        "name": "spark_get_unread_summary",
        "description": "Get a fast breakdown of unread emails and total counts across all accounts and inboxes.",
        "inputSchema": {
            "type": "object",
            "properties": {}
        }
    },
    {
        "name": "spark_find_unreplied_emails",
        "description": "Find personal emails in inbox that have not received an outgoing reply yet.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "older_than_days": {"type": "integer", "description": "Filter emails older than N days (default 0).", "default": 0},
                "account_id": {"type": "integer", "description": "Optional account ID filter."},
                "limit": {"type": "integer", "description": "Max emails to return (default 10).", "default": 10}
            }
        }
    },
    {
        "name": "spark_find_invoices",
        "description": "Find emails with invoices, receipts, and billing PDFs. Use spark_batch_export_attachments to copy them to disk.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Optional filter keyword (vendor, service, or filename)."},
                "limit": {"type": "integer", "description": "Max invoices to return (default 20).", "default": 20}
            }
        }
    },
    {
        "name": "spark_find_deliveries",
        "description": "Find package delivery and order emails (DHL, DPD, Post.at, Amazon, etc.) with tracking links and order numbers.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "description": "Search past N days (default 30).", "default": 30},
                "limit": {"type": "integer", "description": "Max delivery items to return (default 20).", "default": 20}
            }
        }
    },
    {
        "name": "spark_list_threads",
        "description": "List email threads / conversations (grouped view with participant list, total message counts, and status).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "account_id": {"type": "integer", "description": "Filter by account ID."},
                "only_inbox": {"type": "boolean", "description": "Only return threads currently in Inbox.", "default": False},
                "only_unseen": {"type": "boolean", "description": "Only return threads with unread messages.", "default": False},
                "category": {
                    "type": "string",
                    "enum": ["all", "personal", "notifications", "newsletters"],
                    "description": "Filter by Smart category. Default is 'all'."
                },
                "limit": {"type": "integer", "description": "Max threads to return (default 20).", "default": 20},
                "offset": {"type": "integer", "description": "Offset for pagination.", "default": 0}
            }
        }
    },
    {
        "name": "spark_get_digest",
        "description": "Generate an automated digest for the last N days categorized by Personal, Notifications, and Newsletters.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "description": "Number of past days to include in digest (default 1).", "default": 1},
                "account_id": {"type": "integer", "description": "Optional account ID filter."},
                "limit": {"type": "integer", "description": "Max messages listed across categories (default 50, max 200). Totals still cover the whole period.", "default": 50}
            }
        }
    },
    {
        "name": "spark_list_folders",
        "description": "List folders/mailboxes in Spark Desktop, optionally filtered by account ID.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "account_id": {
                    "type": "integer",
                    "description": "Optional account ID to filter folders for a specific account."
                }
            }
        }
    },
    {
        "name": "spark_list_messages",
        "description": "List emails from Spark Desktop with filters (account, folder, category, inbox, unseen, starred).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "account_id": {"type": "integer", "description": "Filter by account ID."},
                "folder_id": {"type": "integer", "description": "Filter by folder ID."},
                "category": {
                    "type": "string",
                    "enum": ["all", "personal", "notifications", "newsletters"],
                    "description": "Filter by Smart category: 'personal' (direct emails from people), 'notifications' (services), or 'newsletters' (marketing). Default is 'all'."
                },
                "only_inbox": {"type": "boolean", "description": "Only return messages currently in Inbox."},
                "only_unseen": {"type": "boolean", "description": "Only return unread emails."},
                "only_starred": {"type": "boolean", "description": "Only return starred/flagged emails."},
                "limit": {"type": "integer", "description": "Max messages to return (default 10, max 100).", "default": 10},
                "offset": {"type": "integer", "description": "Offset for pagination.", "default": 0}
            }
        }
    },
    {
        "name": "spark_get_message",
        "description": "Get the full content (body, subject, sender, recipients, category, language, attachments) of a specific message by its ID.",
        "inputSchema": {
            "type": "object",
            "required": ["message_id"],
            "properties": {
                "message_id": {"type": "integer", "description": "The ID of the message to retrieve."},
                "format": {
                    "type": "string",
                    "enum": ["text", "html"],
                    "description": "Output body format: 'text' (cleaned plain text) or 'html' (raw HTML). Default is 'text'.",
                    "default": "text"
                },
                "exclude_quoted_history": {
                    "type": "boolean",
                    "description": "If true, cuts off old quoted replies and returns only fresh message text to save context tokens.",
                    "default": False
                }
            }
        }
    },
    {
        "name": "spark_get_thread",
        "description": "Get the full conversation / thread history (all related emails sorted chronologically) by conversation_id or message_id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "conversation_id": {"type": "integer", "description": "The conversation/thread ID."},
                "message_id": {"type": "integer", "description": "The ID of any message in the thread."},
                "format": {
                    "type": "string",
                    "enum": ["text", "html"],
                    "description": "Output body format ('text' or 'html'). Default 'text'.",
                    "default": "text"
                },
                "exclude_quoted_history": {
                    "type": "boolean",
                    "description": "If true, isolates new text per message without repeating previous thread quotes.",
                    "default": True
                }
            }
        }
    },
    {
        "name": "spark_get_attachment",
        "description": "Locate an attachment file on disk and inspect details. If Spark has not cached it yet, downloads it via Spark's bundled CLI (needs Spark running and Settings > AI Agents enabled).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "attachment_id": {"type": "integer", "description": "ID of the attachment from spark_get_message or spark_search_attachments."},
                "message_id": {"type": "integer", "description": "Message ID containing the attachment."},
                "filename": {"type": "string", "description": "Name or part of filename of the attachment."},
                "download": {"type": "boolean", "description": "Download the file through Spark if not cached (default true).", "default": True}
            }
        }
    },
    {
        "name": "spark_search_attachments",
        "description": "Search attachments across all emails by filename, extension, or MIME type (e.g. 'invoice', 'pdf', 'png').",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search keyword or filename extension."},
                "mime_type": {"type": "string", "description": "MIME type filter (e.g. 'application/pdf', 'image/')."},
                "limit": {"type": "integer", "description": "Max attachments to return (default 20).", "default": 20}
            }
        }
    },
    {
        "name": "spark_export_email",
        "description": "Export an email to a file on disk (formats: pdf, html, txt, eml).",
        "inputSchema": {
            "type": "object",
            "required": ["message_id", "output_path"],
            "properties": {
                "message_id": {"type": "integer", "description": "ID of the email to export."},
                "output_path": {"type": "string", "description": "Destination file path (e.g. ~/Desktop/invoice.pdf)."},
                "format": {
                    "type": "string",
                    "enum": ["pdf", "html", "txt", "eml"],
                    "description": "Export file format (default 'pdf').",
                    "default": "pdf"
                }
            }
        }
    },
    {
        "name": "spark_parse_calendar_invites",
        "description": "Extract structured meeting details (title, dates, location, notes, status) from .ics calendar attachments in emails.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "message_id": {"type": "integer", "description": "Optional message ID filter."},
                "limit": {"type": "integer", "description": "Max invites to return (default 10).", "default": 10}
            }
        }
    },
    {
        "name": "spark_extract_links",
        "description": "Extract and categorize all links in an email (action links, tracking numbers, documents, unsubscribe URLs).",
        "inputSchema": {
            "type": "object",
            "required": ["message_id"],
            "properties": {
                "message_id": {"type": "integer", "description": "The ID of the message to extract links from."}
            }
        }
    },
    {
        "name": "spark_search_messages",
        "description": "Search emails across all accounts in Spark Desktop using full-text search.",
        "inputSchema": {
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {"type": "string", "description": "Search term or phrase."},
                "limit": {"type": "integer", "description": "Max results to return (default 20).", "default": 20}
            }
        }
    },
    {
        "name": "spark_search_contacts",
        "description": "Search contacts and address book in Spark Desktop by name or email.",
        "inputSchema": {
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {"type": "string", "description": "Contact name or email snippet to search for."},
                "limit": {"type": "integer", "description": "Max contacts to return (default 20).", "default": 20}
            }
        }
    },
    {
        "name": "spark_get_contact_history",
        "description": "Get complete email interaction history with a specific contact (sent and received messages, timeline, frequency).",
        "inputSchema": {
            "type": "object",
            "required": ["email"],
            "properties": {
                "email": {"type": "string", "description": "Email address of the contact."},
                "limit": {"type": "integer", "description": "Max messages to return in history (default 20).", "default": 20}
            }
        }
    },
    {
        "name": "spark_list_subscriptions",
        "description": "List marketing newsletters, subscriptions and mailing lists detected by Spark, including 1-click unsubscribe links.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Max subscriptions to return (default 25).", "default": 25}
            }
        }
    },
    {
        "name": "spark_list_signatures",
        "description": "List saved email signatures configured for user accounts in Spark Desktop.",
        "inputSchema": {
            "type": "object",
            "properties": {}
        }
    },
    {
        "name": "spark_list_calendar_events",
        "description": "List calendar events/meetings from Spark's calendar database.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "start_timestamp": {"type": "integer", "description": "Filter events starting after this Unix timestamp."},
                "end_timestamp": {"type": "integer", "description": "Filter events ending before this Unix timestamp."},
                "query": {"type": "string", "description": "Search text in event title, description or location."},
                "limit": {"type": "integer", "description": "Max events to return (default 20).", "default": 20}
            }
        }
    },
    {
        "name": "spark_compose_email",
        "description": "Open Spark Desktop with a pre-filled composer window to write/send an email.",
        "inputSchema": {
            "type": "object",
            "required": ["to"],
            "properties": {
                "to": {"type": "string", "description": "Recipient email address."},
                "subject": {"type": "string", "description": "Subject of the email."},
                "body": {"type": "string", "description": "Email body content."},
                "cc": {"type": "string", "description": "CC recipients."},
                "bcc": {"type": "string", "description": "BCC recipients."}
            }
        }
    },
    {
        "name": "spark_reply_to_email",
        "description": "Smart reply to an email with auto-matched user signature based on incoming email language (RU/DE/EN).",
        "inputSchema": {
            "type": "object",
            "required": ["message_id", "body"],
            "properties": {
                "message_id": {"type": "integer", "description": "ID of the email to reply to."},
                "body": {"type": "string", "description": "Reply content to write."},
                "cc": {"type": "string", "description": "Optional CC recipients."},
                "bcc": {"type": "string", "description": "Optional BCC recipients."},
                "auto_signature": {"type": "boolean", "description": "Automatically append user signature matching email language (default True).", "default": True}
            }
        }
    },
    {
        "name": "spark_search_attachment_content",
        "description": "Search text INSIDE PDF/document attachments using Spark's SQLite FTS5 index (contracts, SEPA, IBAN, receipts, keywords).",
        "inputSchema": {
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {"type": "string", "description": "Text or keywords to search for inside attachment contents."},
                "limit": {"type": "integer", "description": "Maximum number of results to return (default 20).", "default": 20}
            }
        }
    },
    {
        "name": "spark_get_latest_otp",
        "description": "Find recent one-time verification codes (2FA / OTP), password reset tokens, or activation emails, returning code and confirmation link.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "service": {"type": "string", "description": "Optional service name or sender to filter by (e.g. Ryanair, Booking, Apple)."},
                "max_age_hours": {"type": "integer", "description": "Maximum age of the email in hours (default 24).", "default": 24}
            }
        }
    },
    {
        "name": "spark_batch_export_attachments",
        "description": "Batch export attachments matching filters (file extension, sender, search term) to a local directory. Uncached files are downloaded through Spark's CLI.",
        "inputSchema": {
            "type": "object",
            "required": ["target_dir"],
            "properties": {
                "target_dir": {"type": "string", "description": "Target folder on macOS to copy the files to (e.g. ~/Downloads/invoices)."},
                "file_extension": {"type": "string", "description": "Optional file extension to filter by (e.g. 'pdf', 'xlsx', 'docx')."},
                "query": {"type": "string", "description": "Optional search term to filter attachment name or email subject."},
                "sender": {"type": "string", "description": "Optional sender name or email address."},
                "limit": {"type": "integer", "description": "Maximum number of files to export (default 50).", "default": 50},
                "download": {"type": "boolean", "description": "Download uncached files through Spark (default true).", "default": True}
            }
        }
    },
    {
        "name": "spark_export_thread",
        "description": "Export an entire conversation thread into a structured Markdown or PDF document.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "conversation_id": {"type": "integer", "description": "Thread conversation ID."},
                "message_id": {"type": "integer", "description": "Message ID (used to lookup conversation ID if conversation_id omitted)."},
                "output_path": {"type": "string", "description": "Optional output file path (defaults to ~/Downloads/Thread_<id>.md or .pdf)."},
                "format": {"type": "string", "description": "Output format: 'markdown' (default) or 'pdf'.", "enum": ["markdown", "pdf"], "default": "markdown"}
            }
        }
    }
]

# --- Spark CLI delegation ---------------------------------------------------
# Writes, calendar, drafts, teams and meetings are delegated to Spark's own CLI, the same
# backend as the official Spark MCP. The tool list comes from `spark tools`, the catalog Spark
# publishes, so names, parameters and access levels always match the installed Spark version.
CLI_CATALOG = {}  # tool name -> catalog entry; filled by refresh_cli_catalog()
CLI_OUTPUT_LIMIT = 30000


def refresh_cli_catalog():
    """(Re)load `spark tools`. Leaves the catalog empty when Spark is not running."""
    try:
        tools = json.loads(run_spark_cli(["tools"], timeout=15))["tools"]
    except Exception:
        return
    CLI_CATALOG.clear()
    CLI_CATALOG.update({"spark_cli_" + t["command"].replace("-", "_"): t for t in tools})


def cli_tool_schemas():
    schemas = []
    for name, t in CLI_CATALOG.items():
        props = {}
        for p in t["parameters"]:
            prop = {"type": p["type"], "description": p.get("description", "")}
            if p["type"] == "array":
                prop["items"] = p.get("items", {"type": "string"})
            props[p["name"]] = prop
        schema = {"type": "object", "properties": props}
        required = [p["name"] for p in t["parameters"] if p.get("required")]
        if required:
            schema["required"] = required
        schemas.append({"name": name, "description": t["description"] + " (via Spark CLI)", "inputSchema": schema})
    return schemas


def call_cli_tool(name, kw):
    t = CLI_CATALOG[name]
    params = {p["name"]: p for p in t["parameters"]}
    unknown = set(kw) - set(params)
    if unknown:
        raise ValueError(f"Unknown arguments: {sorted(unknown)}")
    args = [t["command"]]
    for p in t["parameters"]:  # positionals first, in catalog order
        v = kw.get(p["name"])
        if "flag" not in p:
            if v in (None, "", []):
                if p.get("required"):
                    raise ValueError(f"Missing required argument: {p['name']}")
            else:
                args.extend(v if isinstance(v, list) else [v])
    for p in t["parameters"]:
        v = kw.get(p["name"])
        if "flag" not in p or v is None:
            continue
        if p["type"] == "boolean":
            if v:
                args.append(p["flag"])
        else:
            for item in (v if isinstance(v, list) else [v]):
                args.extend([p["flag"], item])
    out = run_spark_cli(args).rstrip()
    if len(out) > CLI_OUTPUT_LIMIT:  # e.g. `search <query>` returns 20 full bodies
        out = out[:CLI_OUTPUT_LIMIT] + f"\n\n[truncated: {len(out)} chars total. Narrow the query/filter or use a smaller page_size.]"
    return {"output": out}


TOOL_NAMES = {t["name"] for t in TOOLS_SCHEMA}


def handle_request(req):
    req_id = req.get("id")
    method = req.get("method")
    params = req.get("params", {})

    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {}
                },
                "serverInfo": {
                    "name": "spark-desktop-mcp",
                    "version": "2.1.0"
                }
            }
        }

    if "id" not in req:
        # JSON-RPC notifications (initialized, cancelled, ...) must never get a response.
        return None

    if method == "ping":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {}
        }

    if method == "tools/list":
        refresh_cli_catalog()
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "tools": TOOLS_SCHEMA + cli_tool_schemas()
            }
        }

    if method == "tools/call":
        tool_name = params.get("name")
        args = params.get("arguments", {})

        try:
            # Schema names are the allowlist; argument defaults live on the functions.
            if tool_name in TOOL_NAMES:
                res = globals()[tool_name](**args)
            else:
                if tool_name not in CLI_CATALOG:
                    refresh_cli_catalog()
                if tool_name not in CLI_CATALOG:
                    raise ValueError(f"Unknown tool: {tool_name} (Spark CLI tools need Spark running)")
                res = call_cli_tool(tool_name, args)

            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(res, ensure_ascii=False, separators=(",", ":"))
                        }
                    ],
                    "isError": False
                }
            }
        except Exception as e:
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": f"Error executing {tool_name}: {str(e)}"
                        }
                    ],
                    "isError": True
                }
            }

    return {
        "jsonrpc": "2.0",
        "id": req_id,
        "error": {
            "code": -32601,
            "message": f"Method not found: {method}"
        }
    }


def main():
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            resp = handle_request(req)
            if resp is not None:
                sys.stdout.write(json.dumps(resp, ensure_ascii=False) + "\n")
                sys.stdout.flush()
        except Exception as e:
            sys.stderr.write(f"Protocol error: {e}\n")
            sys.stderr.flush()


if __name__ == "__main__":
    main()
