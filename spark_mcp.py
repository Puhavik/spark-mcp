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
import io
import json
import base64
import sqlite3
import atexit
import signal
import inspect
import shutil
import time
import subprocess
import urllib.parse
import unicodedata
import csv
import zipfile
import xml.etree.ElementTree as ET
from contextlib import closing
from datetime import datetime
from email.message import EmailMessage
from email.utils import formatdate, getaddresses, parseaddr
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

# Export directories jail: default is ~/Downloads
SPARK_ALLOWED_ROOTS = [os.path.realpath(os.path.expanduser("~/Downloads"))]
_env_roots = os.getenv("SPARK_ALLOWED_ROOTS")
if _env_roots:
    for _r in _env_roots.split(","):
        _r = _r.strip()
        if _r:
            SPARK_ALLOWED_ROOTS.append(os.path.realpath(os.path.expanduser(_r)))


def validate_safe_export_path(path):
    """Ensure export destination path is jailed within allowed roots (default ~/Downloads)."""
    real = os.path.realpath(os.path.abspath(os.path.expanduser(path)))
    if not any(real == root or real.startswith(root + os.sep) for root in SPARK_ALLOWED_ROOTS):
        roots_str = ", ".join(SPARK_ALLOWED_ROOTS)
        raise PermissionError(
            f"Path '{path}' is outside allowed export directories: [{roots_str}]. Allowed default is ~/Downloads."
        )
    return real


# Prompt injection & invisible Unicode sanitizer
_INVISIBLE_CHARS = re.compile(
    "["
    "\u200b"  # zero-width space
    "\u200c"  # zero-width non-joiner
    "\u200d"  # zero-width joiner
    "\u200e"  # left-to-right mark
    "\u200f"  # right-to-left mark
    "\u2028"  # line separator
    "\u2029"  # paragraph separator
    "\u202a-\u202e"  # bidi embedding/override
    "\u2060"  # word joiner
    "\u2061-\u2064"  # invisible operators
    "\ufeff"  # zero width no-break space / BOM
    "\ufff9-\ufffb"  # interlinear annotations
    "]"
)
_EXCESSIVE_NEWLINES = re.compile(r"\n{3,}")


def sanitize_user_content(text, max_length=None):
    """Sanitize untrusted user/email content: strip invisible characters, control chars, and excessive newlines."""
    if not text:
        return ""
    cleaned = []
    for ch in text:
        cat = unicodedata.category(ch)
        if cat in ("Cc", "Cf"):
            if ch in ("\n", "\r", "\t"):
                cleaned.append(ch)
            # drop unprintable control characters
        else:
            cleaned.append(ch)
    result = "".join(cleaned)
    result = _INVISIBLE_CHARS.sub("", result)
    result = _EXCESSIVE_NEWLINES.sub("\n\n", result)
    result = result.strip()
    if max_length and len(result) > max_length:
        result = result[:max_length] + " ...[truncated]"
    return result


def split_sender(sender_str):
    """Split 'Name <email@domain>' into clean ('Name', 'email@domain')."""
    if not sender_str:
        return "", ""
    name, addr = parseaddr(sender_str)
    return sanitize_user_content(name.strip()), addr.strip()


# Tool Exposure Profiles
MUTATING_TOOLS = {
    "spark_compose_email",
    "spark_reply_to_email",
    "spark_export_email",
    "spark_export_thread",
    "spark_batch_export_attachments",
    "spark_cli_action",
    "spark_cli_draft",
    "spark_cli_event",
    "spark_cli_comment",
    "spark_cli_contact_action",
}

CORE_TOOLS = {
    "spark_get_unread_summary",
    "spark_list_threads",
    "spark_list_messages",
    "spark_get_message",
    "spark_get_thread",
    "spark_search_messages",
    "spark_inspect_attachment",
    "spark_inspect_document",
    "inspect_document",
    "spark_find_invoices",
    "spark_compose_email",
    "spark_reply_to_email",
}


def is_tool_exposed(name):
    """Check if tool is allowed under SPARK_EXPOSED_TOOLS environment variable."""
    raw = os.getenv("SPARK_EXPOSED_TOOLS", "all").strip().lower()
    if raw == "all" or not raw:
        return True
    if raw == "core":
        return name in CORE_TOOLS
    if raw.startswith("read-only"):
        extra = set()
        if "+" in raw:
            _, plus = raw.split("+", 1)
            extra = {t.strip() for t in plus.split(",") if t.strip()}
        if name.lower() in extra:
            return True
        return name not in MUTATING_TOOLS
    allowed = {t.strip() for t in raw.split(",") if t.strip()}
    return name.lower() in allowed


def get_tool_annotations(name):
    """Return MCP 2024-11 tool annotations (readOnlyHint, destructiveHint, idempotentHint)."""
    if name in MUTATING_TOOLS:
        is_destructive = name in {"spark_cli_draft", "spark_cli_action"}
        return {
            "readOnlyHint": False,
            "destructiveHint": is_destructive,
            "idempotentHint": name.startswith("spark_export")
        }
    return {
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True
    }


CATEGORY_MAP = {
    0: "other",
    1: "personal",
    2: "notifications",
    3: "newsletters",
    5: "pinned",
    6: "system"
}
REVERSE_CATEGORY_MAP = {v: k for k, v in CATEGORY_MAP.items()}


_CONN_CACHE = {}


class CursorProxy:
    """Wrapper around sqlite3.Cursor that ignores duplicate ATTACH errors."""
    def __init__(self, cur):
        self._cur = cur

    def execute(self, sql, *args):
        if isinstance(sql, str) and sql.strip().upper().startswith("ATTACH DATABASE"):
            try:
                return self._cur.execute(sql, *args)
            except sqlite3.OperationalError as e:
                if "already in use" in str(e):
                    return self
                raise
        res = self._cur.execute(sql, *args)
        return self if res is self._cur else res

    def executemany(self, sql, *args):
        return self._cur.executemany(sql, *args)

    def __iter__(self):
        return iter(self._cur)

    def __getattr__(self, name):
        return getattr(self._cur, name)


class PersistentConnProxy:
    """Wrapper that prevents closing() from destroying long-lived cached SQLite connections."""
    def __init__(self, conn):
        self._conn = conn

    def close(self):
        # Keep underlying SQLite connection open across requests
        pass

    def cursor(self):
        return CursorProxy(self._conn.cursor())

    def execute(self, sql, *args):
        if isinstance(sql, str) and sql.strip().upper().startswith("ATTACH DATABASE"):
            try:
                return self._conn.execute(sql, *args)
            except sqlite3.OperationalError as e:
                if "already in use" in str(e):
                    return None
                raise
        return self._conn.execute(sql, *args)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __getattr__(self, name):
        return getattr(self._conn, name)


def get_ro_conn(db_path):
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found: {db_path}")
    real_path = os.path.realpath(db_path)
    if real_path in _CONN_CACHE:
        return _CONN_CACHE[real_path]

    uri = f"file:{real_path}?mode=ro"
    # isolation_level=None enables autocommit mode to release WAL read locks immediately after SELECT
    conn = sqlite3.connect(uri, uri=True, timeout=20.0, cached_statements=256, isolation_level=None)
    conn.execute("PRAGMA query_only = 1;")
    conn.execute("PRAGMA busy_timeout = 20000;")
    conn.execute("PRAGMA temp_store = MEMORY;")
    conn.execute("PRAGMA cache_size = -64000;")
    try:
        conn.execute("PRAGMA mmap_size = 268435456;")
    except Exception:
        pass
    conn.row_factory = sqlite3.Row
    # Spark stores raw folded headers ("Name\r\n <addr>"); unfold them for every TEXT column.
    conn.text_factory = lambda b: b.decode("utf-8", errors="replace").replace("\r\n ", " ").replace("\r\n\t", " ")

    proxy = PersistentConnProxy(conn)
    _CONN_CACHE[real_path] = proxy
    return proxy


def close_all_connections():
    for p, proxy in list(_CONN_CACHE.items()):
        try:
            proxy._conn.close()
        except Exception:
            pass
    _CONN_CACHE.clear()


atexit.register(close_all_connections)


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
        lines = (line.strip() for line in "".join(self.text_parts).splitlines())
        raw = re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()
        return sanitize_user_content(raw)


def format_timestamp(ts):
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts)


def parse_date_to_timestamp(val, end_of_day=False):
    """
    Parse a date string or timestamp into a Unix timestamp (seconds).
    Supports:
      - Raw int/float or numeric string (e.g. 1728000000)
      - ISO 8601 strings (e.g. '2026-10-01', '2026-10-01T14:30:00', '2026-10-01 14:30:00')
      - Common date formats ('%Y-%m-%d', '%Y/%m/%d', '%d.%m.%Y')
    If end_of_day is True and only a date is provided, sets time to 23:59:59.
    """
    if val is None or val == "":
        return None
    if isinstance(val, (int, float)):
        return float(val)
    val_str = str(val).strip()
    try:
        return float(val_str)
    except ValueError:
        pass
    try:
        dt = datetime.fromisoformat(val_str)
        if end_of_day and len(val_str) <= 10:
            dt = dt.replace(hour=23, minute=59, second=59)
        return dt.timestamp()
    except Exception:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%d.%m.%Y"):
            try:
                dt = datetime.strptime(val_str, fmt)
                if end_of_day and fmt in ("%Y/%m/%d", "%d.%m.%Y"):
                    dt = dt.replace(hour=23, minute=59, second=59)
                return dt.timestamp()
            except Exception:
                pass
        raise ValueError(f"Unrecognized date format: '{val}'. Expected YYYY-MM-DD, ISO string, or timestamp.")


def get_message_body(message_id, format="text"):
    if not os.path.exists(CACHE_DB):
        return None
    with closing(get_ro_conn(CACHE_DB)) as cache_conn:
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


def get_message_parsed_info(message_id):
    if not os.path.exists(CACHE_DB):
        return None, None
    with closing(get_ro_conn(CACHE_DB)) as cache_conn:
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
                        clean_text = sanitize_user_content("\n\n".join(text_parts))
            except Exception:
                pass
        return clean_text, lang


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


def check_new_file(path):
    """Export targets must stay within allowed roots (default ~/Downloads) and not exist yet."""
    validate_safe_export_path(path)
    if os.path.lexists(path):
        raise FileExistsError(f"Refusing to overwrite existing file: {path}")


def safe_filename(name, fallback):
    """Strip directories and path-hostile chars from a sender-controlled attachment name."""
    base = os.path.basename(name or "") or fallback
    return re.sub(r'[\\/*?:"<>|]', '_', base).lstrip(".") or fallback


# Tool implementations
def spark_list_accounts():
    with closing(get_ro_conn(MESSAGES_DB)) as conn:
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


def spark_get_unread_summary():
    with closing(get_ro_conn(MESSAGES_DB)) as conn:
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


def spark_find_unreplied_emails(older_than_days=0, account_id=None, limit=10):
    limit = max(1, min(int(limit), 50))
    now_ts = int(datetime.now().timestamp())
    cutoff_ts = now_ts - (int(older_than_days) * 86400)

    with closing(get_ro_conn(MESSAGES_DB)) as conn:
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


def spark_list_folders(account_id=None):
    with closing(get_ro_conn(MESSAGES_DB)) as conn:
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


def spark_list_threads(account_id=None, only_inbox=False, only_unseen=False, category=None, limit=20, offset=0, cursor=None):
    if cursor is not None:
        try:
            offset = int(cursor)
        except (ValueError, TypeError):
            pass
    limit = max(1, min(int(limit), 100))
    offset = max(0, int(offset))

    with closing(get_ro_conn(MESSAGES_DB)) as conn:
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
        params.extend([limit + 1, offset])

        c.execute(sql, params)
        rows = c.fetchall()
        has_more = len(rows) > limit
        if has_more:
            rows = rows[:limit]
        next_cursor = offset + limit if has_more else None

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
        return {
            "items": threads,
            "has_more": has_more,
            "next_cursor": next_cursor
        }


def spark_list_messages(account_id=None, folder_id=None, category=None, only_inbox=False, only_unseen=False, only_starred=False, limit=10, offset=0, days=None, since_date=None, until_date=None, cursor=None):
    if cursor is not None:
        try:
            offset = int(cursor)
        except (ValueError, TypeError):
            pass
    limit = max(1, min(int(limit), 100))
    offset = max(0, int(offset))

    with closing(get_ro_conn(MESSAGES_DB)) as conn:
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
        if days is not None:
            try:
                d_val = float(days)
            except (ValueError, TypeError):
                raise ValueError(f"Invalid days value: {days}. Expected a number.")
            if d_val < 0:
                raise ValueError(f"days must be non-negative, got {days}")
            where_clauses.append("m.receivedDate >= ?")
            params.append(time.time() - (d_val * 86400))
        if since_date is not None:
            s_ts = parse_date_to_timestamp(since_date)
            if s_ts is not None:
                where_clauses.append("m.receivedDate >= ?")
                params.append(s_ts)
        if until_date is not None:
            u_ts = parse_date_to_timestamp(until_date, end_of_day=True)
            if u_ts is not None:
                where_clauses.append("m.receivedDate <= ?")
                params.append(u_ts)

        if where_clauses:
            query += " AND " + " AND ".join(where_clauses)

        query += " ORDER BY m.receivedDate DESC LIMIT ? OFFSET ?"
        params.extend([limit + 1, offset])

        c.execute(query, params)
        rows = c.fetchall()
        has_more = len(rows) > limit
        if has_more:
            rows = rows[:limit]
        next_cursor = offset + limit if has_more else None

        messages = []
        for r in rows:
            cat_val = r["category"]
            from_name, from_email = split_sender(r["messageFrom"])
            messages.append({
                "message_id": r["pk"],
                "conversation_id": r["conversationPk"],
                "account_id": r["accountPk"],
                "date": format_timestamp(r["receivedDate"]),
                "from": r["messageFrom"],
                "from_name": from_name,
                "from_email": from_email,
                "to": r["messageTo"],
                "subject": sanitize_user_content(r["subject"] or ""),
                "snippet": sanitize_user_content(r["shortBody"] or ""),
                "category": CATEGORY_MAP.get(cat_val, "other"),
                "unseen": bool(r["unseen"]),
                "starred": bool(r["starred"]),
                "in_inbox": bool(r["inInbox"]),
                "attachments_count": r["numberOfFileAttachments"] or 0
            })
        return {
            "items": messages,
            "has_more": has_more,
            "next_cursor": next_cursor
        }


def spark_get_message(message_id, format="text", exclude_quoted_history=False):
    message_id = int(message_id)

    with closing(get_ro_conn(MESSAGES_DB)) as meta_conn:
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

    clean_unquoted, detected_lang = get_message_parsed_info(message_id)

    if exclude_quoted_history and clean_unquoted:
        body = clean_unquoted
    else:
        body = get_message_body(message_id, format=format)
        if not body:
            body = clean_unquoted or msg["shortBody"] or ""

    from_name, from_email = split_sender(msg["messageFrom"])
    return {
        "message_id": msg["pk"],
        "conversation_id": msg["conversationPk"],
        "account_id": msg["accountPk"],
        "received_date": format_timestamp(msg["receivedDate"]),
        "from": msg["messageFrom"],
        "from_name": from_name,
        "from_email": from_email,
        "to": msg["messageTo"],
        "cc": msg["messageCc"],
        "bcc": msg["messageBcc"],
        "subject": sanitize_user_content(msg["subject"] or ""),
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

    with closing(get_ro_conn(MESSAGES_DB)) as conn:
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

            from_name, from_email = split_sender(r["messageFrom"])
            messages.append({
                "message_id": m_pk,
                "account_id": r["accountPk"],
                "date": format_timestamp(r["receivedDate"]),
                "from": r["messageFrom"],
                "from_name": from_name,
                "from_email": from_email,
                "to": r["messageTo"],
                "cc": r["messageCc"],
                "subject": sanitize_user_content(r["subject"] or ""),
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


def spark_get_attachment(attachment_id=None, message_id=None, filename=None):
    with closing(get_ro_conn(MESSAGES_DB)) as conn:
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

        return {
            "attachment_id": att["pk"],
            "message_id": msg_pk,
            "filename": att_name,
            "size_bytes": att["attachmentSize"],
            "mime_type": att["attachmentMIMEType"],
            "cached_locally": found_path is not None,
            "file_path": found_path
        }


def spark_inspect_attachment(attachment_id=None, message_id=None, filename=None, **kwargs):
    """
    Inspect and read the contents of an attachment (PDF, image, document, text)
    entirely in memory (RAM, no disk footprint), like inspect_document in telegram-mcp.

    Returns extracted text for PDFs and documents, or an image block for photos/scans
    to allow instant visual verification in Claude.

    Args:
        attachment_id: ID of the attachment (pk) from messageAttachment table.
        message_id: ID of the email message containing the attachment.
        filename: Optional name or substring of attachment filename to match.
    """
    with closing(get_ro_conn(MESSAGES_DB)) as conn:
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
            if message_id:
                return f"There is no attached document or media file in message {message_id}."
            return "Attachment not found in database."

        att_id = att["pk"]
        att_name = att["attachmentName"] or "attachment"
        account_pk = att["accountPk"]
        msg_pk = att["messagePk"]
        att_url = att["attachmentURL"]
        mime_type = (att["attachmentMIMEType"] or "").lower()
        ext = os.path.splitext(att_name)[1].lower()

        data = None
        found_path = find_cached_attachment_file(account_pk, msg_pk, att_name, att_url)
        if found_path and os.path.isfile(found_path):
            with open(found_path, "rb") as f:
                data = f.read()
        elif os.path.exists(SPARK_CLI):
            try:
                proc = subprocess.run(
                    [SPARK_CLI, "attachment", str(att_id), "--stream"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=30
                )
                if proc.returncode == 0 and proc.stdout:
                    data = proc.stdout
            except Exception:
                pass

        if not data:
            if os.path.exists(SEARCH_DB):
                try:
                    with closing(get_ro_conn(SEARCH_DB)) as sconn:
                        rows = sconn.execute(
                            "SELECT text FROM attachmentsfts WHERE attachmentPk = ? ORDER BY chunkIndex ASC",
                            (att_id,)
                        ).fetchall()
                        text_chunks = [r["text"] for r in rows if r["text"]]
                        if text_chunks:
                            return f"Contents of document '{att_name}' (extracted from Spark index):\n\n" + "\n\n".join(text_chunks)
                except Exception:
                    pass
            return f"Attachment '{att_name}' (ID {att_id}) is not cached locally. Run Spark with CLI or download it first."

        # 1. If this is an image or scan
        if mime_type.startswith("image/") or ext in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
            fmt = (
                "png"
                if (ext == ".png" or mime_type == "image/png")
                else (
                    "webp"
                    if (ext == ".webp" or mime_type == "image/webp")
                    else ("gif" if (ext == ".gif" or mime_type == "image/gif") else "jpeg")
                )
            )
            b64_data = base64.b64encode(data).decode("ascii")
            del data
            return {
                "_mcp_content": [
                    {
                        "type": "image",
                        "data": b64_data,
                        "mimeType": f"image/{fmt}"
                    }
                ]
            }

        # 2. If this is a PDF
        if mime_type == "application/pdf" or ext == ".pdf":
            text_pages = []
            has_any_text = False
            read_error = None
            try:
                try:
                    from pypdf import PdfReader
                except ImportError:
                    from PyPDF2 import PdfReader
                reader = PdfReader(io.BytesIO(data))
                for i, page in enumerate(reader.pages):
                    extracted = (page.extract_text() or "").strip()
                    if extracted:
                        has_any_text = True
                    text_pages.append(f"--- Page {i + 1} ---\n{extracted}")
            except Exception as e:
                read_error = e

            del data

            if has_any_text:
                full_text = "\n\n".join(text_pages).strip()
                return f"Contents of document '{att_name}' ({len(text_pages)} pages):\n\n{full_text}"

            if os.path.exists(SEARCH_DB):
                try:
                    with closing(get_ro_conn(SEARCH_DB)) as sconn:
                        rows = sconn.execute(
                            "SELECT text FROM attachmentsfts WHERE attachmentPk = ? ORDER BY chunkIndex ASC",
                            (att_id,)
                        ).fetchall()
                        text_chunks = [r["text"] for r in rows if r["text"]]
                        if text_chunks:
                            return f"Contents of document '{att_name}' (extracted from Spark index):\n\n" + "\n\n".join(text_chunks)
                except Exception:
                    pass

            if read_error and not text_pages:
                return f"Error reading PDF '{att_name}': {str(read_error)}"

            pages_count = len(text_pages) if text_pages else 0
            pages_info = f" ({pages_count} pages)" if pages_count else ""
            return (
                f"Document '{att_name}'{pages_info} "
                "does not contain a text layer (possibly a scanned image without OCR)."
            )

        # 3. If this is a Word document (.docx)
        if ext == ".docx" or mime_type in {
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "application/msword"
        }:
            try:
                with zipfile.ZipFile(io.BytesIO(data)) as z:
                    xml_content = z.read("word/document.xml")
                root = ET.fromstring(xml_content)
                del xml_content
                del data
                paragraphs = []
                for p in root.iter():
                    if p.tag.endswith("}p"):
                        texts = [node.text for node in p.iter() if node.tag.endswith("}t") and node.text]
                        if texts:
                            paragraphs.append("".join(texts))
                full_text = "\n\n".join(paragraphs).strip()
                if full_text:
                    return f"Contents of Word document '{att_name}':\n\n{sanitize_user_content(full_text)}"
                return f"Word document '{att_name}' does not contain readable text."
            except Exception as e:
                return f"Error reading Word document '{att_name}': {e}"

        # 4. If this is an Excel spreadsheet (.xlsx)
        if ext == ".xlsx" or mime_type in {
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/vnd.ms-excel"
        }:
            try:
                with zipfile.ZipFile(io.BytesIO(data)) as z:
                    shared_strings = []
                    if "xl/sharedStrings.xml" in z.namelist():
                        sst_root = ET.fromstring(z.read("xl/sharedStrings.xml"))
                        for si in sst_root.iter():
                            if si.tag.endswith("}si"):
                                t_nodes = [node.text for node in si.iter() if node.tag.endswith("}t") and node.text]
                                shared_strings.append("".join(t_nodes))

                    sheet_name = "xl/worksheets/sheet1.xml"
                    if sheet_name not in z.namelist():
                        sheets = [n for n in z.namelist() if n.startswith("xl/worksheets/sheet") and n.endswith(".xml")]
                        sheet_name = sheets[0] if sheets else None

                    if not sheet_name:
                        return f"Spreadsheet '{att_name}' contains no readable worksheets."

                    sheet_root = ET.fromstring(z.read(sheet_name))
                del data

                rows_text = []
                for row in sheet_root.iter():
                    if row.tag.endswith("}row"):
                        cells = []
                        for c in row.iter():
                            if c.tag.endswith("}c"):
                                cell_type = c.get("t")
                                val_node = next((n for n in c if n.tag.endswith("}v")), None)
                                val = val_node.text if val_node is not None and val_node.text else ""
                                if cell_type == "s" and val.isdigit():
                                    idx = int(val)
                                    val = shared_strings[idx] if idx < len(shared_strings) else val
                                elif cell_type == "inlineStr":
                                    is_t = next((n for n in c.iter() if n.tag.endswith("}t") and n.text), None)
                                    val = is_t.text if is_t else val
                                cells.append(val.strip())
                        if any(cells):
                            rows_text.append(" | ".join(cells))

                if rows_text:
                    full_text = "\n".join(rows_text[:200])
                    if len(rows_text) > 200:
                        full_text += f"\n\n[... {len(rows_text) - 200} more rows truncated]"
                    return f"Contents of spreadsheet '{att_name}' ({len(rows_text)} rows):\n\n{sanitize_user_content(full_text)}"
                return f"Spreadsheet '{att_name}' is empty."
            except Exception as e:
                return f"Error reading spreadsheet '{att_name}': {e}"

        # 5. If this is a text file (TXT, CSV, JSON, MD, LOG, XML, HTML, ICS, etc.)
        if (
            mime_type.startswith("text/")
            or ext in {
                ".txt", ".csv", ".json", ".md", ".log", ".yaml", ".yml", ".xml", ".html", ".ics", ".rtf", ".tsv"
            }
            or mime_type in {
                "application/json", "application/xml", "application/javascript", "text/calendar"
            }
        ):
            try:
                text_content = data.decode("utf-8")
            except UnicodeDecodeError:
                text_content = data.decode("latin-1", errors="replace")
            del data
            return sanitize_user_content(text_content)

        del data
        return (
            f"File format '{att_name}' ({mime_type}) "
            "is not currently supported for direct text analysis."
        )


spark_inspect_document = spark_inspect_attachment
inspect_document = spark_inspect_attachment


def spark_search_attachments(query=None, mime_type=None, limit=20):
    limit = max(1, min(int(limit), 50))
    with closing(get_ro_conn(MESSAGES_DB)) as conn:
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


def spark_find_invoices(query=None, limit=20):
    limit = max(1, min(int(limit), 50))
    with closing(get_ro_conn(MESSAGES_DB)) as conn:
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


def spark_find_deliveries(days=30, limit=20):
    limit = max(1, min(int(limit), 50))
    since_ts = int(datetime.now().timestamp()) - (int(days) * 86400)

    with closing(get_ro_conn(MESSAGES_DB)) as conn:
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
            link_info = spark_extract_links(m_id, _html=get_message_body(m_id, format="html") or "")
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


def spark_list_subscriptions(limit=25):
    limit = max(1, min(int(limit), 100))
    with closing(get_ro_conn(MESSAGES_DB)) as conn:
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


def spark_get_contact_history(email, limit=20):
    limit = max(1, min(int(limit), 50))
    with closing(get_ro_conn(MESSAGES_DB)) as conn:
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


def spark_search_contacts(query, limit=20):
    limit = max(1, min(int(limit), 50))
    if not os.path.exists(CONTACTS_DB):
        return []

    with closing(get_ro_conn(CONTACTS_DB)) as conn:
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


def spark_list_signatures():
    if not os.path.exists(SETTINGS_DB):
        return []

    with closing(get_ro_conn(SETTINGS_DB)) as conn:
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
                    sig_html = d.get("htmlContent") or ""
                    parser = HTMLToTextParser()
                    parser.feed(sig_html)
                    signatures.append({
                        "id": d.get("identifier"),
                        "text": parser.get_text(),
                        "html": sig_html
                    })
            except Exception:
                pass
        return signatures


def spark_get_digest(days=1, account_id=None, limit=50):
    days = max(1, min(int(days), 30))
    limit = max(1, min(int(limit), 200))
    since_ts = int(datetime.now().timestamp()) - (days * 86400)

    with closing(get_ro_conn(MESSAGES_DB)) as conn:
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


def spark_export_email(message_id, output_path=None, format="html"):
    format_lower = format.lower()
    msg = spark_get_message(message_id, format="html" if format_lower == "html" else "text")
    if not output_path:
        out_file = os.path.join(SPARK_ALLOWED_ROOTS[0], f"Email_{message_id}.{format_lower}")
    elif not os.path.isabs(os.path.expanduser(output_path)):
        out_file = os.path.join(SPARK_ALLOWED_ROOTS[0], output_path)
    else:
        out_file = os.path.expanduser(output_path)
    check_new_file(out_file)
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

    elif format_lower == "html":
        full_html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; img-src data:">
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
  <h1>{html.escape(msg['subject'] or '')}</h1>
  <div class="meta-row"><strong>From:</strong> {html.escape(msg['from'] or '')}</div>
  <div class="meta-row"><strong>To:</strong> {html.escape(msg['to'] or '')}</div>
  <div class="meta-row"><strong>Date:</strong> {html.escape(msg['received_date'] or '')}</div>
</div>
<div class="body">
{msg['body']}
</div>
</body>
</html>"""

        with open(out_file, "w", encoding="utf-8") as f:
            f.write(full_html)

    else:
        raise ValueError(f"Unsupported format: {format}. Supported: html, txt, eml")

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
    with closing(get_ro_conn(MESSAGES_DB)) as conn:
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


def spark_extract_links(message_id, _html=None):
    # _html: caller already has the HTML body (skips the full spark_get_message).
    msg = {} if _html is not None else spark_get_message(message_id, format="html")
    html_content = _html if _html is not None else msg.get("body", "")

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
    with closing(get_ro_conn(MESSAGES_DB)) as conn:
        row = conn.execute("""
            SELECT m.messageReplyToMailbox AS reply_to, a.ownerFullName AS owner
            FROM messages m LEFT JOIN accounts a ON a.pk = m.accountPk
            WHERE m.pk = ?
        """, (int(message_id),)).fetchone()
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

    with closing(get_ro_conn(CALENDAR_DB)) as conn:
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


def spark_search_messages(query, limit=20, sort_by="relevance"):
    if not (query or "").strip():
        raise ValueError("query argument is required")
    limit = max(1, min(int(limit), 50))
    sort_by_norm = "date" if str(sort_by).lower() == "date" else "relevance"
    results = []

    if os.path.exists(SEARCH_DB):
        try:
            with closing(get_ro_conn(SEARCH_DB)) as fts_conn:
                fts_conn.execute(f'ATTACH DATABASE "file:{MESSAGES_DB}?mode=ro" AS msg_db')
                fc = fts_conn.cursor()
                clean_query = "".join(c if c.isalnum() or c.isspace() else " " for c in query).strip()
                if clean_query:
                    match_expr = " ".join(f'"{word}"*' for word in clean_query.split())
                    order_clause = "relevance ASC, m.receivedDate DESC" if sort_by_norm == "relevance" else "m.receivedDate DESC"
                    sql = f"""
                        SELECT f.messagePk, f.messageFrom, f.messageTo, f.subject, f.searchBody,
                               snippet(messagesfts, 4, '<mark>', '</mark>', '...', 25) AS match_snippet,
                               bm25(messagesfts, 0.0, 5.0, 2.0, 10.0, 1.0, 0.0) AS relevance,
                               m.receivedDate
                        FROM messagesfts f
                        LEFT JOIN msg_db.messages m ON m.pk = f.messagePk
                        WHERE messagesfts MATCH ?
                        ORDER BY {order_clause}
                        LIMIT ?
                    """
                    fc.execute(sql, (match_expr, limit))
                    rows = fc.fetchall()
                    for r in rows:
                        from_name, from_email = split_sender(r["messageFrom"])
                        snippet_text = r["match_snippet"] if "match_snippet" in r.keys() and r["match_snippet"] else (r["searchBody"] or "")[:200]
                        results.append({
                            "message_id": r["messagePk"],
                            "from": r["messageFrom"],
                            "from_name": from_name,
                            "from_email": from_email,
                            "to": r["messageTo"],
                            "subject": sanitize_user_content(r["subject"] or ""),
                            "snippet": sanitize_user_content(snippet_text),
                            "date": format_timestamp(r["receivedDate"])
                        })
        except Exception as e:
            sys.stderr.write(f"FTS search fallback due to: {e}\n")

    if not results:
        with closing(get_ro_conn(MESSAGES_DB)) as conn:
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
                from_name, from_email = split_sender(r["messageFrom"])
                results.append({
                    "message_id": r["pk"],
                    "from": r["messageFrom"],
                    "from_name": from_name,
                    "from_email": from_email,
                    "to": r["messageTo"],
                    "subject": sanitize_user_content(r["subject"] or ""),
                    "snippet": sanitize_user_content(r["shortBody"] or ""),
                    "date": format_timestamp(r["receivedDate"])
                })

    return results


def resolve_email_recipient(to_str):
    """
    Resolve recipient string into comma-separated bare email addresses.
    If a recipient is a human or contact name without '@', searches Spark contacts database.
    """
    if not to_str or not str(to_str).strip():
        raise ValueError("Recipient address or name cannot be empty.")

    to_str = str(to_str).strip()
    reader = csv.reader([to_str], skipinitialspace=True)
    items = next(reader, [])

    resolved = []
    for item in items:
        item = item.strip()
        if not item:
            continue
        if "@" in item:
            addrs = [a for _, a in getaddresses([item]) if a and "@" in a]
            if addrs:
                resolved.extend(addrs)
            else:
                resolved.append(item)
        else:
            contacts = spark_search_contacts(item, limit=1)
            if contacts and contacts[0].get("email"):
                resolved.append(contacts[0]["email"])
            else:
                raise ValueError(f"Could not resolve contact '{item}' to an email address.")

    if not resolved:
        raise ValueError(f"No valid email addresses found in '{to_str}'.")
    return ",".join(resolved)


def spark_compose_email(to, subject="", body="", cc="", bcc=""):
    to_bare = resolve_email_recipient(to)

    params = {}
    if subject:
        params["subject"] = subject
    if body:
        params["body"] = body
    if cc:
        params["cc"] = resolve_email_recipient(cc)
    if bcc:
        params["bcc"] = resolve_email_recipient(bcc)

    query_str = urllib.parse.urlencode(params, safe="@,", quote_via=urllib.parse.quote)
    to_q = urllib.parse.quote(to_bare, safe=",@")
    mailto_url = f"mailto:{to_q}?{query_str}" if query_str else f"mailto:{to_q}"

    res = subprocess.run(["open", "-a", "Spark Desktop", mailto_url], capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"Failed to open Spark Desktop composer: {res.stderr}")

    out = {
        "status": "opened",
        "to": to_bare,
        "subject": subject,
        "message": "Composer opened in Spark Desktop with prefilled content"
    }
    if to != to_bare:
        out["original_to"] = to
    return out


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
    with closing(get_ro_conn(SEARCH_DB)) as conn:
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


def spark_get_latest_otp(service=None, max_age_hours=24):
    """
    Find recent one-time verification codes (2FA / OTP), password reset tokens, or activation emails.
    Extracts authentication codes and verification URLs.
    """
    max_age_hours = max(1, min(int(max_age_hours), 168))
    since_ts = int(time.time() - (max_age_hours * 3600))

    with closing(get_ro_conn(MESSAGES_DB)) as conn:
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


def spark_batch_export_attachments(target_dir=None, file_extension=None, query=None, sender=None, limit=50, progress_callback=None):
    """
    Batch export cached attachments matching filters to a local directory (jailed to allowed roots).
    """
    limit = max(1, min(int(limit), 100))
    if not target_dir:
        dest_dir = SPARK_ALLOWED_ROOTS[0]
    elif not os.path.isabs(os.path.expanduser(target_dir)):
        dest_dir = os.path.join(SPARK_ALLOWED_ROOTS[0], target_dir)
    else:
        dest_dir = os.path.expanduser(target_dir)
    validate_safe_export_path(dest_dir)
    os.makedirs(dest_dir, exist_ok=True)

    with closing(get_ro_conn(MESSAGES_DB)) as conn:
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

        total_rows = len(rows)
        exported = []
        skipped = 0
        for i, r in enumerate(rows):
            if progress_callback:
                progress_callback(i + 1, total_rows, f"Exporting {r['attachmentName']}")
            cached_path = find_cached_attachment_file(r["accountPk"], r["messagePk"], r["attachmentName"], r["attachmentURL"])
            if not cached_path or not os.path.exists(cached_path):
                skipped += 1
                continue

            safe_name = safe_filename(r["attachmentName"], f"file_{r['attachmentPk']}")
            target_file = os.path.join(dest_dir, f"{r['messagePk']}_{safe_name}")
            if os.path.lexists(target_file):
                skipped += 1
                continue

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
            "files": exported
        }


def spark_export_thread(conversation_id=None, message_id=None, output_path=None, format="markdown"):
    """
    Export an entire conversation thread into a structured Markdown document.
    """
    thread = spark_get_thread(conversation_id=conversation_id, message_id=message_id, format="text", exclude_quoted_history=True)
    if not thread or not thread.get("messages"):
        raise ValueError(f"No thread found for conversation_id={conversation_id}, message_id={message_id}")

    conv_id = thread.get("conversation_id")
    subject = thread.get("subject", "Conversation Thread")
    messages = thread.get("messages", [])

    # spark_get_thread returns only counts; fetch names for the export.
    with closing(get_ro_conn(MESSAGES_DB)) as conn:
        for m in messages:
            if m.get("attachments_count"):
                rows = conn.execute("SELECT attachmentName FROM messageAttachment WHERE messagePk = ?", (m["message_id"],)).fetchall()
                m["attachments"] = [{"filename": a["attachmentName"] or ""} for a in rows]

    safe_subject = re.sub(r'[\\/*?:"<>|]', '_', subject)[:50]
    fmt = format.lower().strip()

    if not output_path:
        output_path = os.path.join(SPARK_ALLOWED_ROOTS[0], f"Thread_{conv_id}_{safe_subject}.md")
    elif not os.path.isabs(os.path.expanduser(output_path)):
        output_path = os.path.join(SPARK_ALLOWED_ROOTS[0], output_path)
    else:
        output_path = os.path.expanduser(output_path)

    check_new_file(output_path)
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

    else:
        raise ValueError(f"Unsupported format: {format}. Only 'markdown' is supported")


# MCP Server Definitions
RESOURCES_SCHEMA = [
    {
        "uri": "email://accounts",
        "name": "Accounts",
        "description": "All mail accounts configured in Spark Desktop",
        "mimeType": "application/json"
    },
    {
        "uri": "email://folders",
        "name": "Folders",
        "description": "Folder hierarchy and unread message counts across accounts",
        "mimeType": "application/json"
    },
    {
        "uri": "email://signatures",
        "name": "Signatures",
        "description": "Active email signatures configured in Spark Desktop",
        "mimeType": "application/json"
    }
]

RESOURCE_TEMPLATES_SCHEMA = [
    {
        "uriTemplate": "email://messages/{message_id}",
        "name": "Email Message",
        "description": "Read full content of an email by its message ID",
        "mimeType": "application/json"
    },
    {
        "uriTemplate": "email://threads/{conversation_id}",
        "name": "Conversation Thread",
        "description": "Read full conversation history by conversation ID",
        "mimeType": "application/json"
    }
]

PROMPTS_SCHEMA = [
    {
        "name": "inbox_triage",
        "description": "Triage unread/unreplied inbox emails into Urgent, Action Required, and Archive.",
        "arguments": [
            {
                "name": "limit",
                "description": "Max emails to review (default 10)",
                "required": False
            }
        ]
    },
    {
        "name": "daily_briefing",
        "description": "Generate a morning briefing with new emails, package deliveries, invoices, and calendar events.",
        "arguments": [
            {
                "name": "hours",
                "description": "Lookback window in hours (default 24)",
                "required": False
            }
        ]
    },
    {
        "name": "draft_reply",
        "description": "Draft a contextual reply to an email thread matching conversation tone and language.",
        "arguments": [
            {
                "name": "message_id",
                "description": "Message ID of the email to reply to",
                "required": True
            },
            {
                "name": "intent",
                "description": "Key points or instructions for the response",
                "required": False
            }
        ]
    }
]

TOOLS_SCHEMA = [
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
                "offset": {"type": "integer", "description": "Offset for pagination.", "default": 0},
                "cursor": {"type": "integer", "description": "Cursor for pagination (pass next_cursor from previous page)."}
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
        "name": "spark_list_messages",
        "description": "List emails from Spark Desktop with filters (account, folder, category, date range, inbox, unseen, starred).",
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
                "days": {
                    "type": "number",
                    "description": "Filter emails received within the last N days (e.g. 7 for past week, 1 for past 24 hours)."
                },
                "since_date": {
                    "type": "string",
                    "description": "Filter emails received on or after this date/time (ISO 8601 string 'YYYY-MM-DD' or timestamp)."
                },
                "until_date": {
                    "type": "string",
                    "description": "Filter emails received on or before this date/time (ISO 8601 string 'YYYY-MM-DD' or timestamp)."
                },
                "limit": {"type": "integer", "description": "Max messages to return (default 10, max 100).", "default": 10},
                "offset": {"type": "integer", "description": "Offset for pagination.", "default": 0},
                "cursor": {"type": "integer", "description": "Cursor for pagination (pass next_cursor from previous page)."}
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
        "description": "Locate an attachment file from Spark's local cache on disk and inspect details. To download an uncached one use spark_cli_attachment.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "attachment_id": {"type": "integer", "description": "ID of the attachment from spark_get_message or spark_search_attachments."},
                "message_id": {"type": "integer", "description": "Message ID containing the attachment."},
                "filename": {"type": "string", "description": "Name or part of filename of the attachment."}
            }
        }
    },
    {
        "name": "spark_inspect_attachment",
        "description": "Inspect and read the contents of an email attachment (PDF, image, document, text) entirely in memory (RAM, no disk footprint). Returns extracted text for PDFs and documents, or an image block for photos/scans to allow visual inspection.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "attachment_id": {"type": "integer", "description": "ID of the attachment from spark_get_message or spark_search_attachments."},
                "message_id": {"type": "integer", "description": "Message ID containing the attachment."},
                "filename": {"type": "string", "description": "Name or part of filename of the attachment to find."}
            }
        }
    },
    {
        "name": "spark_inspect_document",
        "description": "Alias for spark_inspect_attachment. Inspect and read an email attachment entirely in RAM without saving to disk.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "attachment_id": {"type": "integer", "description": "ID of the attachment."},
                "message_id": {"type": "integer", "description": "Message ID containing the attachment."},
                "filename": {"type": "string", "description": "Name or part of filename of the attachment."}
            }
        }
    },
    {
        "name": "inspect_document",
        "description": "Inspect and read the contents of an email attachment (PDF, image, or text file) entirely in memory (RAM, no disk footprint), same as in telegram-mcp.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "attachment_id": {"type": "integer", "description": "ID of the attachment."},
                "message_id": {"type": "integer", "description": "Message ID containing the attachment."},
                "filename": {"type": "string", "description": "Name or part of filename of the attachment."}
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
        "description": "Export an email to a file on disk (formats: html, txt, eml). Default path is ~/Downloads.",
        "inputSchema": {
            "type": "object",
            "required": ["message_id"],
            "properties": {
                "message_id": {"type": "integer", "description": "ID of the email to export."},
                "output_path": {"type": "string", "description": "Destination file path within ~/Downloads (default ~/Downloads/Email_<id>.<format>)."},
                "format": {
                    "type": "string",
                    "enum": ["html", "txt", "eml"],
                    "description": "Export file format (default 'html').",
                    "default": "html"
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
        "description": "Search emails across all accounts using FTS5 with BM25 relevance ranking and native snippet match highlighting.",
        "inputSchema": {
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {"type": "string", "description": "Search term or phrase."},
                "limit": {"type": "integer", "description": "Max results to return (default 20).", "default": 20},
                "sort_by": {
                    "type": "string",
                    "enum": ["relevance", "date"],
                    "description": "Sort order: 'relevance' (BM25 weighted score) or 'date' (most recent first). Default is 'relevance'.",
                    "default": "relevance"
                }
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
                "to": {"type": "string", "description": "Recipient email address or contact name (auto-resolved from contacts)."},
                "subject": {"type": "string", "description": "Subject of the email."},
                "body": {"type": "string", "description": "Email body content."},
                "cc": {"type": "string", "description": "CC recipients (email addresses or contact names)."},
                "bcc": {"type": "string", "description": "BCC recipients (email addresses or contact names)."}
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
        "description": "Batch export cached attachments matching filters (file extension, sender, search term) to a local directory.",
        "inputSchema": {
            "type": "object",
            "required": ["target_dir"],
            "properties": {
                "target_dir": {"type": "string", "description": "Target folder on macOS to copy the files to (e.g. ~/Downloads/invoices)."},
                "file_extension": {"type": "string", "description": "Optional file extension to filter by (e.g. 'pdf', 'xlsx', 'docx')."},
                "query": {"type": "string", "description": "Optional search term to filter attachment name or email subject."},
                "sender": {"type": "string", "description": "Optional sender name or email address."},
                "limit": {"type": "integer", "description": "Maximum number of files to export (default 50).", "default": 50}
            }
        }
    },
    {
        "name": "spark_export_thread",
        "description": "Export an entire conversation thread into a structured Markdown document.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "conversation_id": {"type": "integer", "description": "Thread conversation ID."},
                "message_id": {"type": "integer", "description": "Message ID (used to lookup conversation ID if conversation_id omitted)."},
                "output_path": {"type": "string", "description": "Optional output file path (defaults to ~/Downloads/Thread_<id>_<subject>.md)."},
                "format": {"type": "string", "description": "Output format: only 'markdown'.", "enum": ["markdown"], "default": "markdown"}
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


_cli_tried_at = 0.0


def refresh_cli_catalog(max_age=300):
    """(Re)load `spark tools`, at most once per max_age seconds (30 while the catalog is empty).
    Leaves the catalog empty when Spark is not running."""
    global _cli_tried_at
    if time.time() - _cli_tried_at < (max_age if CLI_CATALOG else min(max_age, 30)):
        return
    _cli_tried_at = time.time()
    try:
        tools = json.loads(run_spark_cli(["tools"], timeout=15))["tools"]
    except Exception as e:
        sys.stderr.write(f"Spark CLI catalog not loaded: {e}\n")
        return
    CLI_CATALOG.clear()
    CLI_CATALOG.update({"spark_cli_" + t["command"].replace("-", "_"): t for t in tools})


def cli_tool_schemas():
    schemas = []
    for name, t in CLI_CATALOG.items():
        try:  # one malformed catalog entry must not take down tools/list
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
        except Exception as e:
            sys.stderr.write(f"Skipping malformed Spark CLI tool {name}: {e}\n")
    return schemas


def call_cli_tool(name, kw):
    t = CLI_CATALOG[name]
    params = {p["name"]: p for p in t["parameters"]}
    unknown = set(kw) - set(params)
    if unknown:
        raise ValueError(f"Unknown arguments: {sorted(unknown)}")
    args = [t["command"]]
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
    positionals, gap = [], None  # `--` below keeps values starting with "-" from being parsed as options
    for p in t["parameters"]:
        if "flag" in p:
            continue
        v = kw.get(p["name"])
        if v in (None, "", []):
            if p.get("required"):
                raise ValueError(f"Missing required argument: {p['name']}")
            gap = gap or p["name"]
        else:
            if gap:
                raise ValueError(f"Argument {p['name']} needs {gap} to be set first")
            positionals.extend(v if isinstance(v, list) else [v])
    if positionals:
        args.extend(["--"] + positionals)
    out = run_spark_cli(args).rstrip()
    if len(out) > CLI_OUTPUT_LIMIT:  # e.g. `search <query>` returns 20 full bodies
        out = out[:CLI_OUTPUT_LIMIT] + f"\n\n[truncated: {len(out)} chars total. Narrow the query/filter or use a smaller page_size.]"
    return {"output": out}


TOOL_NAMES = {t["name"] for t in TOOLS_SCHEMA}
COMPAT_TOOLS = {"spark_list_accounts", "spark_list_folders", "spark_list_signatures"}


def safe_stdout_write(data_str):
    """Write data to stdout and flush safely, exiting without stack trace on pipe breakage."""
    try:
        sys.stdout.write(data_str)
        sys.stdout.flush()
    except (BrokenPipeError, IOError):
        sys.exit(0)


def setup_signal_handlers():
    """Register termination signal handlers to cleanly release SQLite locks."""
    def handle_signal(sig, frame):
        close_all_connections()
        sys.exit(0)
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(s, handle_signal)
        except (ValueError, AttributeError):
            pass


def handle_resource_read(uri):
    """Fetch structured data for an email:// MCP Resource URI."""
    if uri == "email://accounts":
        data = spark_list_accounts()
    elif uri == "email://folders":
        data = spark_list_folders()
    elif uri == "email://signatures":
        data = spark_list_signatures()
    else:
        m_msg = re.match(r"^email://messages/(\d+)$", uri)
        if m_msg:
            data = spark_get_message(int(m_msg.group(1)))
        else:
            m_th = re.match(r"^email://threads/(\d+)$", uri)
            if m_th:
                data = spark_get_thread(conversation_id=int(m_th.group(1)))
            else:
                raise ValueError(f"Unknown resource URI: {uri}")

    return {
        "contents": [
            {
                "uri": uri,
                "mimeType": "application/json",
                "text": json.dumps(data, ensure_ascii=False, indent=2)
            }
        ]
    }


def handle_prompt_inbox_triage(args):
    limit = int(args.get("limit", 10))
    summary = spark_get_unread_summary()
    unreplied = spark_find_unreplied_emails(limit=limit)
    text = (
        "You are an executive assistant conducting an inbox triage.\n"
        "Review the unreplied correspondence and mailbox state below. "
        "Classify every email into one of three action categories:\n"
        "1. [URGENT]: Requires immediate same-day response or critical escalation.\n"
        "2. [ACTION REQUIRED]: Needs follow-up, task creation, or reply this week.\n"
        "3. [ARCHIVE / FYI]: Informational, newsletter, or already handled.\n\n"
        f"Mailbox Overview:\n{json.dumps(summary, indent=2, ensure_ascii=False)}\n\n"
        f"Unreplied Emails:\n{json.dumps(unreplied, indent=2, ensure_ascii=False)}\n\n"
        "Provide prioritized next steps and concise drafts for any urgent replies."
    )
    return {
        "description": "Triage unread/unreplied inbox emails into Urgent, Action Required, and Archive.",
        "messages": [
            {
                "role": "user",
                "content": {"type": "text", "text": text}
            }
        ]
    }


def handle_prompt_daily_briefing(args):
    hours = int(args.get("hours", 24))
    days = max(1, hours // 24)
    deliveries = spark_find_deliveries(days=days)
    invoices = spark_find_invoices()
    events = spark_list_calendar_events(limit=10)
    summary = spark_get_unread_summary()
    text = (
        "Prepare a daily morning briefing based on current Spark Desktop mail and calendar activity.\n\n"
        f"Inbox Overview:\n{json.dumps(summary, indent=2, ensure_ascii=False)}\n\n"
        f"Upcoming Calendar Events:\n{json.dumps(events, indent=2, ensure_ascii=False)}\n\n"
        f"Package Deliveries (last {hours}h):\n{json.dumps(deliveries, indent=2, ensure_ascii=False)}\n\n"
        f"Invoices & Receipts:\n{json.dumps(invoices, indent=2, ensure_ascii=False)}\n\n"
        "Summarize the agenda for today, key deliverables, incoming shipments, and outstanding bills."
    )
    return {
        "description": "Generate a morning briefing with new emails, package deliveries, invoices, and calendar events.",
        "messages": [
            {
                "role": "user",
                "content": {"type": "text", "text": text}
            }
        ]
    }


def handle_prompt_draft_reply(args):
    msg_id = args.get("message_id")
    if not msg_id:
        raise ValueError("message_id is required for draft_reply prompt")
    intent = args.get("intent", "Draft a polite and clear reply addressing all questions.")
    msg = spark_get_message(int(msg_id))
    thread_info = []
    conv_id = msg.get("conversation_id")
    if conv_id:
        try:
            thread_data = spark_get_thread(conversation_id=conv_id)
            thread_info = thread_data.get("messages", [])
        except Exception:
            pass

    text = (
        f"Draft a response to this email thread.\n\n"
        f"User Instructions / Intent:\n{intent}\n\n"
        f"Target Email:\n"
        f"From: {msg.get('from', '')}\n"
        f"Subject: {msg.get('subject', '')}\n"
        f"Date: {msg.get('received_date', '')}\n"
        f"Body:\n{msg.get('body', '')}\n\n"
    )
    if thread_info:
        text += f"Prior Conversation History ({len(thread_info)} messages):\n{json.dumps(thread_info, indent=2, ensure_ascii=False)}\n\n"
    text += "Draft the response matching the sender's language, appropriate tone, and clear call-to-actions."

    return {
        "description": "Draft a contextual reply to an email thread matching conversation tone and language.",
        "messages": [
            {
                "role": "user",
                "content": {"type": "text", "text": text}
            }
        ]
    }


def handle_prompt_get(name, args):
    """Generate prompt messages for a requested MCP Prompt."""
    if name == "inbox_triage":
        return handle_prompt_inbox_triage(args)
    if name == "daily_briefing":
        return handle_prompt_daily_briefing(args)
    if name == "draft_reply":
        return handle_prompt_draft_reply(args)
    raise ValueError(f"Prompt not found: {name}")


def handle_request(req):
    req_id = req.get("id")
    method = req.get("method")
    params = req.get("params", {})

    if method == "initialize":
        client_version = params.get("protocolVersion")
        protocol_version = client_version if client_version in ("2024-11-05", "0.1.0") else "2024-11-05"
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": protocol_version,
                "capabilities": {
                    "tools": {"listChanged": False},
                    "resources": {"subscribe": False, "listChanged": False},
                    "prompts": {"listChanged": False}
                },
                "serverInfo": {
                    "name": "spark-desktop-mcp",
                    "version": "2.2.0"
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

    if method == "resources/list":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "resources": RESOURCES_SCHEMA
            }
        }

    if method == "resources/templates/list":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "resourceTemplates": RESOURCE_TEMPLATES_SCHEMA
            }
        }

    if method == "resources/read":
        uri = params.get("uri")
        if not uri:
            raise ValueError("Missing 'uri' parameter in resources/read")
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": handle_resource_read(uri)
        }

    if method == "prompts/list":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "prompts": PROMPTS_SCHEMA
            }
        }

    if method == "prompts/get":
        prompt_name = params.get("name")
        prompt_args = params.get("arguments", {})
        if not prompt_name:
            raise ValueError("Missing 'name' parameter in prompts/get")
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": handle_prompt_get(prompt_name, prompt_args)
        }

    if method == "tools/list":
        try:
            refresh_cli_catalog()
            cli_tools = cli_tool_schemas()
        except Exception as e:
            sys.stderr.write(f"Spark CLI tools unavailable: {e}\n")
            cli_tools = []
        all_tools = TOOLS_SCHEMA + cli_tools
        exposed_tools = []
        for t in all_tools:
            if is_tool_exposed(t["name"]):
                item = dict(t)
                item["annotations"] = get_tool_annotations(t["name"])
                exposed_tools.append(item)
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "tools": exposed_tools
            }
        }

    if method == "tools/call":
        tool_name = params.get("name")
        args = params.get("arguments", {})
        meta = params.get("_meta", {})
        progress_token = meta.get("progressToken")

        try:
            if not is_tool_exposed(tool_name):
                raise PermissionError(f"Tool '{tool_name}' is disabled by SPARK_EXPOSED_TOOLS.")

            # Create progress callback if client provided a progressToken
            progress_cb = None
            if progress_token is not None:
                def progress_cb(progress, total=None, message=None):
                    notif = {
                        "jsonrpc": "2.0",
                        "method": "notifications/progress",
                        "params": {
                            "progressToken": progress_token,
                            "progress": progress
                        }
                    }
                    if total is not None:
                        notif["params"]["total"] = total
                    if message:
                        notif["params"]["message"] = str(message)
                    safe_stdout_write(json.dumps(notif, ensure_ascii=False) + "\n")

            # Schema names are the allowlist; argument defaults live on the functions.
            if tool_name in TOOL_NAMES or tool_name in COMPAT_TOOLS:
                fn = globals()[tool_name]
                sig = inspect.signature(fn)
                if "progress_callback" in sig.parameters:
                    args["progress_callback"] = progress_cb
                res = fn(**args)
            else:
                if tool_name not in CLI_CATALOG:
                    refresh_cli_catalog(max_age=30)
                if tool_name not in CLI_CATALOG:
                    raise ValueError(f"Unknown tool: {tool_name} (Spark CLI tools need Spark running)")
                res = call_cli_tool(tool_name, args)

            if isinstance(res, dict) and "_mcp_content" in res:
                content = res["_mcp_content"]
            elif isinstance(res, str):
                content = [{"type": "text", "text": res}]
            else:
                content = [
                    {
                        "type": "text",
                        "text": json.dumps(res, ensure_ascii=False, separators=(",", ":"))
                    }
                ]

            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": content,
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


def install_mcp():
    """Auto-configure spark-mail MCP server into installed AI clients on macOS."""
    python_bin = sys.executable
    script_path = os.path.abspath(__file__)
    server_entry = {
        "command": python_bin,
        "args": [script_path]
    }

    installed = []

    # 1. Claude Desktop
    claude_cfg = os.path.expanduser("~/Library/Application Support/Claude/claude_desktop_config.json")
    if os.path.isdir(os.path.dirname(claude_cfg)):
        try:
            data = {}
            if os.path.exists(claude_cfg):
                shutil.copy2(claude_cfg, claude_cfg + ".bak")
                with open(claude_cfg, "r", encoding="utf-8") as f:
                    data = json.load(f)
            data.setdefault("mcpServers", {})["spark-mail"] = server_entry
            with open(claude_cfg, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            installed.append("Claude Desktop (restart with Cmd+Q)")
        except Exception as e:
            sys.stderr.write(f"Failed to configure Claude Desktop: {e}\n")

    # 2. Antigravity
    ag_cfg = os.path.expanduser("~/.gemini/antigravity/mcp_config.json")
    if os.path.isdir(os.path.dirname(ag_cfg)):
        try:
            data = {}
            if os.path.exists(ag_cfg):
                shutil.copy2(ag_cfg, ag_cfg + ".bak")
                with open(ag_cfg, "r", encoding="utf-8") as f:
                    data = json.load(f)
            data.setdefault("mcpServers", {})["spark-mail"] = server_entry
            with open(ag_cfg, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            installed.append("Antigravity")
        except Exception as e:
            sys.stderr.write(f"Failed to configure Antigravity: {e}\n")

    # 3. Cursor
    cursor_dir = os.path.expanduser("~/.cursor")
    cursor_cfg = os.path.join(cursor_dir, "mcp.json")
    if os.path.exists(cursor_dir):
        try:
            data = {}
            if os.path.exists(cursor_cfg):
                shutil.copy2(cursor_cfg, cursor_cfg + ".bak")
                with open(cursor_cfg, "r", encoding="utf-8") as f:
                    data = json.load(f)
            data.setdefault("mcpServers", {})["spark-mail"] = server_entry
            with open(cursor_cfg, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            installed.append("Cursor")
        except Exception as e:
            sys.stderr.write(f"Failed to configure Cursor: {e}\n")

    # 4. Claude Code CLI
    if shutil.which("claude"):
        try:
            subprocess.run(
                ["claude", "mcp", "add", "spark-mail", "--scope", "user", "--", python_bin, script_path],
                check=True, capture_output=True, text=True
            )
            installed.append("Claude Code CLI")
        except Exception as e:
            pass

    if installed:
        print("✓ Successfully configured spark-mail MCP server for:")
        for item in installed:
            print(f"  • {item}")
        print(f"\nCommand: {python_bin}")
        print(f"Script:  {script_path}")
    else:
        print("No supported AI client configurations found on this Mac.")


def uninstall_mcp():
    """Remove spark-mail MCP server from installed AI clients on macOS."""
    removed = []

    # Claude Desktop
    claude_cfg = os.path.expanduser("~/Library/Application Support/Claude/claude_desktop_config.json")
    if os.path.exists(claude_cfg):
        try:
            with open(claude_cfg, "r", encoding="utf-8") as f:
                data = json.load(f)
            if "mcpServers" in data and "spark-mail" in data["mcpServers"]:
                del data["mcpServers"]["spark-mail"]
                with open(claude_cfg, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                removed.append("Claude Desktop")
        except Exception as e:
            sys.stderr.write(f"Failed to update Claude Desktop: {e}\n")

    # Antigravity
    ag_cfg = os.path.expanduser("~/.gemini/antigravity/mcp_config.json")
    if os.path.exists(ag_cfg):
        try:
            with open(ag_cfg, "r", encoding="utf-8") as f:
                data = json.load(f)
            if "mcpServers" in data and "spark-mail" in data["mcpServers"]:
                del data["mcpServers"]["spark-mail"]
                with open(ag_cfg, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                removed.append("Antigravity")
        except Exception as e:
            sys.stderr.write(f"Failed to update Antigravity: {e}\n")

    # Cursor
    cursor_cfg = os.path.expanduser("~/.cursor/mcp.json")
    if os.path.exists(cursor_cfg):
        try:
            with open(cursor_cfg, "r", encoding="utf-8") as f:
                data = json.load(f)
            if "mcpServers" in data and "spark-mail" in data["mcpServers"]:
                del data["mcpServers"]["spark-mail"]
                with open(cursor_cfg, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                removed.append("Cursor")
        except Exception as e:
            sys.stderr.write(f"Failed to update Cursor: {e}\n")

    # Claude Code
    if shutil.which("claude"):
        try:
            subprocess.run(["claude", "mcp", "remove", "spark-mail"], capture_output=True)
            removed.append("Claude Code CLI")
        except Exception:
            pass

    if removed:
        print("✓ Successfully removed spark-mail from:")
        for item in removed:
            print(f"  • {item}")
    else:
        print("spark-mail configuration not found.")


def main():
    setup_signal_handlers()
    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()
        if cmd in ("--install", "-i", "install"):
            install_mcp()
            return
        if cmd in ("--uninstall", "-u", "uninstall"):
            uninstall_mcp()
            return
        if cmd in ("--help", "-h", "help"):
            print("Spark Desktop MCP Server")
            print("Usage:")
            print("  python3 spark_mcp.py             Run MCP stdio server")
            print("  python3 spark_mcp.py --install   Auto-configure into Claude Desktop, Cursor, Antigravity")
            print("  python3 spark_mcp.py --uninstall Remove from all AI clients")
            return

    while True:
        line = sys.stdin.readline()
        if not line:
            break
        line = line.strip()
        if not line:
            continue
        req = None
        try:
            req = json.loads(line)
            resp = handle_request(req)
            if resp is not None:
                safe_stdout_write(json.dumps(resp, ensure_ascii=False) + "\n")
        except Exception as e:
            sys.stderr.write(f"Protocol error: {e}\n")
            sys.stderr.flush()
            if isinstance(req, dict) and "id" in req:  # never leave a request unanswered
                safe_stdout_write(json.dumps({"jsonrpc": "2.0", "id": req["id"],
                                             "error": {"code": -32603, "message": str(e)}}) + "\n")


if __name__ == "__main__":
    main()
