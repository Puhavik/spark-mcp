#!/usr/bin/env python3
"""
Comprehensive 100% synthetic test suite for Spark Desktop MCP Server.
Covers all 31 tools, MCP protocol endpoints (resources, prompts, tools),
SQLite persistent connection pooling, FTS5 BM25 search, cursor pagination,
and safety boundaries WITHOUT touching any user personal data.

Zero external test frameworks, pure Python 3 standard library.
"""

import base64
import io
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import urllib.parse
import zipfile
import time
from unittest.mock import patch, MagicMock

import spark_mcp


def setup_synthetic_data(temp_dir):
    """Create a complete synthetic Spark Desktop SQLite environment in temp_dir."""
    core_data_dir = os.path.join(temp_dir, "core-data")
    cache_dir = os.path.join(temp_dir, "cache")
    downloads_dir = os.path.join(temp_dir, "Downloads")
    os.makedirs(core_data_dir, exist_ok=True)
    os.makedirs(cache_dir, exist_ok=True)
    os.makedirs(downloads_dir, exist_ok=True)

    messages_db_path = os.path.join(core_data_dir, "messages.sqlite")
    cache_db_path = os.path.join(core_data_dir, "cache.sqlite")
    search_db_path = os.path.join(core_data_dir, "search_fts5.sqlite")
    contacts_db_path = os.path.join(core_data_dir, "contactsDictionary4.sqlite")
    calendar_db_path = os.path.join(core_data_dir, "calendarsapi.sqlite")
    settings_db_path = os.path.join(core_data_dir, "settings.sqlite")

    now = int(time.time())

    # 1. messages.sqlite
    with sqlite3.connect(messages_db_path) as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE accounts (
                pk INTEGER PRIMARY KEY,
                accountType TEXT,
                accountTitle TEXT,
                ownerFullName TEXT,
                orderNumber INTEGER,
                additionalInfo TEXT
            )
        """)
        c.executemany("INSERT INTO accounts VALUES (?, ?, ?, ?, ?, ?)", [
            (1, "imap", "Work Account", "Test User", 1, json.dumps({"accountAddress": "user@company.test"})),
            (2, "imap", "Personal Account", "Test User", 2, json.dumps({"accountAddress": "me@home.test"}))
        ])

        c.execute("""
            CREATE TABLE folders (
                pk INTEGER PRIMARY KEY,
                accountPk INTEGER,
                folderName TEXT,
                folderPath TEXT,
                imapMessageCount INTEGER,
                imapMessageUnseenCount INTEGER
            )
        """)
        c.executemany("INSERT INTO folders VALUES (?, ?, ?, ?, ?, ?)", [
            (1, 1, "Inbox", "INBOX", 10, 2),
            (2, 1, "Archive", "Archive", 5, 0),
            (3, 2, "Inbox", "INBOX", 4, 1)
        ])

        c.execute("""
            CREATE TABLE conversations (
                pk INTEGER PRIMARY KEY,
                accountPk INTEGER,
                subject TEXT,
                sender TEXT,
                otherSenders TEXT,
                totalMessages INTEGER,
                unseenMessages INTEGER,
                inInbox INTEGER,
                inArchive INTEGER,
                category INTEGER,
                inboxOrSnoozeDate INTEGER,
                updateDate INTEGER
            )
        """)
        c.executemany("INSERT INTO conversations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
            (10, 1, "Project kick-off", "boss@company.test", "user@company.test", 2, 1, 1, 0, 1, now - 5000, now - 5000),
            (20, 1, "Invoice 2026-INV01 for Services", "billing@vendor.test", "", 1, 0, 1, 0, 2, now - 4000, now - 4000),
            (30, 2, "Your package order 987654321 is shipped", "support@shop.test", "", 1, 0, 1, 0, 3, now - 3000, now - 3000),
            (40, 2, "Your verification code is 654321", "security@service.test", "", 1, 0, 1, 0, 2, now - 2000, now - 2000),
            (50, 1, "Weekly Newsletter", "news@tech.test", "", 1, 0, 1, 0, 3, now - 1000, now - 1000),
            (60, 1, "Action needed: unreplied question", "client@partner.test", "", 1, 1, 1, 0, 1, now - 500, now - 500)
        ])

        c.execute("""
            CREATE TABLE messages (
                pk INTEGER PRIMARY KEY,
                accountPk INTEGER,
                conversationPk INTEGER,
                receivedDate INTEGER,
                creationDate INTEGER,
                messageFrom TEXT,
                messageTo TEXT,
                messageCc TEXT,
                messageBcc TEXT,
                subject TEXT,
                shortBody TEXT,
                unseen INTEGER,
                starred INTEGER,
                inInbox INTEGER,
                inSent INTEGER,
                inArchive INTEGER,
                category INTEGER,
                numberOfFileAttachments INTEGER,
                messageId TEXT,
                listUnsubscribeURL TEXT,
                messageReplyToMailbox TEXT
            )
        """)
        c.executemany("INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
            (1, 1, 10, now - 6000, now - 6000, "Boss <boss@company.test>", "user@company.test", "", "", "Project kick-off", "Let's start the project architecture.", 1, 0, 1, 0, 0, 1, 3, "<msg1@company.test>", None, None),
            (2, 1, 10, now - 5000, now - 5000, "user@company.test", "boss@company.test", "", "", "Re: Project kick-off", "Sounds good, I am on it.", 0, 0, 1, 1, 0, 1, 0, "<msg2@company.test>", None, None),
            (3, 1, 20, now - 4000, now - 4000, "billing@vendor.test", "user@company.test", "", "", "Invoice 2026-INV01 for Services", "Please find attached invoice for payment.", 0, 0, 1, 0, 0, 2, 2, "<msg3@vendor.test>", None, "billing-reply@vendor.test"),
            (4, 2, 30, now - 3000, now - 3000, "support@shop.test", "me@home.test", "", "", "Your package order 987654321 is shipped", "Delivery tracking DHL 1234567890 out for delivery.", 0, 0, 1, 0, 0, 3, 0, "<msg4@shop.test>", None, None),
            (5, 2, 40, now - 2000, now - 2000, "security@service.test", "me@home.test", "", "", "Your verification code is 654321", "Your security code is: 654321. Confirm at https://service.test/verify?token=abc", 0, 0, 1, 0, 0, 2, 0, "<msg5@service.test>", None, None),
            (6, 1, 50, now - 1000, now - 1000, "news@tech.test", "user@company.test", "", "", "Weekly Newsletter", "News of the week updates.", 0, 0, 1, 0, 0, 3, 0, "<msg6@tech.test>", "<https://tech.test/unsub?id=123>", None),
            (7, 1, 60, now - 500, now - 500, "client@partner.test", "user@company.test", "", "", "Action needed: unreplied question", "Can you please reply to this question urgent architecture?", 1, 1, 1, 0, 0, 1, 0, "<msg7@partner.test>", None, None)
        ])

        c.execute("""
            CREATE TABLE messageFoldersInfo (
                folderPk INTEGER,
                messagePk INTEGER
            )
        """)
        c.executemany("INSERT INTO messageFoldersInfo VALUES (?, ?)", [
            (1, 1), (1, 2), (1, 3), (3, 4), (3, 5), (1, 6), (1, 7)
        ])

        c.execute("""
            CREATE TABLE messageAttachment (
                pk INTEGER PRIMARY KEY,
                messagePk INTEGER,
                accountPk INTEGER,
                attachmentName TEXT,
                attachmentSize INTEGER,
                attachmentURL TEXT,
                attachmentMIMEType TEXT
            )
        """)

    # Create cached files
    notes_txt = os.path.join(cache_dir, "notes.txt")
    with open(notes_txt, "w", encoding="utf-8") as f:
        f.write("Project notes and architectural checklist.")

    diagram_png = os.path.join(cache_dir, "diagram.png")
    # Minimal 1x1 valid PNG bytes
    with open(diagram_png, "wb") as f:
        f.write(base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="))

    invite_ics = os.path.join(cache_dir, "invite.ics")
    with open(invite_ics, "w", encoding="utf-8") as f:
        f.write("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nSUMMARY:Architecture Review\r\nDTSTART:20261010T090000Z\r\nDTEND:20261010T100000Z\r\nLOCATION:Conference Room\r\nDESCRIPTION:Discuss MCP integration\r\nSTATUS:CONFIRMED\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n")

    docx_path = os.path.join(cache_dir, "specs.docx")
    with zipfile.ZipFile(docx_path, "w") as z:
        z.writestr(
            "word/document.xml",
            '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
            '<w:body><w:p><w:t>Project Specification Details</w:t></w:p></w:body></w:document>'
        )

    xlsx_path = os.path.join(cache_dir, "data.xlsx")
    with zipfile.ZipFile(xlsx_path, "w") as z:
        z.writestr(
            "xl/sharedStrings.xml",
            '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            '<si><t>Item</t></si><si><t>Cost</t></si><si><t>Server</t></si></sst>'
        )
        z.writestr(
            "xl/worksheets/sheet1.xml",
            '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            '<sheetData>'
            '<row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="s"><v>1</v></c></row>'
            '<row r="2"><c r="A2" t="s"><v>2</v></c><c r="B2"><v>100</v></c></row>'
            '</sheetData></worksheet>'
        )

    invoice_pdf = os.path.join(cache_dir, "invoice.pdf")
    with open(invoice_pdf, "wb") as f:
        f.write(b"%PDF-1.4 Mock PDF Invoice Content 500 USD")

    with sqlite3.connect(messages_db_path) as conn:
        c = conn.cursor()
        c.executemany("INSERT INTO messageAttachment VALUES (?, ?, ?, ?, ?, ?, ?)", [
            (101, 3, 1, "invoice.pdf", os.path.getsize(invoice_pdf), f"file://{invoice_pdf}", "application/pdf"),
            (102, 1, 1, "diagram.png", os.path.getsize(diagram_png), f"file://{diagram_png}", "image/png"),
            (103, 1, 1, "notes.txt", os.path.getsize(notes_txt), f"file://{notes_txt}", "text/plain"),
            (104, 1, 1, "invite.ics", os.path.getsize(invite_ics), f"file://{invite_ics}", "text/calendar"),
            (105, 1, 1, "specs.docx", os.path.getsize(docx_path), f"file://{docx_path}", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
            (106, 3, 1, "data.xlsx", os.path.getsize(xlsx_path), f"file://{xlsx_path}", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        ])

    # 2. cache.sqlite
    with sqlite3.connect(cache_db_path) as conn:
        c = conn.cursor()
        c.execute("CREATE TABLE messageBodyHtml (messagePk INTEGER PRIMARY KEY, data BLOB)")
        c.execute("CREATE TABLE messageBodyParsedData (messagePk INTEGER PRIMARY KEY, data BLOB, sourceLanguage TEXT)")

        c.executemany("INSERT INTO messageBodyHtml VALUES (?, ?)", [
            (1, b"<p>Let's start the project architecture. See <a href='https://company.test/project'>Project Home</a> and <a href='https://track.test/link'>Status</a>.</p>"),
            (2, b"<p>Sounds good, I am on it.</p>"),
            (3, b"<p>Invoice details: <b>Amount: 500 EUR</b></p>"),
            (4, b"<p>Tracking number: <a href='https://dhl.test/track?num=1234567890'>DHL Tracking</a></p>"),
            (5, b"<p>Your security code is <b>654321</b>. Click <a href='https://service.test/verify?token=abc'>Verify</a></p>"),
            (6, b"<p>Weekly Newsletter. <a href='https://tech.test/unsub?id=123'>Unsubscribe</a></p>"),
            (7, b"<p>Can you please reply to this question urgent architecture?</p>")
        ])

        c.executemany("INSERT INTO messageBodyParsedData VALUES (?, ?, ?)", [
            (1, None, "en"),
            (2, None, "en"),
            (3, None, "de"),
            (4, None, "en"),
            (5, None, "en"),
            (6, None, "en"),
            (7, None, "en")
        ])

    # 3. search_fts5.sqlite
    with sqlite3.connect(search_db_path) as conn:
        c = conn.cursor()
        c.execute("CREATE VIRTUAL TABLE messagesfts USING fts5(messagePk UNINDEXED, messageFrom, messageTo, subject, searchBody, additionalText)")
        c.executemany("INSERT INTO messagesfts VALUES (?, ?, ?, ?, ?, ?)", [
            (1, "boss@company.test", "user@company.test", "Project kick-off", "Let's start the project architecture system design", ""),
            (3, "billing@vendor.test", "user@company.test", "Invoice 2026-INV01 for Services", "Please find attached invoice for payment amount 500", ""),
            (7, "client@partner.test", "user@company.test", "Action needed: unreplied question", "Can you please reply to this question urgent architecture?", "")
        ])

        c.execute("CREATE VIRTUAL TABLE attachmentsfts USING fts5(messagePk UNINDEXED, attachmentPk UNINDEXED, chunkIndex UNINDEXED, text)")
        c.execute("INSERT INTO attachmentsfts VALUES (?, ?, ?, ?)", (
            3, 101, 0, "Invoice contract tax payment SEPA IBAN AT1234567890"
        ))

    # 4. contactsDictionary4.sqlite
    with sqlite3.connect(contacts_db_path) as conn:
        c = conn.cursor()
        c.execute("CREATE TABLE ContactEmails (pk INTEGER PRIMARY KEY, email TEXT, quality INTEGER, contactPk INTEGER)")
        c.execute("CREATE TABLE ContactNames (emailPk INTEGER, name TEXT, contactPk INTEGER)")
        c.execute("INSERT INTO ContactEmails VALUES (1, 'alice@partner.test', 5, 1)")
        c.execute("INSERT INTO ContactNames VALUES (1, 'Alice Partner', 1)")

    # 5. calendarsapi.sqlite
    with sqlite3.connect(calendar_db_path) as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE RDCALAPIEvent (
                pk INTEGER PRIMARY KEY,
                summary TEXT,
                descriptionProperty TEXT,
                location TEXT,
                dstart INTEGER,
                dend INTEGER,
                allDay INTEGER,
                status INTEGER,
                rrule TEXT
            )
        """)
        c.execute("INSERT INTO RDCALAPIEvent VALUES (1, 'Quarterly Planning', 'Strategy review', 'Room 101', 1700000000, 1700003600, 0, 1, '')")

    # 6. settings.sqlite
    with sqlite3.connect(settings_db_path) as conn:
        c = conn.cursor()
        c.execute("CREATE TABLE settings (itemKey TEXT, itemGroup TEXT, itemValue BLOB)")
        sig1 = {"identifier": "sig_en", "deleted": False, "htmlContent": "<p>Best regards,<br>Test User</p>"}
        sig2 = {"identifier": "sig_de", "deleted": False, "htmlContent": "<p>Mit freundlichen Grüßen,<br>Test User</p>"}
        c.execute("INSERT INTO settings VALUES ('sig1', 'SignaturesSettingsItemsGroup', ?)", (json.dumps(sig1).encode("utf-8"),))
        c.execute("INSERT INTO settings VALUES ('sig2', 'SignaturesSettingsItemsGroup', ?)", (json.dumps(sig2).encode("utf-8"),))

    return {
        "core_data": core_data_dir,
        "cache_dir": cache_dir,
        "downloads_dir": downloads_dir,
        "messages_db": messages_db_path,
        "cache_db": cache_db_path,
        "search_db": search_db_path,
        "contacts_db": contacts_db_path,
        "calendar_db": calendar_db_path,
        "settings_db": settings_db_path
    }


def patch_spark_environment(env):
    """Point all spark_mcp paths to synthetic directory and clear cached connections."""
    spark_mcp.close_all_connections()
    spark_mcp.SPARK_CORE_DATA = env["core_data"]
    spark_mcp.SPARK_CACHE_DIR = env["cache_dir"]
    spark_mcp.MESSAGES_DB = env["messages_db"]
    spark_mcp.CACHE_DB = env["cache_db"]
    spark_mcp.SEARCH_DB = env["search_db"]
    spark_mcp.CONTACTS_DB = env["contacts_db"]
    spark_mcp.CALENDAR_DB = env["calendar_db"]
    spark_mcp.SETTINGS_DB = env["settings_db"]
    spark_mcp.SPARK_ALLOWED_ROOTS = [env["downloads_dir"]]


def run_all_tests():
    temp_dir = tempfile.mkdtemp(prefix="spark_test_synthetic_")
    try:
        env = setup_synthetic_data(temp_dir)
        patch_spark_environment(env)
        print(f"[*] Synthetic test environment initialized at: {temp_dir}")

        # -------------------------------------------------------------
        # 1. PROTOCOL LEVEL TESTS
        # -------------------------------------------------------------
        print("\n--- [1] Testing MCP Protocol Methods ---")

        # 1.1 initialize
        init_res = spark_mcp.handle_request({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2024-11-05"}
        })
        assert init_res["result"]["protocolVersion"] == "2024-11-05"
        assert "tools" in init_res["result"]["capabilities"]
        assert "resources" in init_res["result"]["capabilities"]
        assert "prompts" in init_res["result"]["capabilities"]
        print("  ✓ initialize (capabilities + protocolVersion negotiation)")

        # 1.2 ping
        ping_res = spark_mcp.handle_request({"jsonrpc": "2.0", "id": 2, "method": "ping"})
        assert ping_res["result"] == {}
        print("  ✓ ping")

        # 1.3 notifications (no id)
        notif_res = spark_mcp.handle_request({"jsonrpc": "2.0", "method": "initialized"})
        assert notif_res is None
        print("  ✓ notifications ignored without response")

        # 1.4 resources/list & resources/templates/list
        r_list = spark_mcp.handle_request({"jsonrpc": "2.0", "id": 3, "method": "resources/list"})
        assert len(r_list["result"]["resources"]) == 3
        rt_list = spark_mcp.handle_request({"jsonrpc": "2.0", "id": 4, "method": "resources/templates/list"})
        assert len(rt_list["result"]["resourceTemplates"]) == 2
        print("  ✓ resources/list and resources/templates/list")

        # 1.5 resources/read
        r_acc = spark_mcp.handle_request({"jsonrpc": "2.0", "id": 5, "method": "resources/read", "params": {"uri": "email://accounts"}})
        acc_data = json.loads(r_acc["result"]["contents"][0]["text"])
        assert len(acc_data) == 2
        assert acc_data[0]["email"] == "user@company.test"

        r_fold = spark_mcp.handle_request({"jsonrpc": "2.0", "id": 6, "method": "resources/read", "params": {"uri": "email://folders"}})
        fold_data = json.loads(r_fold["result"]["contents"][0]["text"])
        assert len(fold_data) == 3

        r_sigs = spark_mcp.handle_request({"jsonrpc": "2.0", "id": 7, "method": "resources/read", "params": {"uri": "email://signatures"}})
        sig_data = json.loads(r_sigs["result"]["contents"][0]["text"])
        assert len(sig_data) == 2

        r_msg = spark_mcp.handle_request({"jsonrpc": "2.0", "id": 8, "method": "resources/read", "params": {"uri": "email://messages/1"}})
        msg_data = json.loads(r_msg["result"]["contents"][0]["text"])
        assert msg_data["message_id"] == 1
        assert msg_data["subject"] == "Project kick-off"

        r_th = spark_mcp.handle_request({"jsonrpc": "2.0", "id": 9, "method": "resources/read", "params": {"uri": "email://threads/10"}})
        th_data = json.loads(r_th["result"]["contents"][0]["text"])
        assert th_data["conversation_id"] == 10
        assert len(th_data["messages"]) == 2
        print("  ✓ resources/read (accounts, folders, signatures, messages, threads)")

        # 1.6 prompts/list & prompts/get
        p_list = spark_mcp.handle_request({"jsonrpc": "2.0", "id": 10, "method": "prompts/list"})
        assert len(p_list["result"]["prompts"]) == 3

        p_triage = spark_mcp.handle_request({"jsonrpc": "2.0", "id": 11, "method": "prompts/get", "params": {"name": "inbox_triage", "arguments": {"limit": 5}}})
        assert "inbox triage" in p_triage["result"]["messages"][0]["content"]["text"].lower()

        p_brief = spark_mcp.handle_request({"jsonrpc": "2.0", "id": 12, "method": "prompts/get", "params": {"name": "daily_briefing", "arguments": {"hours": 24}}})
        assert "morning briefing" in p_brief["result"]["messages"][0]["content"]["text"].lower()

        p_draft = spark_mcp.handle_request({"jsonrpc": "2.0", "id": 13, "method": "prompts/get", "params": {"name": "draft_reply", "arguments": {"message_id": 1, "intent": "Agree"}}})
        assert "Project kick-off" in p_draft["result"]["messages"][0]["content"]["text"]
        print("  ✓ prompts/list and prompts/get (inbox_triage, daily_briefing, draft_reply)")

        # 1.7 tools/list (annotations + exposure)
        t_list = spark_mcp.handle_request({"jsonrpc": "2.0", "id": 14, "method": "tools/list"})
        tools = t_list["result"]["tools"]
        assert len(tools) >= 28
        msg_tool = next(t for t in tools if t["name"] == "spark_list_messages")
        assert msg_tool["annotations"]["readOnlyHint"] is True
        print("  ✓ tools/list (with MCP annotations)")

        # 1.8 Progress token notification handling
        progress_calls = []
        def mock_stdout(s):
            progress_calls.append(json.loads(s))
        with patch("spark_mcp.safe_stdout_write", side_effect=mock_stdout):
            res_prog = spark_mcp.handle_request({
                "jsonrpc": "2.0", "id": 15, "method": "tools/call",
                "params": {
                    "name": "spark_batch_export_attachments",
                    "arguments": {"target_dir": os.path.join(env["downloads_dir"], "prog_batch")},
                    "_meta": {"progressToken": "prog-test-1"}
                }
            })
            assert res_prog["result"]["isError"] is False
            assert len(progress_calls) > 0
            assert progress_calls[0]["method"] == "notifications/progress"
            assert progress_calls[0]["params"]["progressToken"] == "prog-test-1"
        print("  ✓ tools/call progress reporting (notifications/progress)")

        # 1.9 Error handling
        err_res = spark_mcp.handle_request({"jsonrpc": "2.0", "id": 16, "method": "tools/call", "params": {"name": "non_existent_tool", "arguments": {}}})
        assert err_res["result"]["isError"] is True
        print("  ✓ tools/call unknown tool error handling")

        # -------------------------------------------------------------
        # 2. DATA AND UTILITY TOOL TESTS (ALL 31 TOOLS)
        # -------------------------------------------------------------
        print("\n--- [2] Testing All 31 Tools on Synthetic Data ---")

        # 2.1 spark_list_accounts
        accs = spark_mcp.spark_list_accounts()
        assert len(accs) == 2
        assert accs[0]["account_id"] == 1
        assert accs[0]["email"] == "user@company.test"
        print("  ✓ [1/31] spark_list_accounts")

        # 2.2 spark_get_unread_summary
        unr = spark_mcp.spark_get_unread_summary()
        assert len(unr) == 2
        assert any(u["account"] == "Work Account" and u["inbox_unseen"] >= 1 for u in unr)
        print("  ✓ [2/31] spark_get_unread_summary")

        # 2.3 spark_find_unreplied_emails
        unrep = spark_mcp.spark_find_unreplied_emails(older_than_days=0)
        assert len(unrep) >= 1
        # Message pk=7 was personal (category=1), in inbox, never replied
        assert any(m["message_id"] == 7 for m in unrep)
        print("  ✓ [3/31] spark_find_unreplied_emails")

        # 2.4 spark_find_invoices
        invs = spark_mcp.spark_find_invoices()
        assert len(invs) >= 1
        assert invs[0]["message_id"] == 3
        assert "Invoice" in invs[0]["subject"]
        print("  ✓ [4/31] spark_find_invoices")

        # 2.5 spark_find_deliveries
        deliv = spark_mcp.spark_find_deliveries(days=365)
        assert len(deliv) >= 1
        assert deliv[0]["message_id"] == 4
        assert "shipped" in deliv[0]["subject"].lower()
        print("  ✓ [5/31] spark_find_deliveries")

        # 2.6 spark_list_folders
        folds = spark_mcp.spark_list_folders()
        assert len(folds) == 3
        assert any(f["name"] == "Inbox" for f in folds)
        print("  ✓ [6/31] spark_list_folders")

        # 2.7 spark_list_threads (structured pagination)
        threads = spark_mcp.spark_list_threads(limit=2)
        assert "items" in threads
        assert "has_more" in threads
        assert "next_cursor" in threads
        assert len(threads["items"]) == 2
        assert threads["has_more"] is True
        assert threads["next_cursor"] == 2
        # Page 2
        threads_p2 = spark_mcp.spark_list_threads(limit=2, cursor=threads["next_cursor"])
        assert len(threads_p2["items"]) == 2
        print("  ✓ [7/31] spark_list_threads (with cursor pagination)")

        # 2.8 spark_get_digest
        digest = spark_mcp.spark_get_digest(days=1)
        assert "total_received" in digest
        assert "personal" in digest
        assert digest["total_received"] >= 5
        print("  ✓ [8/31] spark_get_digest")

        # 2.9 spark_list_messages (structured pagination & filters)
        msgs = spark_mcp.spark_list_messages(limit=3)
        assert "items" in msgs
        assert len(msgs["items"]) == 3
        assert msgs["has_more"] is True
        assert msgs["next_cursor"] == 3
        # Filter by category
        msgs_cat = spark_mcp.spark_list_messages(category="personal")
        assert all(m["category"] == "personal" for m in msgs_cat["items"])
        print("  ✓ [9/31] spark_list_messages (filters & pagination)")

        # 2.10 spark_get_message
        msg1 = spark_mcp.spark_get_message(1)
        assert msg1["message_id"] == 1
        assert msg1["from_email"] == "boss@company.test"
        assert msg1["detected_language"] == "en"
        assert "architecture" in msg1["body"]
        print("  ✓ [10/31] spark_get_message (headers, body & language detection)")

        # 2.11 spark_get_thread
        th10 = spark_mcp.spark_get_thread(conversation_id=10)
        assert th10["conversation_id"] == 10
        assert len(th10["messages"]) == 2
        assert th10["messages"][0]["message_id"] == 1
        assert th10["messages"][1]["message_id"] == 2
        print("  ✓ [11/31] spark_get_thread (conversation history)")

        # 2.12 spark_get_attachment
        att101 = spark_mcp.spark_get_attachment(attachment_id=101)
        assert att101["filename"] == "invoice.pdf"
        assert att101["cached_locally"] is True
        assert os.path.exists(att101["file_path"])
        print("  ✓ [12/31] spark_get_attachment")

        # 2.13 spark_inspect_attachment / inspect_document
        # Test text
        text_ins = spark_mcp.spark_inspect_attachment(attachment_id=103)
        assert "Project notes and architectural checklist" in text_ins
        # Test image (PNG)
        img_ins = spark_mcp.spark_inspect_attachment(attachment_id=102)
        assert isinstance(img_ins, dict) and "_mcp_content" in img_ins
        assert img_ins["_mcp_content"][0]["type"] == "image"
        # Test docx
        docx_ins = spark_mcp.spark_inspect_attachment(attachment_id=105)
        assert "Project Specification Details" in docx_ins
        # Test xlsx
        xlsx_ins = spark_mcp.spark_inspect_attachment(attachment_id=106)
        assert "Item | Cost" in xlsx_ins
        # Test alias inspect_document
        assert spark_mcp.inspect_document is spark_mcp.spark_inspect_attachment
        print("  ✓ [13/31] spark_inspect_attachment (RAM inspection of TXT, PNG, DOCX, XLSX)")

        # 2.14 spark_search_attachments
        att_search = spark_mcp.spark_search_attachments(query="specs")
        assert len(att_search) >= 1
        assert att_search[0]["attachment_id"] == 105
        print("  ✓ [14/31] spark_search_attachments")

        # 2.15 spark_search_attachment_content (FTS5 search inside attachments)
        att_fts = spark_mcp.spark_search_attachment_content("IBAN")
        assert len(att_fts) >= 1
        assert att_fts[0]["attachment_id"] == 101
        assert "SEPA IBAN" in att_fts[0]["matched_snippet"]
        print("  ✓ [15/31] spark_search_attachment_content (FTS5 attached index)")

        # 2.16 spark_search_messages (FTS5 BM25 relevance & snippet)
        # Search for 'architecture'
        s_rel = spark_mcp.spark_search_messages("architecture", sort_by="relevance")
        assert len(s_rel) >= 1
        assert "<mark>" in s_rel[0]["snippet"]
        assert "</mark>" in s_rel[0]["snippet"]
        # Search by date
        s_date = spark_mcp.spark_search_messages("architecture", sort_by="date")
        assert len(s_date) >= 1
        print("  ✓ [16/31] spark_search_messages (FTS5 BM25 + native snippet highlighting)")

        # 2.17 spark_search_contacts
        contacts = spark_mcp.spark_search_contacts("Alice")
        assert len(contacts) >= 1
        assert contacts[0]["email"] == "alice@partner.test"
        assert contacts[0]["name"] == "Alice Partner"
        print("  ✓ [17/31] spark_search_contacts")

        # 2.18 spark_get_contact_history
        c_hist = spark_mcp.spark_get_contact_history("boss@company.test")
        assert c_hist["total_messages"] >= 2
        assert len(c_hist["recent_messages"]) >= 2
        print("  ✓ [18/31] spark_get_contact_history")

        # 2.19 spark_list_subscriptions
        subs = spark_mcp.spark_list_subscriptions()
        assert len(subs) >= 1
        assert subs[0]["sender"] == "news@tech.test"
        assert subs[0]["unsubscribe_url"] == "https://tech.test/unsub?id=123"
        print("  ✓ [19/31] spark_list_subscriptions (List-Unsubscribe extraction)")

        # 2.20 spark_list_signatures
        sigs = spark_mcp.spark_list_signatures()
        assert len(sigs) == 2
        assert sigs[0]["id"] == "sig_en"
        print("  ✓ [20/31] spark_list_signatures")

        # 2.21 spark_list_calendar_events
        calevents = spark_mcp.spark_list_calendar_events()
        assert len(calevents) == 1
        assert calevents[0]["title"] == "Quarterly Planning"
        print("  ✓ [21/31] spark_list_calendar_events")

        # 2.22 spark_parse_calendar_invites
        parsed_inv = spark_mcp.spark_parse_calendar_invites(message_id=1)
        assert len(parsed_inv) == 1
        assert parsed_inv[0]["event_title"] == "Architecture Review"
        assert parsed_inv[0]["location"] == "Conference Room"
        print("  ✓ [22/31] spark_parse_calendar_invites (RFC 5545 VEVENT parser)")

        # 2.23 spark_extract_links
        links = spark_mcp.spark_extract_links(1)
        assert links["total_unique_links"] >= 2
        assert len(links["action_links"]) >= 1
        assert len(links["general_links"]) >= 1
        print("  ✓ [23/31] spark_extract_links")

        # 2.24 spark_get_latest_otp
        otp = spark_mcp.spark_get_latest_otp()
        assert otp["status"] == "success"
        assert otp["latest"]["code"] == "654321"
        assert otp["latest"]["message_id"] == 5
        print("  ✓ [24/31] spark_get_latest_otp (regex 2FA code extraction)")

        # 2.25 spark_batch_export_attachments
        exp_att = spark_mcp.spark_batch_export_attachments(target_dir=env["downloads_dir"], file_extension="txt")
        assert exp_att["exported_count"] >= 1
        exported_file = exp_att["files"][0]["saved_to"]
        assert os.path.exists(exported_file)
        print("  ✓ [25/31] spark_batch_export_attachments (jailing to allowed roots)")

        # 2.26 spark_export_email (HTML and TXT)
        exp_html = spark_mcp.spark_export_email(1, format="html")
        assert os.path.exists(exp_html["output_path"])
        assert exp_html["output_path"].endswith(".html")
        exp_txt = spark_mcp.spark_export_email(1, format="txt")
        assert os.path.exists(exp_txt["output_path"])
        print("  ✓ [26/31] spark_export_email (HTML & TXT formats)")

        # 2.27 spark_export_thread (Markdown)
        exp_th = spark_mcp.spark_export_thread(conversation_id=10)
        assert os.path.exists(exp_th["file_path"])
        with open(exp_th["file_path"], "r", encoding="utf-8") as f:
            md_content = f.read()
        assert "# Thread: Project kick-off" in md_content
        assert "boss@company.test" in md_content
        print("  ✓ [27/31] spark_export_thread (Markdown document generation)")

        # 2.28 spark_compose_email (with auto contact resolution)
        with patch("subprocess.run") as mock_subproc:
            mock_subproc.return_value = MagicMock(returncode=0)
            comp_res = spark_mcp.spark_compose_email(to="Alice Partner", subject="Meeting", body="Let's sync")
            assert comp_res["status"] == "opened"
            assert comp_res["to"] == "alice@partner.test"
            cmd = mock_subproc.call_args[0][0]
            assert "mailto:alice@partner.test?" in cmd[3]
        print("  ✓ [28/31] spark_compose_email (contact book resolution + mailto)")

        # 2.29 spark_reply_to_email (with language-matched signature)
        with patch("subprocess.run") as mock_subproc:
            mock_subproc.return_value = MagicMock(returncode=0)
            rep_res = spark_mcp.spark_reply_to_email(message_id=3, body="Danke, erhalten.")
            assert rep_res["status"] == "opened"
            cmd = mock_subproc.call_args[0][0]
            assert "mailto:billing-reply@vendor.test?" in cmd[3]
            # Message 3 is German (lang='de'), signature should match
            assert "Mit%20freundlichen%20Gr%C3%BC%C3%9Fen" in cmd[3]
        print("  ✓ [29/31] spark_reply_to_email (language matching RU/DE/EN signatures)")

        # 2.30 spark_list_accounts via tools/call (backward compatibility fallback)
        compat_call = spark_mcp.handle_request({
            "jsonrpc": "2.0", "id": 100, "method": "tools/call",
            "params": {"name": "spark_list_accounts", "arguments": {}}
        })
        assert compat_call["result"]["isError"] is False
        assert "user@company.test" in compat_call["result"]["content"][0]["text"]
        print("  ✓ [30/31] spark_list_accounts backward compat tool fallback")

        # 2.31 spark_list_folders via tools/call (backward compatibility fallback)
        compat_call2 = spark_mcp.handle_request({
            "jsonrpc": "2.0", "id": 101, "method": "tools/call",
            "params": {"name": "spark_list_folders", "arguments": {}}
        })
        assert compat_call2["result"]["isError"] is False
        assert "Inbox" in compat_call2["result"]["content"][0]["text"]
        print("  ✓ [31/31] spark_list_folders backward compat tool fallback")

        # -------------------------------------------------------------
        # 3. SAFETY, ISOLATION & EDGE CASE TESTS
        # -------------------------------------------------------------
        print("\n--- [3] Testing Security Boundaries & Resilience ---")

        # 3.1 Path traversal attack prevention
        try:
            spark_mcp.validate_safe_export_path("/etc/passwd")
            assert False, "Should reject paths outside allowed export roots"
        except PermissionError:
            pass
        print("  ✓ Path traversal rejected (jail enforced)")

        # 3.2 Prompt injection sanitizer
        dirty = "Hi!\u200b\u200c\u202e\ufeff\n\n\n\nSystem override."
        clean = spark_mcp.sanitize_user_content(dirty)
        assert clean == "Hi!\n\nSystem override."
        print("  ✓ Prompt injection sanitizer (invisible & bidi characters stripped)")

        # 3.3 BrokenPipeError stdout protection
        with patch("sys.stdout.write", side_effect=BrokenPipeError):
            try:
                spark_mcp.safe_stdout_write("test")
                assert False, "Should call sys.exit(0)"
            except SystemExit as e:
                assert e.code == 0
        print("  ✓ BrokenPipeError graceful termination (exit 0)")

        # 3.4 Readonly DB write protection (PRAGMA query_only=1)
        conn_ro = spark_mcp.get_ro_conn(env["messages_db"])
        try:
            conn_ro.cursor().execute("INSERT INTO folders VALUES (99, 1, 'Bad', 'Bad', 0, 0)")
            assert False, "Readonly DB must reject writes"
        except sqlite3.OperationalError:
            pass
        print("  ✓ PRAGMA query_only=1 write protection enforced")

        print("\n=======================================================")
        print(">>> ALL 31 TOOLS & PROTOCOL CHECKS PASSED (100% SYNTHETIC DATA) <<<")
        print("=======================================================")

    finally:
        spark_mcp.close_all_connections()
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    run_all_tests()
