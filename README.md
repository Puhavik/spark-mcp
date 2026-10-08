# Spark Desktop MCP Server

An unofficial [Model Context Protocol](https://modelcontextprotocol.io) (MCP) server for the [Spark Desktop](https://sparkmailapp.com) email client on macOS.

It lets AI assistants such as Claude Desktop or Claude Code read, search and summarize your local Spark mail, find attachments, invoices, verification codes and calendar invites, and open pre-filled drafts in Spark.

> This project is not affiliated with, endorsed by, or supported by Readdle. "Spark" is a trademark of its respective owner.

## Highlights

- **Single file, zero dependencies.** One script, `spark_mcp.py`, using only the Python 3 standard library. Speaks JSON-RPC over stdio.
- **Local reads are read-only.** Spark databases are opened with `?mode=ro`. Spark uses WAL mode, so reading never blocks Spark itself.
- **Local tools never send mail.** `spark_compose_email` and `spark_reply_to_email` only open drafts in the Spark composer. The delegated `spark_cli_*` tools can act up to the access level you grant in Spark: `spark_cli_action` can send and archive, `spark_cli_draft` can delete drafts permanently, and `spark_cli_event` can send invitations. Grant only the access you want an agent to have.
- **No network requests.** The server itself talks only to local files and the local Spark app.
- **HTML to text.** Strips styles and scripts and returns readable message text.
- **Quote stripping.** Optionally returns only the new text of a reply, without repeated quoted history.
- **Language detection.** Returns the message language (`de`, `en`, `ru`, ...) detected by Spark.
- **Unreplied emails.** Personal inbox emails that have no reply yet.
- **Smart reply.** Replies go to the `Reply-To` address when present and get a signature that matches the message language (RU/DE/EN).
- **Full Spark CLI coverage (`spark_cli_*` tools).** Calendar events and RSVP, availability, drafts, comments, email and contact actions, templates, teams, meetings and Spark's own semantic search are delegated to Spark's CLI, the same backend as the official Spark MCP. The tool list is read from `spark tools`, so it always matches your Spark version and access level. Setup: in Spark **Settings > AI Agents** click **Setup CLI** (this creates `/usr/local/bin/spark`) and set an access level per account. Spark must be running. These tools can write (archive, snooze, create events) up to the level you grant; local SQLite tools work without Spark running.
- **Calendar invites.** Parses `VEVENT` blocks from `.ics` attachments (respects `TZID`, decodes RFC 5545 escapes).
- **Link extraction.** Splits message links into action/tracking, unsubscribe and other links.
- **Full-text search.** Uses Spark's own FTS5 indexes for messages (prefix search) and attachment contents.
- **Document inspection (PDF, DOCX, XLSX, Images).** Reads PDF, Word (.docx), Excel (.xlsx), images and text attachments entirely in RAM with zero external dependencies (pure Python standard library).
- **Prompt injection defense.** Automatically strips zero-width invisible Unicode characters, bidi overrides, and malicious formatting from untrusted incoming emails.
- **Tool surface restriction (`SPARK_EXPOSED_TOOLS`).** Configurable tool exposure (`all`, `read-only`, `read-only+spark_compose_email`, or `core`) to protect against unauthorized writes and save thousands of tokens in Claude's context window.
- **Safe export directory jailing.** Exports default to and are strictly jailed inside `~/Downloads` (customizable via `SPARK_ALLOWED_ROOTS`), preventing directory traversal.
- **Date range filtering.** Filter messages by relative days (`days=7`) or specific date bounds (`since_date`, `until_date` in ISO format or timestamps).
- **Smart contact resolution.** Compose emails using human names or nicknames; recipient names without `@` are automatically resolved against Spark's contact book.
- **Export.** Messages to HTML, TXT or EML; threads to Markdown (jailed to `~/Downloads`).

## Requirements

- macOS
- [Spark Desktop](https://sparkmailapp.com) (the current Spark for Mac, app name "Spark Desktop"), installed and signed in at least once
- Python 3.9 or newer (`python3 --version`). Install it with `xcode-select --install` or from [python.org](https://www.python.org/downloads/macos/) if missing.
- An MCP client, for example [Claude Desktop](https://claude.ai/download) or [Claude Code](https://docs.claude.com/en/docs/claude-code)

## Quick Installation (1-Step Auto Install)

Run this one-liner in Terminal to download and auto-configure Spark MCP for Claude Desktop, Cursor, and Antigravity:

```bash
curl -fsSL https://raw.githubusercontent.com/Puhavik/spark-mcp/main/spark_mcp.py -o ~/spark_mcp.py && python3 ~/spark_mcp.py --install
```

Or if you clone the repo:

```bash
git clone https://github.com/Puhavik/spark-mcp.git ~/spark-mcp
python3 ~/spark-mcp/spark_mcp.py --install
```

Restart Claude Desktop (Cmd+Q) and the tools will appear immediately in your chat!

To remove:
```bash
python3 ~/spark-mcp/spark_mcp.py --uninstall
```

---

## Manual Installation

### 1. Get the script

Clone the repository:

```bash
git clone https://github.com/Puhavik/spark-mcp.git ~/spark-mcp
```

Or download only the script:

```bash
mkdir -p ~/spark-mcp && curl -fsSL https://raw.githubusercontent.com/Puhavik/spark-mcp/main/spark_mcp.py -o ~/spark-mcp/spark_mcp.py
```

### 2. Find the absolute paths

MCP clients need absolute paths. Print them:

```bash
which python3
```

```bash
echo ~/spark-mcp/spark_mcp.py
```

### 3. Check that the server starts

```bash
echo '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | python3 ~/spark-mcp/spark_mcp.py
```

You should see a JSON response with the list of tools.

### 4. Connect it to your MCP client

#### Claude Desktop

1. Open Claude Desktop, then **Settings > Developer > Edit Config**. This opens `~/Library/Application Support/Claude/claude_desktop_config.json`.
2. Add the server. Replace both paths with the values from step 2:

   ```json
   {
     "mcpServers": {
       "spark-mail": {
         "command": "/usr/bin/python3",
         "args": ["/Users/YOUR_USER/spark-mcp/spark_mcp.py"]
       }
     }
   }
   ```

   If the file already has an `mcpServers` object, add only the `"spark-mail"` entry inside it.
3. Quit Claude Desktop completely (Cmd+Q) and open it again.
4. The Spark tools now appear in the tools menu of a new chat.

#### Claude Code

```bash
claude mcp add spark-mail -- python3 ~/spark-mcp/spark_mcp.py
```

Add `--scope user` to make it available in all projects. Check with `claude mcp list`.

#### Other MCP clients

Any client that supports stdio servers works. Use `python3` as the command and the absolute path to `spark_mcp.py` as the only argument.

### 5. Try it

Ask your assistant, for example:

- "What unread emails do I have?"
- "Which personal emails are still waiting for my reply?"
- "Find my latest verification code from Apple."
- "Give me a digest of the last 3 days."
- "Find all invoices from last month and copy the PDFs to ~/Downloads/invoices."
- "Draft a reply to the last email from Anna saying I agree."

## Tools (28)

### Mail and inbox triage
- `spark_list_accounts`: all mail accounts configured in Spark.
- `spark_get_unread_summary`: unread counts across all accounts and inboxes.
- `spark_find_unreplied_emails`: personal inbox emails waiting for a reply, with waiting time.
- `spark_get_latest_otp`: recent 2FA / OTP codes and verification links.
- `spark_get_digest`: digest for the last N days, grouped by category.
- `spark_list_folders`: folders with message counts.

### Reading and threads
- `spark_list_messages`: messages with filters (`account_id`, `folder_id`, `category`, `days`, `since_date`, `until_date`, `only_inbox`, `only_unseen`, `only_starred`, `limit`, `offset`).
- `spark_list_threads`: threads with participants, total and unread counts, inbox status.
- `spark_get_message`: full message (subject, body, recipients, language, category, attachments; optional `exclude_quoted_history`).
- `spark_get_thread`: all messages of a thread by `conversation_id` or `message_id`.

### Attachments and invoices
- `spark_inspect_attachment` (alias `inspect_document`): inspect and read attachment contents (PDF text, images, text files) entirely in RAM without saving to disk.
- `spark_get_attachment`: attachment metadata and its path in Spark's local cache.
- `spark_search_attachments`: search attachments by file name or MIME type.
- `spark_search_attachment_content`: full-text search inside PDFs and documents.
- `spark_batch_export_attachments`: copy cached attachments matching filters (extension, sender, query) to a folder. Files are saved as `<message_id>_<file_name>`.
- `spark_find_invoices`: invoices, receipts and bills (also German "Rechnung", "Beleg", "Quittung").

### Search, orders and newsletters
- `spark_search_messages`: full-text search across all accounts.
- `spark_find_deliveries`: shipping and order emails with order numbers and tracking links.
- `spark_list_subscriptions`: newsletters with unsubscribe links (web links preferred over `mailto:`).

### Calendar and invites
- `spark_parse_calendar_invites`: meeting details (title, dates, location, description, status) from `.ics` attachments.
- `spark_list_calendar_events`: events from Spark's calendar database. Returns an empty list while the Spark calendar is disabled.

### Contacts
- `spark_search_contacts`: search contacts by name or address.
- `spark_get_contact_history`: correspondence history with one contact.

### Links
- `spark_extract_links`: message links by category (from `<a>` tags and visible text).

### Signatures and export
- `spark_list_signatures`: active signatures from Spark settings.
- `spark_export_email`: export a message to HTML, TXT or EML.
- `spark_export_thread`: export a thread to Markdown (default path `~/Downloads/Thread_<id>_<subject>.md`).

### Composing and replying
- `spark_compose_email`: open a new pre-filled message in Spark (supports emails or contact names with automatic address resolution).
- `spark_reply_to_email`: open a pre-filled reply (respects `Reply-To`, `Re:` subject, signature in the message language).

## Limitations

- **Sender account.** A `mailto:` link cannot select the sender, so Spark always opens the draft from the default account. Switch the sender in the composer before replying from another mailbox.
- **Signatures.** Spark does not link signatures to accounts. A reply is signed only when a signature contains the account owner's name.
- **No sending.** Nothing is sent directly. New emails and replies only open as drafts in Spark.
- **Attachments.** Only files that Spark has already downloaded to its cache can be found and exported. To download an uncached one use `spark_cli_attachment`.
- **Undocumented format.** The server reads Spark's internal SQLite databases. A Spark update can change their schema and break some tools.

## Troubleshooting

- **`Database not found`.** Check that Spark Desktop is installed and signed in, and that this folder exists: `~/Library/Application Support/Spark Desktop/core-data`.
- **Permission errors.** Give your MCP client (for example Claude Desktop, or your terminal for Claude Code) access in **System Settings > Privacy & Security > Full Disk Access**, then restart it.
- **Tools do not appear.** Check that both paths in the config are absolute, then fully restart the client. Claude Desktop logs are in `~/Library/Logs/Claude/`.
- **Changes to the script are not visible.** Restart the client. It loads the server at startup.

## Privacy

The server only reads local files and does not connect to the internet. However, everything a tool returns (email text, addresses, codes) goes to the AI model your client uses. Read your AI provider's privacy policy before you connect your mailbox.

## Contributing

Issues and pull requests are welcome. Keep the server dependency-free and in one file.

## License

Copyright (C) 2026 Vikentiy Pukhaev

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. See [LICENSE](LICENSE).
