#!/usr/bin/env python3
"""
Self-check test for spark_inspect_attachment (and inspect_document alias).
Zero external test frameworks, pure assert-based.
"""

import io
import json
import os
import tempfile
from unittest.mock import patch, MagicMock

import spark_mcp


def test_tool_registration():
    assert "spark_inspect_attachment" in spark_mcp.TOOL_NAMES
    assert "spark_inspect_document" in spark_mcp.TOOL_NAMES
    assert "inspect_document" in spark_mcp.TOOL_NAMES
    assert spark_mcp.spark_inspect_document is spark_mcp.spark_inspect_attachment
    assert spark_mcp.inspect_document is spark_mcp.spark_inspect_attachment


def test_missing_args():
    try:
        spark_mcp.spark_inspect_attachment()
        assert False, "Should raise ValueError when neither attachment_id nor message_id is provided"
    except ValueError as e:
        assert "Provide either attachment_id" in str(e)


def test_image_inspection_in_ram():
    fake_png = b"\x89PNG\r\n\x1a\nfakeimagebytes"
    mock_att = {
        "pk": 101,
        "messagePk": 202,
        "attachmentName": "photo.png",
        "attachmentSize": len(fake_png),
        "attachmentMIMEType": "image/png",
        "attachmentURL": "file:///path/photo.png",
        "accountPk": 1,
    }

    with patch("spark_mcp.get_ro_conn") as mock_conn, \
         patch("spark_mcp.find_cached_attachment_file", return_value="/tmp/photo.png"), \
         patch("os.path.isfile", return_value=True), \
         patch("builtins.open", MagicMock(return_value=io.BytesIO(fake_png))):

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = mock_att
        mock_conn.return_value.cursor.return_value = mock_cursor

        res = spark_mcp.spark_inspect_attachment(attachment_id=101)
        assert isinstance(res, dict)
        assert "_mcp_content" in res
        content = res["_mcp_content"]
        assert len(content) == 1
        assert content[0]["type"] == "image"
        assert content[0]["mimeType"] == "image/png"
        assert content[0]["data"] == "iVBORw0KGgpmYWtlaW1hZ2VieXRlcw=="


def test_text_inspection_in_ram():
    fake_text = "Sample invoice text content\nLine 2".encode("utf-8")
    mock_att = {
        "pk": 102,
        "messagePk": 203,
        "attachmentName": "invoice.txt",
        "attachmentSize": len(fake_text),
        "attachmentMIMEType": "text/plain",
        "attachmentURL": "",
        "accountPk": 1,
    }

    with patch("spark_mcp.get_ro_conn") as mock_conn, \
         patch("spark_mcp.find_cached_attachment_file", return_value="/tmp/invoice.txt"), \
         patch("os.path.isfile", return_value=True), \
         patch("builtins.open", MagicMock(return_value=io.BytesIO(fake_text))):

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = mock_att
        mock_conn.return_value.cursor.return_value = mock_cursor

        res = spark_mcp.spark_inspect_attachment(attachment_id=102)
        assert isinstance(res, str)
        assert res == "Sample invoice text content\nLine 2"


def test_pdf_inspection_in_ram():
    fake_pdf_text = "Contract line 1\nContract line 2"
    mock_att = {
        "pk": 103,
        "messagePk": 204,
        "attachmentName": "contract.pdf",
        "attachmentSize": 1000,
        "attachmentMIMEType": "application/pdf",
        "attachmentURL": "",
        "accountPk": 1,
    }

    class MockPage:
        def extract_text(self):
            return fake_pdf_text

    class MockReader:
        def __init__(self, stream):
            self.pages = [MockPage()]

    with patch("spark_mcp.get_ro_conn") as mock_conn, \
         patch("spark_mcp.find_cached_attachment_file", return_value="/tmp/contract.pdf"), \
         patch("os.path.isfile", return_value=True), \
         patch("builtins.open", MagicMock(return_value=io.BytesIO(b"%PDF-1.4..."))), \
         patch.dict("sys.modules", {"pypdf": MagicMock(PdfReader=MockReader)}):

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = mock_att
        mock_conn.return_value.cursor.return_value = mock_cursor

        res = spark_mcp.spark_inspect_attachment(attachment_id=103)
        assert isinstance(res, str)
        assert "Contents of document 'contract.pdf' (1 pages):" in res
        assert fake_pdf_text in res


def test_handle_request_protocol():
    # Test image unwrapping in JSON-RPC handle_request
    fake_png = b"\x89PNG\r\n\x1a\nfakeimagebytes"
    mock_att = {
        "pk": 104,
        "messagePk": 205,
        "attachmentName": "scan.jpg",
        "attachmentSize": len(fake_png),
        "attachmentMIMEType": "image/jpeg",
        "attachmentURL": "",
        "accountPk": 1,
    }

    with patch("spark_mcp.get_ro_conn") as mock_conn, \
         patch("spark_mcp.find_cached_attachment_file", return_value="/tmp/scan.jpg"), \
         patch("os.path.isfile", return_value=True), \
         patch("builtins.open", MagicMock(return_value=io.BytesIO(fake_png))):

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = mock_att
        mock_conn.return_value.cursor.return_value = mock_cursor

        req = {
            "jsonrpc": "2.0",
            "id": "test-1",
            "method": "tools/call",
            "params": {
                "name": "inspect_document",
                "arguments": {"attachment_id": 104}
            }
        }
        resp = spark_mcp.handle_request(req)
        assert resp["id"] == "test-1"
        assert resp["result"]["isError"] is False
        content = resp["result"]["content"]
        assert len(content) == 1
        assert content[0]["type"] == "image"
        assert content[0]["mimeType"] == "image/jpeg"


def test_zero_disk_footprint():
    before_files = set(os.listdir(tempfile.gettempdir()))

    fake_text = "Testing memory only".encode("utf-8")
    mock_att = {
        "pk": 105,
        "messagePk": 206,
        "attachmentName": "notes.md",
        "attachmentSize": len(fake_text),
        "attachmentMIMEType": "text/markdown",
        "attachmentURL": "",
        "accountPk": 1,
    }

    with patch("spark_mcp.get_ro_conn") as mock_conn, \
         patch("spark_mcp.find_cached_attachment_file", return_value="/tmp/notes.md"), \
         patch("os.path.isfile", return_value=True), \
         patch("builtins.open", MagicMock(return_value=io.BytesIO(fake_text))):

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = mock_att
        mock_conn.return_value.cursor.return_value = mock_cursor

        res = spark_mcp.spark_inspect_attachment(attachment_id=105)
        assert res == "Testing memory only"

    after_files = set(os.listdir(tempfile.gettempdir()))
    # Temp directory was untouched by this inspection
    assert len(after_files - before_files) == 0


if __name__ == "__main__":
    test_tool_registration()
    test_missing_args()
    test_image_inspection_in_ram()
    test_text_inspection_in_ram()
    test_pdf_inspection_in_ram()
    test_handle_request_protocol()
    test_zero_disk_footprint()
    print("ALL CHECKS PASSED SUCCESSFULLY.")
