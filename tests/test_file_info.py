"""Tests for the FileInfo semantic file type system."""

from __future__ import annotations

import base64
import json
import tempfile
from pathlib import Path

import pytest

from plsautomate_runtime.file_info import FileInfo, parse_file_info
from plsautomate_runtime.types import FileInput


# ─── Helpers ──────────────────────────────────────────────────────────────────


def make_file_input(
    filename: str,
    content: bytes | str | None = None,
    *,
    use_path: bool = False,
    mime_type: str | None = None,
) -> FileInput:
    """Create a FileInput for testing.

    If use_path=True, writes content to a temp file and sets path.
    Otherwise, base64-encodes content into data field.
    """
    ext = filename.rsplit(".", 1)[-1] if "." in filename else ""

    if content is None:
        return FileInput(filename=filename, extension=ext, mime_type=mime_type)

    raw = content.encode("utf-8") if isinstance(content, str) else content

    if use_path:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=f".{ext}")
        tmp.write(raw)
        tmp.close()
        return FileInput(
            filename=filename,
            extension=ext,
            size=len(raw),
            mime_type=mime_type,
            path=tmp.name,
        )

    return FileInput(
        filename=filename,
        extension=ext,
        size=len(raw),
        mime_type=mime_type,
        data=base64.b64encode(raw).decode(),
    )


# ─── Text files ───────────────────────────────────────────────────────────────


class TestTextFiles:
    def test_txt_file(self) -> None:
        fi = make_file_input("hello.txt", "Hello, world!")
        info = fi.get_info()
        assert info.type == "text"
        assert info.text == "Hello, world!"
        assert info.encoding == "utf-8"
        assert info.line_count == 1

    def test_multiline_text(self) -> None:
        fi = make_file_input("notes.md", "line1\nline2\nline3")
        info = fi.get_info()
        assert info.type == "text"
        assert info.line_count == 3

    def test_csv_is_text(self) -> None:
        fi = make_file_input("data.csv", "name,age\nAlice,30")
        info = fi.get_info()
        assert info.type == "text"
        assert "Alice" in info.text

    def test_code_file(self) -> None:
        fi = make_file_input("app.py", "print('hello')")
        info = fi.get_info()
        assert info.type == "text"

    def test_path_based_text(self) -> None:
        fi = make_file_input("test.txt", "from path", use_path=True)
        info = fi.get_info()
        assert info.text == "from path"


# ─── Email files ──────────────────────────────────────────────────────────────


def _make_simple_eml() -> bytes:
    """Build a minimal .eml file."""
    return (
        b"From: sender@example.com\r\n"
        b"To: recipient@example.com\r\n"
        b"Cc: cc@example.com\r\n"
        b"Subject: Test Email\r\n"
        b"Date: Mon, 1 Jan 2024 12:00:00 +0000\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n"
        b"\r\n"
        b"Hello, this is the body.\r\n"
    )


def _make_multipart_eml_with_attachment() -> bytes:
    """Build a multipart .eml with a text attachment."""
    boundary = "----=_Part_123"
    return (
        f"From: sender@example.com\r\n"
        f"To: recipient@example.com\r\n"
        f"Subject: With Attachment\r\n"
        f"Content-Type: multipart/mixed; boundary=\"{boundary}\"\r\n"
        f"\r\n"
        f"--{boundary}\r\n"
        f"Content-Type: text/plain; charset=utf-8\r\n"
        f"\r\n"
        f"Main body text.\r\n"
        f"--{boundary}\r\n"
        f"Content-Type: text/plain; charset=utf-8\r\n"
        f"Content-Disposition: attachment; filename=\"notes.txt\"\r\n"
        f"\r\n"
        f"Attachment content here.\r\n"
        f"--{boundary}--\r\n"
    ).encode()


class TestEmailFiles:
    def test_simple_eml(self) -> None:
        fi = make_file_input("email.eml", _make_simple_eml())
        info = fi.get_info()
        assert info.type == "email"
        assert info.subject == "Test Email"
        assert info.sender == "sender@example.com"
        assert info.to == ["recipient@example.com"]
        assert info.cc == ["cc@example.com"]
        assert info.recipients is not None
        assert "recipient@example.com" in info.recipients
        assert "cc@example.com" in info.recipients
        assert info.date == "Mon, 1 Jan 2024 12:00:00 +0000"
        assert info.body is not None
        assert "Hello, this is the body" in info.body
        assert "From:" in info.text
        assert "Subject: Test Email" in info.text

    def test_eml_with_attachment(self) -> None:
        fi = make_file_input("email.eml", _make_multipart_eml_with_attachment())
        info = fi.get_info()
        assert info.type == "email"
        assert info.body is not None
        assert "Main body text" in info.body
        assert info.attachments is not None
        assert len(info.attachments) == 1
        att = info.attachments[0]
        assert att.filename == "notes.txt"
        assert att.type == "text"
        assert "Attachment content" in att.text

    def test_eml_no_data(self) -> None:
        fi = make_file_input("empty.eml")
        info = fi.get_info()
        assert info.type == "email"
        assert "no data available" in info.text

    def test_mime_type_detection(self) -> None:
        fi = make_file_input("mail.dat", _make_simple_eml(), mime_type="message/rfc822")
        info = fi.get_info()
        assert info.type == "email"


# ─── Structured files ────────────────────────────────────────────────────────


class TestStructuredFiles:
    def test_json_file(self) -> None:
        data = {"name": "Alice", "age": 30}
        fi = make_file_input("data.json", json.dumps(data))
        info = fi.get_info()
        assert info.type == "structured"
        assert info.format == "json"
        assert info.data == data

    def test_jsonl_file(self) -> None:
        content = '{"a": 1}\n{"a": 2}\n'
        fi = make_file_input("data.jsonl", content)
        info = fi.get_info()
        assert info.type == "structured"
        assert info.format == "json"
        assert isinstance(info.data, list)
        assert len(info.data) == 2

    def test_xml_file(self) -> None:
        content = "<root><item>hello</item></root>"
        fi = make_file_input("data.xml", content)
        info = fi.get_info()
        assert info.type == "structured"
        assert info.format == "xml"
        assert info.data is not None

    def test_invalid_json(self) -> None:
        fi = make_file_input("bad.json", "{invalid json")
        info = fi.get_info()
        assert info.type == "structured"
        assert info.data is None  # parse failed
        assert info.text == "{invalid json"  # raw content preserved


# ─── PDF files ────────────────────────────────────────────────────────────────


class TestPdfFiles:
    def test_pdf_without_library(self) -> None:
        fi = make_file_input("doc.pdf", b"%PDF-1.4 fake content")
        info = fi.get_info()
        assert info.type == "pdf"
        # Without pypdf installed, should get fallback message
        # (if pypdf IS installed, this will have real content)
        assert info.text  # non-empty either way

    def test_pdf_no_data(self) -> None:
        fi = make_file_input("empty.pdf")
        info = fi.get_info()
        assert info.type == "pdf"
        assert "no data available" in info.text


# ─── Image files ──────────────────────────────────────────────────────────────


class TestImageFiles:
    def test_image_without_pillow(self) -> None:
        fi = make_file_input("photo.png", b"\x89PNG fake")
        info = fi.get_info()
        assert info.type == "image"
        assert "photo.png" in info.text

    def test_image_no_data(self) -> None:
        fi = make_file_input("missing.jpg")
        info = fi.get_info()
        assert info.type == "image"


# ─── Binary files ─────────────────────────────────────────────────────────────


class TestBinaryFiles:
    def test_unknown_extension(self) -> None:
        fi = make_file_input("data.xyz", b"\x00\x01\x02")
        info = fi.get_info()
        assert info.type == "binary"
        assert "data.xyz" in info.text

    def test_binary_no_data(self) -> None:
        fi = make_file_input("blob.bin")
        info = fi.get_info()
        assert info.type == "binary"


# ─── Spreadsheet files ───────────────────────────────────────────────────────


class TestSpreadsheetFiles:
    def test_xlsx_without_library(self) -> None:
        fi = make_file_input("sheet.xlsx", b"PK fake xlsx")
        info = fi.get_info()
        assert info.type == "spreadsheet"
        # Without openpyxl, should get fallback
        assert info.text

    def test_xlsx_no_data(self) -> None:
        fi = make_file_input("empty.xlsx")
        info = fi.get_info()
        assert info.type == "spreadsheet"
        assert "no data available" in info.text


# ─── Lazy caching ────────────────────────────────────────────────────────────


class TestCaching:
    def test_get_info_cached(self) -> None:
        fi = make_file_input("test.txt", "hello")
        info1 = fi.get_info()
        info2 = fi.get_info()
        assert info1 is info2  # same object


# ─── Type classification ─────────────────────────────────────────────────────


class TestClassification:
    @pytest.mark.parametrize("ext,expected", [
        ("eml", "email"),
        ("msg", "email"),
        ("pdf", "pdf"),
        ("txt", "text"),
        ("log", "text"),
        ("py", "text"),
        ("md", "text"),
        ("html", "text"),
        ("csv", "text"),
        ("json", "structured"),
        ("xml", "structured"),
        ("yaml", "structured"),
        ("toml", "structured"),
        ("xlsx", "spreadsheet"),
        ("xls", "spreadsheet"),
        ("png", "image"),
        ("jpg", "image"),
        ("gif", "image"),
        ("exe", "binary"),
        ("zip", "binary"),
    ])
    def test_extension_classification(self, ext: str, expected: str) -> None:
        fi = make_file_input(f"test.{ext}", b"data")
        info = fi.get_info()
        assert info.type == expected
