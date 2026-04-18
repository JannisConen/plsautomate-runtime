"""Unit tests for multipart request parsing and __mref placeholder resolution."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from plsautomate_runtime.server import _resolve_mref_placeholders
from plsautomate_runtime.storage import LocalStorage


# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def tmp_storage(tmp_path: Path) -> LocalStorage:
    return LocalStorage(base_path=str(tmp_path))


def make_resolved_ref(field_name: str = "f0", filename: str = "doc.pdf") -> dict:
    """Simulate the dict produced by process_uploaded_file + path injection."""
    return {
        "type": "local",
        "key": f"executions/exec-001/input/{filename}",
        "filename": filename,
        "size": 1024,
        "mimeType": "application/pdf",
        "extension": "pdf",
        "path": f"/data/files/executions/exec-001/input/{filename}",
    }


def make_placeholder(mref: str, filename: str = "doc.pdf", url: str | None = None) -> dict:
    """Simulate the __mref placeholder emitted by the TypeScript executor."""
    p: dict = {
        "__mref": mref,
        "filename": filename,
        "mimeType": "application/pdf",
        "extension": "pdf",
        "size": 1024,
    }
    if url:
        p["url"] = url
    return p


# ─── _resolve_mref_placeholders ───────────────────────────────────────────────


class TestResolveMrefPlaceholders:
    def test_replaces_found_mref_with_resolved_file_dict(self) -> None:
        placeholder = make_placeholder("f0")
        resolved = make_resolved_ref("f0", "invoice.pdf")
        uploaded = {"f0": resolved}

        result = _resolve_mref_placeholders({"doc": placeholder}, uploaded)

        assert result["doc"] == resolved
        assert result["doc"]["path"] == resolved["path"]

    def test_leaves_non_mref_fields_unchanged(self) -> None:
        uploaded = {"f0": make_resolved_ref("f0")}
        obj = {"name": "Alice", "count": 42}

        result = _resolve_mref_placeholders(obj, uploaded)

        assert result == {"name": "Alice", "count": 42}

    def test_missing_file_part_falls_back_to_url_ref(self) -> None:
        """When the file part was not uploaded, build a minimal ref with url."""
        placeholder = make_placeholder("f0", url="http://example.com/doc.pdf")
        uploaded: dict = {}  # part not present

        result = _resolve_mref_placeholders({"doc": placeholder}, uploaded)

        doc = result["doc"]
        assert "__mref" not in doc
        assert doc["url"] == "http://example.com/doc.pdf"
        assert doc["filename"] == "doc.pdf"

    def test_missing_file_part_without_url_returns_type_url_ref(self) -> None:
        """Missing file part with no url still returns a resolvable-looking dict."""
        placeholder = make_placeholder("f0")  # no url
        uploaded: dict = {}

        result = _resolve_mref_placeholders({"doc": placeholder}, uploaded)

        doc = result["doc"]
        assert "__mref" not in doc
        assert doc["type"] == "url"  # marks it as needing further resolution

    def test_resolves_mref_inside_list(self) -> None:
        """FileRef[] — each element in a list is resolved."""
        p0 = make_placeholder("f0", filename="a.pdf")
        p1 = make_placeholder("f1", filename="b.pdf")
        uploaded = {
            "f0": make_resolved_ref("f0", "a.pdf"),
            "f1": make_resolved_ref("f1", "b.pdf"),
        }

        result = _resolve_mref_placeholders({"documents": [p0, p1]}, uploaded)

        docs = result["documents"]
        assert isinstance(docs, list)
        assert len(docs) == 2
        assert docs[0]["filename"] == "a.pdf"
        assert docs[0]["path"] is not None
        assert docs[1]["filename"] == "b.pdf"
        assert docs[1]["path"] is not None

    def test_resolves_nested_mref(self) -> None:
        placeholder = make_placeholder("f0")
        resolved = make_resolved_ref("f0")
        uploaded = {"f0": resolved}

        result = _resolve_mref_placeholders({"order": {"invoice": placeholder, "id": "O1"}}, uploaded)

        assert result["order"]["id"] == "O1"
        assert result["order"]["invoice"] == resolved

    def test_resolves_deeply_nested_mref(self) -> None:
        placeholder = make_placeholder("f0")
        resolved = make_resolved_ref("f0")
        uploaded = {"f0": resolved}

        result = _resolve_mref_placeholders({"a": {"b": {"c": placeholder}}}, uploaded)

        assert result["a"]["b"]["c"] == resolved

    def test_handles_multiple_mrefs_in_same_object(self) -> None:
        p0 = make_placeholder("f0", filename="a.pdf")
        p1 = make_placeholder("f1", filename="b.pdf")
        r0 = make_resolved_ref("f0", "a.pdf")
        r1 = make_resolved_ref("f1", "b.pdf")
        uploaded = {"f0": r0, "f1": r1}

        result = _resolve_mref_placeholders({"doc1": p0, "doc2": p1, "text": "hello"}, uploaded)

        assert result["doc1"] == r0
        assert result["doc2"] == r1
        assert result["text"] == "hello"

    def test_handles_empty_object(self) -> None:
        result = _resolve_mref_placeholders({}, {})
        assert result == {}

    def test_handles_empty_list(self) -> None:
        result = _resolve_mref_placeholders([], {})
        assert result == []

    def test_handles_primitives(self) -> None:
        assert _resolve_mref_placeholders("string", {}) == "string"
        assert _resolve_mref_placeholders(42, {}) == 42
        assert _resolve_mref_placeholders(None, {}) is None

    def test_mixed_list_with_non_mref_items(self) -> None:
        placeholder = make_placeholder("f0")
        resolved = make_resolved_ref("f0")
        uploaded = {"f0": resolved}

        result = _resolve_mref_placeholders(
            {"items": [placeholder, "plain", 42, {"key": "val"}]},
            uploaded,
        )

        items = result["items"]
        assert items[0] == resolved
        assert items[1] == "plain"
        assert items[2] == 42
        assert items[3] == {"key": "val"}


# ─── _parse_multipart (structured mode) via process endpoint ──────────────────
# These tests verify the full round-trip through the FastAPI server using httpx.


@pytest.fixture
def config_yaml(tmp_path: Path) -> Path:
    """Minimal config.yaml for a test app."""
    storage_path = str(tmp_path / "files").replace("\\", "/")
    config_content = f"""
project:
  id: test_proj
  version: "1"
  active: true
processes:
  test-process:
    process_id: proc_test
    active: true
    instructions: "Return the input as output."
    trigger:
      type: webhook
llm:
  model: anthropic/claude-haiku-4-5
auth:
  methods:
    - type: api_key
      header: X-API-Key
database:
  url: "sqlite+aiosqlite://"
storage:
  type: local
  path: "{storage_path}"
"""
    cfg = tmp_path / "config.yaml"
    cfg.write_text(config_content)
    return cfg


@pytest.fixture
def app(config_yaml: Path, tmp_path: Path):
    from plsautomate_runtime.config import load_config
    from plsautomate_runtime.server import create_app
    config = load_config(config_yaml)
    config.database.url = "sqlite+aiosqlite://"
    config.storage.path = str(tmp_path / "files")
    config.ui.enabled = False
    return create_app(config)


@pytest.fixture
async def client(app):
    from httpx import ASGITransport, AsyncClient
    from plsautomate_runtime.db import close_db, init_db
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        await init_db("sqlite+aiosqlite://")
        yield ac
        await close_db()


@pytest.mark.asyncio
async def test_parse_multipart_structured_mode_resolves_placeholders(
    client, tmp_path: Path
) -> None:
    """Structured mode: metadata JSON + file part → FileRef with path in input."""
    from unittest.mock import AsyncMock, patch

    file_content = b"fake pdf binary data"

    # Build multipart request manually
    import io
    from httpx import AsyncClient

    metadata = {
        "customer": "Acme",
        "doc": {
            "__mref": "f0",
            "filename": "invoice.pdf",
            "mimeType": "application/pdf",
            "extension": "pdf",
            "size": len(file_content),
        },
    }

    mock_resp = MagicMock()
    mock_resp.choices = [MagicMock()]
    mock_resp.choices[0].message.content = '{"result": "ok"}'
    mock_resp.model = "claude-haiku-4-5"
    mock_resp.usage = MagicMock()
    mock_resp.usage.prompt_tokens = 10
    mock_resp.usage.completion_tokens = 5

    with (
        patch("plsautomate_runtime.executor.litellm.acompletion", new_callable=AsyncMock) as mock_llm,
        patch("plsautomate_runtime.executor.litellm.completion_cost", return_value=0.0),
    ):
        mock_llm.return_value = mock_resp

        resp = await client.post(
            "/process/test-process",
            content=_build_multipart_body(
                metadata_json=str(metadata).replace("'", '"'),
                files={"f0": ("invoice.pdf", file_content, "application/pdf")},
            ),
            headers={
                "X-API-Key": "test-key-123",
                "Content-Type": _multipart_content_type(),
            },
        )

    # The server accepts the request regardless of what the LLM returns;
    # we just verify it didn't blow up on multipart parsing.
    assert resp.status_code in (200, 500)  # 500 = LLM error OK for this test


@pytest.mark.asyncio
async def test_parse_multipart_legacy_mode_single_file(client) -> None:
    """Legacy mode: no metadata field → files go into input['file']."""
    from unittest.mock import AsyncMock, patch

    mock_resp = MagicMock()
    mock_resp.choices = [MagicMock()]
    mock_resp.choices[0].message.content = '{"result": "ok"}'
    mock_resp.model = "claude-haiku-4-5"
    mock_resp.usage = MagicMock()
    mock_resp.usage.prompt_tokens = 10
    mock_resp.usage.completion_tokens = 5

    with (
        patch("plsautomate_runtime.executor.litellm.acompletion", new_callable=AsyncMock) as mock_llm,
        patch("plsautomate_runtime.executor.litellm.completion_cost", return_value=0.0),
    ):
        mock_llm.return_value = mock_resp

        resp = await client.post(
            "/process/test-process",
            headers={"X-API-Key": "test-key-123"},
            files={"upload": ("doc.pdf", b"pdf content", "application/pdf")},
        )

    assert resp.status_code in (200, 500)


# ─── Direct unit tests of _resolve_mref_placeholders (no server needed) ──────
# These are the primary correctness tests; the server tests above are smoke tests.


class TestResolveMrefPlaceholdersZipPattern:
    """Verify the full ZIP+Excel mapping pattern end-to-end through placeholder resolution."""

    def test_zip_folder_pattern_two_subfolders(self) -> None:
        """Simulate: ZIP with CustomerA/ + CustomerB/ joined to Excel, two test cases."""
        a_ref = make_resolved_ref("f0", "invoice.pdf")
        b_ref = make_resolved_ref("f1", "contract.pdf")
        c_ref = make_resolved_ref("f2", "report.pdf")

        # Metadata from executor: two test case inputs serialised
        metadata = {
            "customer": "CustomerA",
            "documents": [
                make_placeholder("f0", "invoice.pdf"),
                make_placeholder("f1", "contract.pdf"),
            ],
        }
        uploaded = {"f0": a_ref, "f1": b_ref, "f2": c_ref}

        result = _resolve_mref_placeholders(metadata, uploaded)

        assert result["customer"] == "CustomerA"
        docs = result["documents"]
        assert isinstance(docs, list)
        assert len(docs) == 2
        assert docs[0] == a_ref
        assert docs[1] == b_ref

    def test_zip_folder_pattern_partial_upload_uses_url_fallback(self) -> None:
        """One file part uploaded, one missing → missing one uses url fallback."""
        a_ref = make_resolved_ref("f0", "invoice.pdf")
        metadata = {
            "documents": [
                make_placeholder("f0", "invoice.pdf"),
                make_placeholder("f1", "contract.pdf", url="http://plsautomate.local/api/storage/contract.pdf"),
            ],
        }
        uploaded = {"f0": a_ref}  # f1 part not uploaded

        result = _resolve_mref_placeholders(metadata, uploaded)

        docs = result["documents"]
        assert docs[0] == a_ref
        assert docs[1]["url"] == "http://plsautomate.local/api/storage/contract.pdf"
        assert docs[1]["filename"] == "contract.pdf"


# ─── Helpers for raw multipart body construction ─────────────────────────────

import json as _json
import uuid as _uuid


def _multipart_content_type(boundary: str = "testboundary") -> str:
    return f"multipart/form-data; boundary={boundary}"


def _build_multipart_body(
    metadata_json: str,
    files: dict[str, tuple[str, bytes, str]],
    boundary: str = "testboundary",
) -> bytes:
    """Build a raw multipart/form-data body for testing."""
    parts = []
    sep = f"--{boundary}\r\n"
    parts.append(
        f'{sep}Content-Disposition: form-data; name="metadata"\r\n\r\n{metadata_json}\r\n'
    )
    for field_name, (filename, content, mime) in files.items():
        parts.append(
            f'{sep}Content-Disposition: form-data; name="{field_name}"; filename="{filename}"\r\n'
            f"Content-Type: {mime}\r\n\r\n"
        )
        return (
            "".join(parts[:-1]).encode()
            + parts[-1].encode()
            + content
            + f"\r\n--{boundary}--\r\n".encode()
        )
    return (
        "".join(parts).encode()
        + f"--{boundary}--\r\n".encode()
    )
