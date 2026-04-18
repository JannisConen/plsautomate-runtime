"""Unit tests for files.py — FileRef resolution and text enrichment."""

from __future__ import annotations

import base64
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from plsautomate_runtime.files import _resolve_single, is_file_ref, resolve_file_refs
from plsautomate_runtime.storage import LocalStorage


# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def tmp_storage(tmp_path: Path) -> LocalStorage:
    """LocalStorage backed by a temporary directory, cleaned up after each test."""
    return LocalStorage(base_path=str(tmp_path))


def make_file_ref(**kwargs) -> dict:
    """Build a minimal FileRef dict, merging any overrides."""
    base = {
        "type": "local",
        "key": "testsets/abc/sources/doc.pdf",
        "filename": "doc.pdf",
        "size": 1024,
        "mimeType": "application/pdf",
        "extension": "pdf",
    }
    base.update(kwargs)
    return base


# ─── is_file_ref ──────────────────────────────────────────────────────────────


class TestIsFileRef:
    def test_valid_local_ref(self) -> None:
        assert is_file_ref(make_file_ref()) is True

    def test_valid_s3_ref(self) -> None:
        assert is_file_ref(make_file_ref(type="s3")) is True

    def test_valid_url_ref(self) -> None:
        assert is_file_ref(make_file_ref(type="url")) is True

    def test_rejects_non_dict(self) -> None:
        assert is_file_ref("string") is False
        assert is_file_ref(42) is False
        assert is_file_ref(None) is False
        assert is_file_ref([]) is False

    def test_rejects_missing_type(self) -> None:
        ref = make_file_ref()
        del ref["type"]
        assert is_file_ref(ref) is False

    def test_rejects_missing_key(self) -> None:
        ref = make_file_ref()
        del ref["key"]
        assert is_file_ref(ref) is False

    def test_rejects_missing_filename(self) -> None:
        ref = make_file_ref()
        del ref["filename"]
        assert is_file_ref(ref) is False

    def test_rejects_unknown_type(self) -> None:
        assert is_file_ref(make_file_ref(type="ftp")) is False

    def test_rejects_mref_placeholder(self) -> None:
        """__mref placeholders must NOT be treated as FileRefs."""
        placeholder = {
            "__mref": "f0",
            "filename": "doc.pdf",
            "mimeType": "application/pdf",
            "extension": "pdf",
            "size": 1024,
        }
        assert is_file_ref(placeholder) is False


# ─── _resolve_single ──────────────────────────────────────────────────────────


class TestResolveSingle:
    @pytest.mark.asyncio
    async def test_returns_immediately_when_path_already_set(
        self, tmp_storage: LocalStorage
    ) -> None:
        """FileRefs that already have a ``path`` (set by multipart parser) are returned as-is."""
        ref = make_file_ref(path="/already/resolved/doc.pdf")
        result = await _resolve_single(ref, "exec-001", tmp_storage)
        assert result is ref
        assert result["path"] == "/already/resolved/doc.pdf"

    @pytest.mark.asyncio
    async def test_decodes_base64_data(self, tmp_storage: LocalStorage) -> None:
        content = b"PDF binary content"
        encoded = base64.b64encode(content).decode()
        ref = make_file_ref(data=encoded)

        result = await _resolve_single(ref, "exec-001", tmp_storage)

        assert result["path"] is not None
        stored = await tmp_storage.get(result["key"])
        assert stored == content

    @pytest.mark.asyncio
    async def test_downloads_from_url(self, tmp_storage: LocalStorage) -> None:
        content = b"downloaded content"
        ref = make_file_ref(url="http://example.com/file.pdf")

        with patch("plsautomate_runtime.files.httpx.AsyncClient") as mock_client_cls:
            mock_resp = AsyncMock()
            mock_resp.content = content
            mock_resp.raise_for_status = MagicMock()
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=AsyncMock(get=AsyncMock(return_value=mock_resp)))
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await _resolve_single(ref, "exec-001", tmp_storage)

        assert result["path"] is not None
        stored = await tmp_storage.get(result["key"])
        assert stored == content

    @pytest.mark.asyncio
    async def test_falls_back_to_local_storage(self, tmp_storage: LocalStorage) -> None:
        content = b"existing local content"
        key = "testsets/abc/sources/doc.pdf"
        await tmp_storage.put(key, content, "application/pdf")

        ref = make_file_ref(key=key)
        result = await _resolve_single(ref, "exec-001", tmp_storage)

        assert result["path"] is not None

    @pytest.mark.asyncio
    async def test_returns_unresolved_marker_when_all_sources_fail(
        self, tmp_storage: LocalStorage
    ) -> None:
        ref = make_file_ref()  # no data, no url, key not in storage
        result = await _resolve_single(ref, "exec-001", tmp_storage)
        assert result.get("_unresolved") is True

    @pytest.mark.asyncio
    async def test_text_files_resolved_without_content_field(self, tmp_storage: LocalStorage) -> None:
        """Content is no longer set by resolve — use FileInput.get_info() instead."""
        content = b"Hello, world!"
        encoded = base64.b64encode(content).decode()
        ref = make_file_ref(filename="note.txt", extension="txt", mimeType="text/plain", data=encoded)

        result = await _resolve_single(ref, "exec-001", tmp_storage)

        assert "content" not in result
        assert result.get("path") is not None

    @pytest.mark.asyncio
    async def test_binary_files_resolved_with_path(self, tmp_storage: LocalStorage) -> None:
        content = b"%PDF binary"
        encoded = base64.b64encode(content).decode()
        ref = make_file_ref(data=encoded)  # PDF

        result = await _resolve_single(ref, "exec-001", tmp_storage)

        assert "content" not in result
        assert result.get("path") is not None

    @pytest.mark.asyncio
    async def test_result_has_type_and_key(self, tmp_storage: LocalStorage) -> None:
        content = b"data"
        encoded = base64.b64encode(content).decode()
        ref = make_file_ref(data=encoded)

        result = await _resolve_single(ref, "exec-001", tmp_storage)

        assert result["type"] == "local"
        assert "key" in result
        assert result["filename"] == "doc.pdf"


# ─── resolve_file_refs ────────────────────────────────────────────────────────


class TestResolveFileRefs:
    @pytest.mark.asyncio
    async def test_passes_through_non_ref_values(self, tmp_storage: LocalStorage) -> None:
        obj = {"name": "Alice", "count": 42, "active": True}
        result = await resolve_file_refs(obj, "exec-001", tmp_storage)
        assert result == obj

    @pytest.mark.asyncio
    async def test_resolves_single_file_ref(self, tmp_storage: LocalStorage) -> None:
        content = b"pdf"
        key = "testsets/abc/sources/doc.pdf"
        await tmp_storage.put(key, content, "application/pdf")

        obj = {"doc": make_file_ref(key=key)}
        result = await resolve_file_refs(obj, "exec-001", tmp_storage)

        assert isinstance(result, dict)
        assert "path" in result["doc"]

    @pytest.mark.asyncio
    async def test_resolves_file_ref_array(self, tmp_storage: LocalStorage) -> None:
        """FileRef[] — each item in the array is resolved independently."""
        key1 = "testsets/abc/sources/a.pdf"
        key2 = "testsets/abc/sources/b.pdf"
        await tmp_storage.put(key1, b"a", "application/pdf")
        await tmp_storage.put(key2, b"b", "application/pdf")

        refs = [make_file_ref(key=key1, filename="a.pdf"), make_file_ref(key=key2, filename="b.pdf")]
        result = await resolve_file_refs(refs, "exec-001", tmp_storage)

        assert isinstance(result, list)
        assert len(result) == 2
        assert "path" in result[0]
        assert "path" in result[1]

    @pytest.mark.asyncio
    async def test_resolves_file_refs_nested_in_array_field(
        self, tmp_storage: LocalStorage
    ) -> None:
        """input.documents: FileRef[] — the list elements are resolved."""
        key1 = "testsets/abc/sources/a.pdf"
        key2 = "testsets/abc/sources/b.pdf"
        await tmp_storage.put(key1, b"a", "application/pdf")
        await tmp_storage.put(key2, b"b", "application/pdf")

        obj = {
            "customer": "Acme",
            "documents": [
                make_file_ref(key=key1, filename="a.pdf"),
                make_file_ref(key=key2, filename="b.pdf"),
            ],
        }
        result = await resolve_file_refs(obj, "exec-001", tmp_storage)

        assert result["customer"] == "Acme"
        docs = result["documents"]
        assert isinstance(docs, list)
        assert len(docs) == 2
        assert "path" in docs[0]
        assert "path" in docs[1]

    @pytest.mark.asyncio
    async def test_already_resolved_refs_returned_as_is(
        self, tmp_storage: LocalStorage
    ) -> None:
        """Refs with a ``path`` set (from multipart parser) are not re-resolved."""
        ref = make_file_ref(path="/already/resolved.pdf")
        obj = {"doc": ref}
        result = await resolve_file_refs(obj, "exec-001", tmp_storage)
        assert result["doc"]["path"] == "/already/resolved.pdf"

    @pytest.mark.asyncio
    async def test_resolves_nested_objects(self, tmp_storage: LocalStorage) -> None:
        key = "testsets/abc/sources/doc.pdf"
        await tmp_storage.put(key, b"data", "application/pdf")

        obj = {"order": {"invoice": make_file_ref(key=key), "id": "O1"}}
        result = await resolve_file_refs(obj, "exec-001", tmp_storage)

        assert result["order"]["id"] == "O1"
        assert "path" in result["order"]["invoice"]

    @pytest.mark.asyncio
    async def test_handles_empty_dict(self, tmp_storage: LocalStorage) -> None:
        result = await resolve_file_refs({}, "exec-001", tmp_storage)
        assert result == {}

    @pytest.mark.asyncio
    async def test_handles_empty_list(self, tmp_storage: LocalStorage) -> None:
        result = await resolve_file_refs([], "exec-001", tmp_storage)
        assert result == []

    @pytest.mark.asyncio
    async def test_handles_primitive(self, tmp_storage: LocalStorage) -> None:
        result = await resolve_file_refs("just a string", "exec-001", tmp_storage)
        assert result == "just a string"
