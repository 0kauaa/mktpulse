from __future__ import annotations

import pytest

from mktpulse.storage.local import LocalStorage


def test_local_storage_roundtrip_and_listing(tmp_path) -> None:
    storage = LocalStorage(tmp_path / "lake")

    storage.write_bytes("a/b/one.txt", b"one")
    storage.write_bytes("a/c/two.txt", b"two")

    assert storage.read_bytes("a/b/one.txt") == b"one"
    assert storage.exists("a/b/one.txt") is True
    assert storage.exists("a/b/missing.txt") is False
    assert storage.list("a/") == ["a/b/one.txt", "a/c/two.txt"]
    assert storage.list("a/b/") == ["a/b/one.txt"]


def test_overwrite_is_atomic_and_leaves_no_temp_file(tmp_path) -> None:
    storage = LocalStorage(tmp_path / "lake")

    storage.write_bytes("table/quotes.parquet", b"old")
    storage.write_bytes("table/quotes.parquet", b"new")

    assert storage.read_bytes("table/quotes.parquet") == b"new"
    assert list((tmp_path / "lake").rglob(".tmp-*")) == []


@pytest.mark.parametrize("key", ["/absolute/path", "../escape", "a/../../escape", "a\\b"])
def test_invalid_keys_are_rejected(tmp_path, key: str) -> None:
    storage = LocalStorage(tmp_path / "lake")
    with pytest.raises(ValueError):
        storage.write_bytes(key, b"x")
