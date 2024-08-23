from hamilton import registry

registry.load_extension("pandas")

import pandas as pd
import pytest

from hamilton.caching.fingerprinting import hash_value
from hamilton.caching.store import ResultRetrievalError, ShelveResultStore, cache_to_file


def test_initialize_empty(tmp_path):
    shelve_cache = ShelveResultStore(tmp_path / "h-cache")
    assert shelve_cache.empty


def test_not_empty_after_set(tmp_path):
    shelve_cache = ShelveResultStore(tmp_path / "h-cache")
    shelve_cache.set(key="foo", value="bar")
    assert not shelve_cache.empty


def test_set_doesnt_produce_duplicates(tmp_path):
    key = "foo"
    value = "bar"
    shelve_cache = ShelveResultStore(tmp_path / "h-cache")
    shelve_cache.open()
    shelve_cache.cache[key] = value

    shelve_cache.set(key=key, value=value)
    assert shelve_cache.size == 1


def test_get(tmp_path):
    key = "foo"
    value = "bar"
    shelve_cache = ShelveResultStore(tmp_path / "h-cache")
    shelve_cache.open()
    shelve_cache.cache[key] = value

    retrieved_value = shelve_cache.get(key)

    assert retrieved_value
    assert value == retrieved_value
    assert shelve_cache.hits == 1


def test_get_exception(tmp_path):
    shelve_cache = ShelveResultStore(tmp_path / "h-cache")
    shelve_cache.open()

    with pytest.raises(ResultRetrievalError):
        shelve_cache.get("foo")


def test_delete(tmp_path):
    key = "foo"
    shelve_cache = ShelveResultStore(tmp_path / "h-cache")
    shelve_cache.open()
    shelve_cache.cache[key] = "bar"

    shelve_cache.delete(key)

    assert shelve_cache.empty


@pytest.mark.parametrize(
    "format,value",
    [
        ("json", {"key1": "value1", "key2": 2}),
        ("pickle", {"key1": "value1", "key2": 2}),
        ("parquet", pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})),
    ],
)
def test_save_and_load_json(format, value, tmp_path):
    path = tmp_path.with_name("foo").with_suffix(f".{format}")
    saver_kwargs = dict(cache=format)

    loader = cache_to_file(value=value, path=str(path), saver_kwargs=saver_kwargs)
    assert path.exists()

    loaded_value, _ = loader.load_data(None)
    assert hash_value(value) == hash_value(loaded_value)
