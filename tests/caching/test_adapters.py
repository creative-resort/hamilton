import pytest

from hamilton.caching.adapters import SmartCacheAdapter
from hamilton.caching.fingerprinting import Fingerprint, create_context_key, hash_value
from hamilton.graph_types import HamiltonNode


@pytest.fixture
def cache_adapter(tmp_path):
    adapter = SmartCacheAdapter(path=tmp_path)
    adapter.code_versions = {"foo": "0", "bar": "0"}
    adapter.metadata_store.initialize()
    adapter.graph = {
        "foo": HamiltonNode(
            name="foo",
            type=str,
            tags={},
            is_external_input=False,
            originating_functions=(),
            documentation="",
            required_dependencies=set(),
            optional_dependencies=set(),
        )
    }
    adapter.run_id = "my-run-id"

    yield adapter

    adapter.metadata_store.reset()


def test_after_node_execution_set_and_get(cache_adapter):
    """Adapter should write to cache and repository if it needs to
    compute the value
    """
    node_name = "foo"
    result = 123
    node_kwargs = {}
    code_version = cache_adapter.code_versions[node_name]
    data_version = hash_value(result)
    fingerprint = Fingerprint(node_name=node_name, code=code_version, data=data_version)
    context_key = create_context_key(to_code=code_version, dependencies=[])
    result_store = cache_adapter.result_store
    metadata_store = cache_adapter.metadata_store

    assert cache_adapter.fingerprints.get(node_name) is None
    assert result_store.size == 0
    assert metadata_store.size == 0

    cache_adapter.run_after_node_execution(
        node_name=node_name, node_kwargs=node_kwargs, result=result
    )

    assert cache_adapter.fingerprints.get(node_name) == fingerprint
    assert result_store.size == 1
    assert result_store.get(data_version) == result
    assert metadata_store.size == 1
    assert metadata_store.get(to_code=code_version, context_key=context_key) == fingerprint


def test_after_node_execution_dont_set_cache_for_existing_fingerprint(cache_adapter):
    """Adapter shouldn't write to cache if it finds the fingerprint in the repository"""
    node_name = "foo"
    result = 123
    node_kwargs = {}
    code_version = cache_adapter.code_versions[node_name]
    data_version = hash_value(result)
    fingerprint = Fingerprint(node_name=node_name, code=code_version, data=data_version)
    context_key = create_context_key(to_code=code_version, dependencies=[])
    result_store = cache_adapter.result_store
    metadata_store = cache_adapter.metadata_store

    assert cache_adapter.fingerprints.get(node_name) is None
    assert result_store.size == 0
    assert metadata_store.size == 0

    metadata_store.set(to=fingerprint, context_key=context_key, h_node={}, run_id="...")
    cache_adapter.run_after_node_execution(
        node_name=node_name, node_kwargs=node_kwargs, result=result
    )

    assert result_store.size == 0
    assert metadata_store.size == 1
    assert metadata_store.get(to_code=code_version, context_key=context_key) == fingerprint


def test_run_to_execute_check_repository_if_previously_unseen(cache_adapter):
    """Adapter needs to check the repository if fingerprint isn't available"""
    metadata_store = cache_adapter.metadata_store
    result_store = cache_adapter.result_store

    result = cache_adapter.run_to_execute_node(
        node_name="foo",
        node_callable=lambda **kwargs: object,
        node_kwargs={},
    )

    assert metadata_store.hits == 0
    assert metadata_store.misses == 1
    assert result_store.hits == 0
    assert result is object


def test_run_to_execute_dont_check_repo_if_previously_seen(cache_adapter):
    """Adapter shouldn't check the repository if fingerprint is available"""
    node_name = "foo"
    cached_result = 123
    code_version = cache_adapter.code_versions[node_name]
    data_version = hash_value(cached_result)
    fingerprint = Fingerprint(node_name=node_name, code=code_version, data=data_version)
    repo = cache_adapter.metadata_store
    result_store = cache_adapter.result_store

    result_store.set(key=data_version, value=cached_result)
    cache_adapter.fingerprints[node_name] = fingerprint

    result = cache_adapter.run_to_execute_node(
        node_name=node_name,
        node_callable=lambda **kwargs: None,
        node_kwargs={},
    )

    # didn't check the repository, only the cache
    assert repo.hits == 0
    assert repo.misses == 0
    assert result_store.hits == 1
    assert result == cached_result


def test_run_to_execute_repo_cache_desync(cache_adapter):
    """The adapter determines the value is in cache,
    but there's an error loading the value from cache.

    The adapter should delete metadata store keys to force recompute and
    writing the result to cache

    NOTE that this will only log and error and not raise any Exception.
    This is because adapters cannot currently raise Exception that stop
    the main execution.
    """
    node_name = "foo"
    code_version = cache_adapter.code_versions[node_name]
    fingerprint = Fingerprint(node_name=node_name, code=code_version, data=hash_value("abcd"))
    context_key = create_context_key(to_code=code_version, dependencies=[])
    metadata_store = cache_adapter.metadata_store
    result_store = cache_adapter.result_store

    metadata_store.set(to=fingerprint, context_key=context_key, h_node={}, run_id="...")
    result = cache_adapter.run_to_execute_node(
        node_name=node_name,
        node_callable=lambda **kwargs: object,
        node_kwargs={},
    )

    # found the fingerprint in repo, but the value wasn't in cache
    assert metadata_store.hits == 1
    assert metadata_store.misses == 0
    assert result_store.hits == 0
    assert result is object
