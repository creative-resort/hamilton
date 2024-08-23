import pytest

from hamilton.caching.fingerprinting import Fingerprint, create_context_key
from hamilton.caching.store import (
    InMemoryMetadataStore,
    MetadataStoreIndexingError,
    SQLiteMetadataStore,
)


@pytest.fixture
def metadata_store(request, tmp_path):
    metdata_store_cls = request.param
    metadata_store = metdata_store_cls(path=tmp_path)
    try:
        metadata_store.initialize()
    except BaseException:
        pass

    yield metadata_store

    metadata_store.reset()


@pytest.mark.parametrize(
    "metadata_store", [InMemoryMetadataStore, SQLiteMetadataStore], indirect=True
)
def test_initialize_empty(metadata_store):
    assert metadata_store.empty


@pytest.mark.parametrize(
    "metadata_store", [InMemoryMetadataStore, SQLiteMetadataStore], indirect=True
)
def test_not_empty_after_set(metadata_store):
    to = Fingerprint(node_name="bar", code="BAR-1", data="bar-a")
    dependencies = [Fingerprint(node_name="foo", code="FOO-1", data="foo-a")]
    context_key = create_context_key(to_code=to.code, dependencies=dependencies)

    metadata_store.set(to=to, context_key=context_key, h_node={}, run_id="...")

    assert not metadata_store.empty


@pytest.mark.parametrize(
    "metadata_store", [InMemoryMetadataStore, SQLiteMetadataStore], indirect=True
)
def test_set_doesnt_produce_duplicates(metadata_store):
    to = Fingerprint(node_name="bar", code="BAR-1", data="bar-a")
    dependencies = [Fingerprint(node_name="foo", code="FOO-1", data="foo-a")]
    context_key = create_context_key(to_code=to.code, dependencies=dependencies)

    metadata_store.set(to=to, context_key=context_key, h_node={}, run_id="...")
    assert metadata_store.size == 1

    metadata_store.set(to=to, context_key=context_key, h_node={}, run_id="...")
    assert metadata_store.size == 1


# TODO not trivial to implement this check for the SQLiteMetadataStore
@pytest.mark.parametrize(
    "metadata_store",
    [
        InMemoryMetadataStore,
    ],
    indirect=True,
)
def test_set_exception_on_non_idempotent(metadata_store):
    """Assuming idempotence, the same dependencies
    with the same `to.code` should produce the same `to.data
    """
    to = Fingerprint(node_name="bar", code="BAR-1", data="bar-a")
    to_alternative = Fingerprint(node_name="bar", code="BAR-1", data="bar-b")
    dependencies = [Fingerprint(node_name="foo", code="FOO-1", data="foo-a")]
    context_key = create_context_key(to_code=to.code, dependencies=dependencies)

    metadata_store.set(to=to, context_key=context_key, h_node={})
    with pytest.raises(MetadataStoreIndexingError):
        metadata_store.set(to=to_alternative, context_key=context_key, h_node={}, run_id="...")


def test_get_hit_simple_repo():
    node_name = "foo"
    code_version = "FOO-1"
    to = Fingerprint(node_name=node_name, code=code_version, data="foo-a")
    dependencies = [Fingerprint(node_name="bar", code="BAR-1", data="bar-a")]
    context_key = create_context_key(to_code=to.code, dependencies=dependencies)

    initial_state = {to.code: {context_key: to.data}}
    metadata_store = InMemoryMetadataStore(state=initial_state)
    metadata_store.code_to_name[code_version] = node_name

    to_fingerprint = metadata_store.get(to_code=to.code, context_key=context_key)

    assert to_fingerprint
    assert isinstance(to_fingerprint, Fingerprint)
    assert to_fingerprint.data == to.data
    assert metadata_store.hits == 1
    assert metadata_store.misses == 0


@pytest.mark.parametrize(
    "metadata_store", [InMemoryMetadataStore, SQLiteMetadataStore], indirect=True
)
def test_get_miss(metadata_store):
    to = Fingerprint(node_name="foo", code="FOO-1", data="foo-a")
    dependencies = [Fingerprint(node_name="bar", code="BAR-1", data="bar-a")]
    context_key = create_context_key(to_code=to.code, dependencies=dependencies)

    to_fingerprint = metadata_store.get(to_code=to.code, context_key=context_key)

    assert to_fingerprint is None
    assert metadata_store.hits == 0
    assert metadata_store.misses == 1


@pytest.mark.parametrize(
    "metadata_store", [InMemoryMetadataStore, SQLiteMetadataStore], indirect=True
)
def test_set_get_without_dependencies(metadata_store):
    to = Fingerprint(node_name="foo", code="FOO-1", data="foo-a")
    dependencies = []
    context_key = create_context_key(to_code=to.code, dependencies=dependencies)

    metadata_store.set(to=to, context_key=context_key, h_node={}, run_id="...")
    to_fingerprint = metadata_store.get(to_code=to.code, context_key=context_key)

    assert to_fingerprint
    assert isinstance(to_fingerprint, Fingerprint)
    assert to_fingerprint.data == to.data
