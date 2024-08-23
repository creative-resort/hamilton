import pytest

from hamilton import driver
from hamilton.caching.adapters import SmartCacheAdapter
from hamilton.caching.store import ShelveResultStore, SQLiteMetadataStore

from tests.caching import cases


@pytest.fixture
def h_driver(request, tmp_path):
    module = request.param
    metadata_store = SQLiteMetadataStore(path=tmp_path)
    cache = ShelveResultStore(path=tmp_path)
    adapter = SmartCacheAdapter(metadata_store=metadata_store, result_store=cache)

    return driver.Builder().with_modules(module).with_adapters(adapter).build()


@pytest.mark.parametrize("h_driver", cases.ALL_MODULES, indirect=True)
def test_single_run_is_successful(h_driver):
    """The Driver with the adapter can be executed at least once
    and return all variables.

    Important to disambiguate more complex bugs
    """
    final_vars = []
    inputs = {}

    for idx, node in enumerate(h_driver.list_available_variables()):
        if node.name in h_driver.graph.config:
            continue

        if node.is_external_input:
            inputs[node.name] = idx

        final_vars.append(node.name)

    h_driver.execute(final_vars, inputs=inputs)


@pytest.mark.parametrize("h_driver", cases.ALL_MODULES, indirect=True)
def test_run_twice_is_successful(h_driver):
    """The Driver with the adapter can be executed at least once
    and return all variables.
    """
    # start by ensuring the Driver has the cache adapter
    assert isinstance(h_driver.adapter.adapters[0], SmartCacheAdapter)
    cache_adapter = h_driver.adapter.adapters[0]

    final_vars = []
    inputs = {}
    for idx, node in enumerate(h_driver.list_available_variables()):
        if node.name in h_driver.graph.config:
            continue

        # generate mock inputs; we currently assume all nodes return integers
        if node.is_external_input:
            inputs[node.name] = idx

        final_vars.append(node.name)

    # inputs are not stored in cache
    expected_number_of_results = len(final_vars) - len(inputs)

    # first execution
    results = h_driver.execute(final_vars, inputs=inputs)
    cache_size_after_first_run = cache_adapter.result_store.size

    assert len(results) == len(final_vars)
    assert not cache_adapter.result_store.empty
    assert cache_adapter.result_store.hits == 0
    assert cache_size_after_first_run == expected_number_of_results

    # second execution
    results2 = h_driver.execute(final_vars, inputs=inputs)
    cache_size_after_second_run = cache_adapter.result_store.size

    assert len(results2) == len(final_vars)
    assert cache_adapter.result_store.hits == expected_number_of_results
    assert cache_size_after_first_run == cache_size_after_second_run == expected_number_of_results

    # compare executions
    assert frozenset(results.keys()) == frozenset(results2.keys())
    assert all(results[k] == results2[k] for k in results.keys())
