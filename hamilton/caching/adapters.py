import json
import logging
import pathlib
from collections.abc import Sequence
from typing import Any, Callable, Dict, Literal, Optional, Union

from hamilton import graph_types
from hamilton.caching import fingerprinting
from hamilton.caching.fingerprinting import Fingerprint
from hamilton.caching.store import (
    BaseStore,
    ResultRetrievalError,
    ShelveResultStore,
    SQLiteMetadataStore,
)
from hamilton.lifecycle import GraphExecutionHook, NodeExecutionHook, NodeExecutionMethod

logger = logging.getLogger(__name__)


class FingerprintingAdapter(GraphExecutionHook, NodeExecutionHook):
    def __init__(self, path: Optional[str] = None, fingerprint: Optional[Callable] = None):
        """Fingerprint node results. This is primarily an interval tool for developing
        and debugging caching features.

        If path is specified, output a {node_name: fingerprint} to ./fingerprints/{run_id}.json
        Strategy allows to pass different callables to fingerprint values. Works well with
        `@functools.single_dispatch`. See `hash_value()` for reference
        """
        self.path = path
        self.fingerprint = fingerprint if fingerprint else fingerprinting.hash_value
        self.run_fingerprints = {}
        self.run_id: str = None

    def run_before_graph_execution(
        self, *, run_id: str, inputs: Dict[str, Any], overrides: Dict[str, Any], **kwargs: Any
    ):
        """Get the fingerprint of inputs and overrides before execution since they
        don't go through `run_to_execute_node()` or `run_after_node_execution()`
        """
        self.run_id = run_id

        if inputs:
            for node_name, value in inputs.items():
                self.run_fingerprints[node_name] = self.fingerprint(value)

        if overrides:
            for node_name, value in overrides.items():
                self.run_fingerprints[node_name] = self.fingerprint(value)

    def run_after_node_execution(self, *, node_name: str, result: Any, **kwargs):
        """Get the fingerprint of the most recent node result"""
        # values passed as inputs or overrides will already have known hashes
        data_hash = self.run_fingerprints.get(node_name)
        if data_hash is None:
            self.run_fingerprints[node_name] = self.fingerprint(result)

    def run_before_node_execution(self, *args, **kwargs):
        """Placeholder required to subclass `NodeExecutionHook`"""

    def run_after_graph_execution(self, *args, **kwargs):
        """If path is specified, output a {node_name: fingerprint} to ./fingerprints/{run_id}.json"""
        if self.path:
            file_path = pathlib.Path(self.path, "fingerprints", f"{self.run_id}.json")
            file_path.parent.mkdir(exist_ok=True)
            file_path.write_text(json.dumps(self.run_fingerprints))


# TODO move this adapter to `hamilton.lifecycle.default`
# TODO change temporary name
# TODO resolve special condition (i.e.,"dont recompute") specified on @tag and adapter
class SmartCacheAdapter(NodeExecutionHook, NodeExecutionMethod, GraphExecutionHook):
    def __init__(
        self,
        path: Union[str, pathlib.Path] = "hamilton_cache",
        metadata_store: Optional[BaseStore] = None,
        result_store: Optional[BaseStore] = None,
        resume_from: Optional[Union[str, Literal["latest"]]] = None,
        dont_store: Optional[Sequence[str]] = None,
        always_recompute: Optional[Sequence[str]] = None,
        constant_fingerprint: Optional[Sequence[str]] = None,
        **kwargs,
    ):
        """
        :param path: path where the cache metadata and results will be stored
        :param metadata_store: BaseStore handling metadata for the cache adapter
        :param result_store: BaseStore caching dataflow execution results
        :param resume_from: Run id or "latest" to load metadata from at execution time.
        :param dont_cache: Node result to fingerprint but not store.
        :param always_recompute: Node to compute and fingerprint, but not read from cache.
        :param constant_fingerprint: Node result is assumed to be constant, i.e., always read from cache
        """
        self.metadata_store = metadata_store if metadata_store else SQLiteMetadataStore(path=path)
        self.result_store = result_store if result_store else ShelveResultStore(path=path)
        self.resume_from: str = resume_from

        # NOTE this special cases are not currently implemented
        self.dont_store = set(dont_store) if dont_store else set()
        self.always_recompute = set(always_recompute) if always_recompute else set()
        self.constant_fingerprint = set(constant_fingerprint) if constant_fingerprint else set()

        self.fingerprints: Dict[str, Fingerprint] = {}
        self.code_versions: Dict[str, Any] = {}
        self.data_savers: Dict[str, Any] = {}
        self.graph: graph_types.HamiltonGraph = None

    def _process_inputs(self, inputs: Optional[dict]) -> None:
        """
        need to hash top-level inputs and store in run `data_versions` to set the
        base case of the recursive `create_input_keys()`
        for top-level inputs, the code version shouldn't be considered in the key
        """
        if not inputs:
            return

        for node_name, value in inputs.items():
            # Create "code version" key that's unique to the input node, but invariant to version
            self.fingerprints[node_name] = Fingerprint(
                node_name=node_name,
                code=f"{node_name}__input",
                data=fingerprinting.hash_value(value),
            )

    def _process_overrides(self, overrides: Optional[dict]) -> None:
        """For overrides, we don't want to store a `(code, inputs_key): fingerprint`
        because the node didn't actually ran.
        We can still store the `fingerprint: value` in case we hit that fingerprint
        on subsequent runs
        """
        if not overrides:
            return

        for node_name, value in overrides.items():
            self.fingerprints[node_name] = Fingerprint(
                node_name=node_name,
                code=self.code_versions[node_name],
                data=fingerprinting.hash_value(value),
            )

    # TODO create a special @cache that contains the right info
    def _parse_node_tags(self, graph: graph_types.HamiltonGraph) -> None:
        for node in graph.nodes:
            if node.tags.get("cache") is None:
                continue

            # TODO parse the `@tag` to collect kwargs for materializers
            self.data_savers[node.name] = node.tags
            if node.tags.get("always_recompute"):
                self.always_recompute.add(node.name)

            if node.tags.get("dont_fingerprint"):
                self.constant_fingerprint.add(node.name)

    # NOTE this feature is a bit tricky. It seems that the history should hold a
    # unique uuid per run (the run_id provided by Hamilton) and a "user specified run_id"
    # for when things are resumed
    def _resolve_run_overrides(self) -> None:
        if self.resume_from is None:
            return

        if self.resume_from == "latest":
            resume_run_id = self.metadata_store.latest_run_id
        else:
            resume_run_id = self.resume_from

        # update the in-memory fingerprints with the fingerprint overrides
        fingerprints_from_run = self.metadata_store.get_run_metadata(run_id=resume_run_id)
        self.fingerprints.update(**fingerprints_from_run)

    def run_before_graph_execution(
        self,
        *,
        run_id: str,
        graph: graph_types.HamiltonGraph,
        inputs: Dict[str, Any],
        overrides: Dict[str, Any],
        **kwargs: Any,
    ):
        """Get code versions for all, and data version for top-level inputs and overrides
        Open the cache
        """
        self.metadata_store.initialize()
        self.run_id = run_id
        self.graph = graph
        # reset the code_versions and fingerprints before each execution
        # code versions aren't expected to change for the lifetime of the Driver though
        self.fingerprints = {}
        self.code_versions = {n.name: n.version for n in graph.nodes}
        self._parse_node_tags(graph)
        self._resolve_run_overrides()

        self._process_inputs(inputs=inputs)
        self._process_overrides(overrides=overrides)

    def run_after_node_execution(
        self, *, node_name: str, node_kwargs: Dict[str, Any], result: Any, **kwargs
    ):
        """Try to read data version from memory or from cache else compute result version.
        Then, store data version in memory (for this run) and data version cache (for next run),
        and store result in result cache (for retrieval in next run)
        """
        code_version = self.code_versions[node_name]
        dependencies = [self.fingerprints[dep_name] for dep_name in node_kwargs.keys()]
        context_key = fingerprinting.create_context_key(
            to_code=code_version, dependencies=dependencies
        )

        # check stored metadata to determine if result exists
        fingerprint = self.metadata_store.get(to_code=code_version, context_key=context_key)

        # if result doesn't exist, version the data and add it to the metadata and result stores
        if fingerprint is None:
            fingerprint = Fingerprint(
                node_name=node_name, code=code_version, data=fingerprinting.hash_value(result)
            )
            self.result_store.set(
                key=fingerprint.data, value=result, saver_kwargs=self.data_savers.get(node_name)
            )
            self.metadata_store.set(
                context_key=context_key,
                to=fingerprint,
                h_node=self.graph[node_name].as_dict(),
                run_id=self.resume_from,
            )

        self.fingerprints[node_name] = fingerprint

    def run_to_execute_node(
        self, *, node_name: str, node_callable: Any, node_kwargs: Dict[str, Any], **kwargs
    ):
        """Create hash key then use cached value if exist"""
        code_version = self.code_versions[node_name]
        dependencies = [self.fingerprints[dep_name] for dep_name in node_kwargs.keys()]
        context_key = fingerprinting.create_context_key(
            to_code=code_version, dependencies=dependencies
        )

        # check in-memory metadata from previous nodes (e.g., node with many children)
        fingerprint = self.fingerprints.get(node_name)

        # check stored metadata to determine if result exists
        if fingerprint is None:
            fingerprint = self.metadata_store.get(to_code=code_version, context_key=context_key)

        # compute the node if the result doesn't exist or specified to always recompute
        if fingerprint is None:
            logger.debug(f"{node_name}: cache miss")
            result = node_callable(**node_kwargs)

        # if the result exists, try to read it from the result store
        else:
            try:
                result = self.result_store.get(key=fingerprint.data)
                logger.debug(f"{node_name}: cache hit")
            except ResultRetrievalError:
                # if there's an error reading from the result store, recompute and
                # delete the metadata to force rewriting the new metadata
                logger.error(f"{node_name}: cache retrieval error")
                self.metadata_store.delete(to=fingerprint)
                result = node_callable(**node_kwargs)

        return result

    def run_before_node_execution(self, *args, **kwargs):
        """Placeholder required to subclass `NodeExecutionHook`"""

    def run_after_graph_execution(self, *args, **kwargs):
        """laceholder required to subclass `GraphExecutionHook`"""
