import abc
import json
import pathlib
import shelve
import sqlite3
from typing import Any, Dict, Optional, Tuple, Type, Union

from hamilton.caching.fingerprinting import Fingerprint
from hamilton.io.data_adapters import DataLoader, DataSaver
from hamilton.registry import LOADER_REGISTRY, SAVER_REGISTRY


class MaterializationError(Exception):
    pass


class ResultRetrievalError(Exception):
    pass


class MetadataStoreIndexingError(Exception):
    """Internal error that should be caught during testing."""


# TODO refactor hamilton.io.materialization
def search_registry(name: str, type_: type) -> Tuple[Type[DataSaver], Type[DataLoader]]:
    """Find pair of DataSaver and DataLoader registered with `name` and supporting `type_`"""
    if name not in SAVER_REGISTRY or name not in LOADER_REGISTRY:
        raise KeyError(f"{name} isn't associated to both a DataLoader and a DataSaver")

    try:
        saver_cls = next(
            saver_cls
            for saver_cls in SAVER_REGISTRY[name]
            if any(
                issubclass(type_, applicable_type)
                for applicable_type in saver_cls.applicable_types()
            )
        )
    except StopIteration as e:
        raise KeyError(f"{name} doesn't have any DataSaver supporting type {type_}") from e

    try:
        loader_cls = next(
            loader_cls
            for loader_cls in LOADER_REGISTRY[name]
            if any(
                issubclass(type_, applicable_type)
                for applicable_type in loader_cls.applicable_types()
            )
        )
    except StopIteration as e:
        raise KeyError(f"{name} doesn't have any DataLoader supporting type {type_}") from e

    return saver_cls, loader_cls


def cache_to_file(value: Any, path: str, saver_kwargs: Dict[str, Any]) -> DataLoader:
    # TODO more flexible support where `format != file extension` (e.g., `cache=file`)
    format = saver_kwargs.pop("cache")
    saver_cls, loader_cls = search_registry(name=format, type_=type(value))

    # TODO this will break for materializer with a `file_path` kwarg
    saver = saver_cls(path=path)
    loader = loader_cls(path=path)

    saver.save_data(value)
    return loader


# TODO since we separate the repository from the cache, we need to make sure they're updated together
class BaseStore(abc.ABC):
    @property
    @abc.abstractmethod
    def path(self) -> str:
        """Path where the cache and files are stored"""

    @property
    @abc.abstractmethod
    def empty(self) -> bool:
        """Flag if the cache is empty"""

    @property
    @abc.abstractmethod
    def size(self) -> int:
        """Number of entries in cache"""

    @abc.abstractmethod
    def set(self, **future_kwargs) -> None:
        """Add to the cache `key: value` or `key: materialization_instructions`"""

    @abc.abstractmethod
    def get(self, **future_kwargs) -> Any:
        """Get the value from cache, potentially loading materialized data"""

    @abc.abstractmethod
    def delete(self, **future_kwargs) -> None:
        """Removes an entry from the cache"""


class ShelveResultStore(BaseStore):
    def __init__(self, path: Union[str, pathlib.Path] = "hamilton_cache"):
        self._directory = pathlib.Path(path).resolve()
        self._path = self._directory.joinpath("result_store")
        self.is_open = False
        self.hits = 0

        self._directory.mkdir(parents=True, exist_ok=True)

    @property
    def path(self) -> str:
        return str(self._path)

    @property
    def empty(self):
        self.open()
        return self.size == 0

    @property
    def size(self):
        self.open()
        return len(self.cache)

    def open(self) -> None:
        if not self.is_open:
            self.cache = shelve.open(self.path, "c")
            self.is_open = True

    def close(self) -> None:
        self.cache.close()
        self.is_open = False

    # TODO Move some of this logic to the base class
    def set(
        self,
        key: str,
        value: Any,
        saver_kwargs: Optional[dict] = None,
    ) -> None:
        self.open()
        # skip existing keys already stored
        if self.cache.get(key):
            return

        try:
            # has @tag(cache=...)
            if saver_kwargs:
                file_path = str(
                    self._directory.joinpath(key).with_suffix("." + saver_kwargs["cache"])
                )
                loader = cache_to_file(value=value, path=file_path, saver_kwargs=saver_kwargs)
                # TODO pickle a dict rather than the loader object {path: ..., loader_cls: "JSONDataLoader", **kwargs}
                self.cache[key] = loader

            # no @tag(cache=...)
            else:
                self.cache[key] = value
        except BaseException as e:
            raise MaterializationError from e

    def get(self, key: str) -> Any:
        self.open()

        try:
            retrieved_obj = self.cache[key]
            # has @tag(cache=...)
            if isinstance(retrieved_obj, DataLoader):
                result, _ = retrieved_obj.load_data(None)

            # no @tag(cache=...)
            else:
                result = retrieved_obj
        except BaseException as e:
            raise ResultRetrievalError from e

        self.hits += 1
        return result

    def delete(self, key: str) -> None:
        del self.cache[key]


class SQLiteMetadataStore(BaseStore):
    def __init__(
        self,
        path: Union[str, pathlib.Path] = "hamilton_cache",
        connection_kwargs: Optional[dict] = None,
    ):
        self._directory = pathlib.Path(path).resolve()
        self._directory.mkdir(parents=True, exist_ok=True)
        self._path = self._directory.joinpath("metadata_store").with_suffix(".db")

        self.connection = sqlite3.connect(
            str(self._path), **connection_kwargs if connection_kwargs else {}
        )

        self.hits = 0
        self.misses = 0

    @property
    def path(self) -> str:
        """Path to the SQLite database supporting the SQLiteMetadataStore"""
        return str(self._path)

    @property
    def empty(self) -> bool:
        """Boolean if the cache_metadata table is empty"""
        return self.size == 0

    @property
    def size(self) -> int:
        """Number of entries in cache_metadata"""
        cur = self.connection.cursor()
        cur.execute("SELECT COUNT(context_key) FROM cache_metadata")
        return cur.fetchone()[0]

    def __del__(self):
        """Close the SQLite connection when the object is deleted"""
        self.connection.close()

    def _create_tables_if_not_exists(self):
        """Create the tables necessary for the cache:
        history: queue of executed node; allows to query "latest" execution of a node
        cache_metadata: information to determine if a node needs to be computed or not
        nodes: node metadata from HamiltonNode objects; could allow node reconstruction
        """
        cur = self.connection.cursor()

        cur.execute(
            """\
            CREATE TABLE IF NOT EXISTS nodes (
                code_version TEXT PRIMARY KEY,
                node_json TEXT
            );
            """
        )
        # TODO add "status" with the cache procedure result
        cur.execute(
            """\
            CREATE TABLE IF NOT EXISTS history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                context_key TEXT,
                run_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (context_key) REFERENCES cache_metadata(context_key)
            );
            """
        )
        cur.execute(
            """\
            CREATE TABLE IF NOT EXISTS cache_metadata (
                context_key TEXT PRIMARY KEY,
                node_name TEXT NOT NULL,
                code_version TEXT NOT NULL,
                data_version TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (code_version) REFERENCES nodes(code_version),
                FOREIGN KEY (context_key) REFERENCES history(context_key)
            );
            """
        )
        self.connection.commit()

    def initialize(self):
        """Initialize needs to be called the first time you use the SQLiteMetadataStore.

        The paremeter `reset` allows to delete all existing tables and recreate them.
        """
        self._create_tables_if_not_exists()

    def reset(self):
        """Delete all existing tables from the database"""
        cur = self.connection.cursor()

        for table_name in ["history", "nodes", "cache_metadata"]:
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")

        self.connection.commit()
        self.hits = 0
        self.misses = 0

    # TODO add metadata in `history` (e.g., get/set operations)
    def add_to_history(self, cur, context_key: str, run_id: str) -> None:
        cur.execute(
            "INSERT INTO history (context_key, run_id) VALUES (?, ?)", (context_key, run_id)
        )

    def set(self, to: Fingerprint, context_key: str, h_node: dict, run_id: str) -> None:
        cur = self.connection.cursor()

        self.add_to_history(cur, context_key=context_key, run_id=run_id)
        cur.execute(
            "INSERT OR IGNORE INTO nodes (code_version, node_json) VALUES (?, ?)",
            (to.code, json.dumps(h_node)),
        )
        cur.execute(
            """\
            INSERT OR IGNORE INTO cache_metadata (
                context_key, node_name, code_version, data_version
            ) VALUES (?, ?, ?, ?)
            """,
            (context_key, to.node_name, to.code, to.data),
        )

        self.connection.commit()

    def get(self, to_code: str, context_key: str) -> Optional[Fingerprint]:
        cur = self.connection.cursor()
        cur.execute(
            """\
            SELECT node_name, data_version
            FROM cache_metadata
            WHERE context_key = ?
            """,
            (context_key,),
        )
        result = cur.fetchone()

        if result is None:
            fingerprint = None
            self.misses += 1
        else:
            node_name, data_version = result
            fingerprint = Fingerprint(node_name=node_name, code=to_code, data=data_version)
            self.hits += 1

        return fingerprint

    def delete(self, to: Fingerprint) -> None:
        cur = self.connection.cursor()
        cur.execute("DELETE FROM cache_metadata WHERE code_version = ?", (to.code,))
        self.connection.commit()

    def get_run_metadata(self, run_id: str) -> Dict[str, Fingerprint]:
        cur = self.connection.cursor()
        cur.execute(
            """\
            SELECT
                cache_metadata.node_name,
                cache_metadata.code_version,
                cache_metadata.data_version
            FROM (SELECT * FROM history WHERE history.run_id = ?) AS run_history
            JOIN cache_metadata ON run_history.context_key = cache_metadata.context_key
            """,
            (run_id,),
        )
        results = cur.fetchall()

        if results is None:
            raise IndexError(f"Can't find run `{run_id}` in table `history`.")

        return {
            node_name: Fingerprint(code=code_version, data=data_version, node_name=node_name)
            for node_name, code_version, data_version in results
        }

    @property
    def latest_run_id(self) -> str:
        cur = self.connection.cursor()
        cur.execute("SELECT run_id FROM history ORDER BY id LIMIT 1")
        result = cur.fetchone()

        if result is None:
            raise IndexError("Can't select 'latest' run; table `history` is empty.")

        return result[0]

    def get_latest_run(self) -> Dict[str, Fingerprint]:
        return self.get_run_metadata(run_id=self.latest_run_id)


class InMemoryMetadataStore(BaseStore):
    def __init__(
        self, path: Union[str, pathlib.Path] = "hamilton_cache", state: Optional[dict] = None
    ):
        # {to_code: {(*from_data): to_data}}
        self._directory = pathlib.Path(path)
        self._path = self._directory.joinpath("repository")
        self.state: Dict[str, Dict[str, str]] = state if state else {}
        self.code_to_name = {}
        self.hits = 0
        self.misses = 0

    @property
    def path(self) -> str:
        return str(self._path)

    @property
    def empty(self) -> bool:
        return self.state == {}

    @property
    def size(self) -> int:
        return len(self.state)

    def set(self, to: Fingerprint, context_key: str, **kwargs) -> None:
        self.code_to_name[to.code] = to.node_name
        if self.state.get(to.code) is None:
            self.state[to.code] = {}

        # don't set value if already exists
        retrieved_to_data = self.state[to.code].get(context_key)
        if retrieved_to_data:
            # retrieved data version should match `to.data` otherwise the
            # node operation is potentially non-idempotent
            if retrieved_to_data != to.data:
                # TODO decide if should overwrite cache or throw error
                raise MetadataStoreIndexingError(
                    "Internal Error. If you're seeing this, please open a GitHub issue."
                )
            return

        self.state[to.code][context_key] = to.data

    def get(self, to_code: str, context_key: str, **kwargs) -> Optional[Fingerprint]:
        to_data = self.state.get(to_code, {}).get(context_key)
        if to_data is None:
            self.misses += 1
            return None

        self.hits += 1
        return Fingerprint(node_name=self.code_to_name[to_code], code=to_code, data=to_data)

    def delete(self, to: Fingerprint) -> None:
        executions = self.state.get(to.code)
        if executions is None:
            return

        # need two loops because you can't iterate and delete
        # keys from a dictionary at once
        keys_to_delete = []
        for context_key, to_data in executions.items():
            if to_data == to.data:
                keys_to_delete.append(context_key)

        for context_key in keys_to_delete:
            del self.state[to.code][context_key]

    def reset(self):
        self.state = {}
        self.hits = 0
        self.misses = 0
