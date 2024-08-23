import base64
import dataclasses
import functools
import hashlib
import logging
import zlib
from collections.abc import Mapping, Sequence, Set

from hamilton.experimental import h_databackends

logger = logging.getLogger(__name__)


def _compress_string(string: str) -> str:
    return base64.b64encode(zlib.compress(string.encode(), level=3)).decode()


def _decompress_string(string: str) -> str:
    return zlib.decompress(base64.b64decode(string.encode())).decode()


def _encode_str_dict(d: Mapping) -> str:
    interleaved_tuple = tuple(item for pair in sorted(d.items()) for item in pair)
    return " ".join(interleaved_tuple)


def _decode_str_dict(s: str) -> Mapping:
    interleaved_tuple = tuple(s.split(" "))
    d = {}
    for i in range(0, len(interleaved_tuple), 2):
        d[interleaved_tuple[i]] = interleaved_tuple[i + 1]
    return d


def _encode_dict(hash_map: Mapping[str, str]) -> str:
    """Store input fingerprints as single string using a revertable encoding.

    For example:
        the input: {"node_a": "version_1", "node_b": "version_2"}
        will be encoded as: 'node_a version_1 node_b version_2'
        and then compress to: 'eF7Ly09JjU9UKEstKs7Mz4s3VMgDCSTBBYwA1BsMWw=='

    NOTE. the compressed string is longer than the original string here
    because `version_1` is much shorter than the real version SHA256 hashes.
    """
    interleaved_string = _encode_str_dict(hash_map)
    return _compress_string(interleaved_string)


def _decode_dict(encoded_dict: str) -> Mapping:
    """Convert encoded input fingerprints back to a dictionary of {node_name: version}
    Does the opposite of `_encode_dict()`

    For example:
        the compressed encoded string: 'eF7Ly09JjU9UKEstKs7Mz4s3VMgDCSTBBYwA1BsMWw=='
        is decompressed to: 'node_a version_1 node_b version_2'
        then converted back to a dictionary: {"node_a": "version_1", "node_b": "version_2"}
    """
    interleaved_string = _decompress_string(encoded_dict)
    return _decode_str_dict(interleaved_string)


@dataclasses.dataclass(frozen=True)
class Fingerprint:
    code: str
    data: str
    node_name: str


def create_context_key(to_code: str, dependencies: Sequence[Fingerprint]) -> str:
    if dependencies:
        data_dependencies = _encode_dict({dep.node_name: dep.data for dep in dependencies})
    else:
        # this happens for top-level nodes without inputs, inputs, and overrides
        data_dependencies = "<none>"

    return _encode_dict({to_code: data_dependencies})


def _compact_hash(digest: bytes) -> str:
    """Compact the hash to a string that's safe to pass around.

    NOTE this is particularly relevant for th Hamilton UI and
    passing hashes/fingerprints through web services.
    """
    return base64.urlsafe_b64encode(digest).decode()


@functools.singledispatch
def hash_value(obj, depth=0, *args, **kwargs) -> str:
    """Fingerprinting strategy that computes a hash of the
    full Python object.

    The default case hashes the `__dict__` attribute of the
    object (recursive).
    """
    MAX_DEPTH = 3
    if hasattr(obj, "__dict__") and depth < MAX_DEPTH:
        depth += 1
        return hash_value(obj.__dict__, depth)

    hash_object = hashlib.md5("<unhashable>".encode())
    return _compact_hash(hash_object.digest())


@hash_value.register(str)
@hash_value.register(int)
@hash_value.register(float)
@hash_value.register(bool)
@hash_value.register(bytes)
def hash_primitive(obj, *args, **kwargs) -> str:
    """Convert the primitive to a string and hash it"""
    hash_object = hashlib.md5(str(obj).encode())
    return _compact_hash(hash_object.digest())


@hash_value.register(Sequence)
def hash_sequence(obj, *, sort: bool = False, **kwargs) -> str:
    """Hash each object of the sequence.

    Orders matters for the hash since orders matters in a sequence.
    """
    hash_object = hashlib.sha224()
    for elem in obj:
        hash_object.update(hash_value(elem).encode())

    return _compact_hash(hash_object.digest())


@hash_value.register(Mapping)
def hash_mapping(obj, *, sort: bool = False, **kwargs) -> str:
    """Hash each key then its value.

    The mapping is always sorted first because order shouldn't matter
    in a mapping.

    NOTE this may clash with Python dictionary ordering since >=3.7
    """
    if sort:
        obj = dict(sorted(obj.items()))

    hash_object = hashlib.sha224()
    for key, value in obj.items():
        hash_object.update(hash_value(key).encode())
        hash_object.update(hash_value(value).encode())

    return _compact_hash(hash_object.digest())


@hash_value.register(Set)
def hash_set(obj, *args, **kwargs) -> str:
    """Hash each element of the set, then sort hashes, and
    create a hash of hashes.

    For the same objects in the set, the hashes will be the
    same.
    """
    hashes = [hash_value(elem) for elem in obj]
    sorted_hashes = sorted(hashes)

    hash_object = hashlib.sha224()
    for hash in sorted_hashes:
        hash_object.update(hash.encode())

    return _compact_hash(hash_object.digest())


@hash_value.register(h_databackends.AbstractPandasDataFrame)
def hash_pandas_dataframe(obj, *args, **kwargs) -> str:
    """Convert a pandas dataframe to a dictionary of {index: row_hash}
    then hash it.

    Given the hashing for mappings, the physical ordering or rows doesn't matter.
    For example, if the index is a date, the hash will represent the {date: row_hash},
    and won't preserve how dates were ordered in the DataFrame.
    """
    from pandas.util import hash_pandas_object

    hash_per_row = hash_pandas_object(obj)
    return hash_value(hash_per_row.to_dict())
