import base64
import dataclasses
import functools
import hashlib
import logging
from collections.abc import Mapping, Sequence, Set
from typing import Union

from hamilton.experimental import h_databackends

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class Fingerprint:
    code: Union[str, None]
    data: Union[str, None]


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
