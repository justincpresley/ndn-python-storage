#    @Author: Justin C Presley
#    @Author-Email: justincpresley@gmail.com
#    @Project: NDN Storage
#    @Source-Code: https://github.com/justincpresley/ndn-python-storage
#    @Pip-Library: https://pypi.org/project/ndn-storage

# Change Notice! This file oringinates from another repository.
# Please look at CHANGES.rst for more details.

# Basic Libraries
import asyncio as aio
from contextlib import suppress
from typing import List, Optional
from abc import abstractmethod
# NDN Imports
from ndn.encoding.tlv_var import parse_tl_num
from ndn.encoding import Name, parse_data, NonStrictName
from ndn.name_tree import NameTrie
# Custom Imports
from .storage import Storage

class DiskStorage(Storage):
    class UninitializedError(Exception):
        pass # raised when DiskStorage is used without initializing the storage.
    class ReinitalizedError(Exception):
        pass # raised when trying to initialize and already initialized DiskStorage.

    def __init__(self, write_period:int=10, initialize:bool=True) -> None:
        super().__init__()
        self.write_period:int = write_period
        self.initialized:bool = initialize
        if initialize == True:
            self._initialize_storage()

    def __del__(self) -> None:
        if self.write_period > 0 and self.initialized:
            self.write_back_task.cancel()

    def _start_writing(self) -> None:
        if self.write_period > 0:
            self.write_back_task = aio.create_task(self._periodic_write_back())

    @abstractmethod
    def _initialize_storage(self) -> None:
        return
    @abstractmethod
    def _put(self, key:bytes, data:bytes, expire_time_ms:int=None) -> None:
        return
    @abstractmethod
    def _put_batch(self, keys:List[bytes], values:List[bytes], expire_time_mss:List[Optional[int]]) -> None:
        return
    @abstractmethod
    def _get(self, key:bytes, can_be_prefix:bool=False, must_be_fresh:bool=False) -> bytes:
        return
    @abstractmethod
    def _remove(self, key:bytes) -> bool:
        return

    @staticmethod
    def _get_name_bytes_wo_tl(name:NonStrictName) -> bytes:
        # remove name's TL as key to support efficient prefix search
        name:bytes = Name.to_bytes(name)
        offset:int = 0
        offset += parse_tl_num(name, offset)[1]
        offset += parse_tl_num(name, offset)[1]
        return name[offset:]

    async def _periodic_write_back(self) -> None:
        with suppress(aio.CancelledError):
            while 1:
                self._write_back()
                await aio.sleep(self.write_period)

    def _write_back(self) -> None:
        keys = []
        values = []
        expire_time_mss = []
        for name, (data, expire_time_ms) in self.cache.iteritems(prefix=[], shallow=True):
            keys.append(self._get_name_bytes_wo_tl(name))
            values.append(data)
            expire_time_mss.append(expire_time_ms)
        if len(keys) > 0:
            self._put_batch(keys, values, expire_time_mss)
        self.cache = NameTrie()

    def initialize(self) -> None:
        if self.initialized == True:
            raise self.ReinitalizedError("The storage was initialized more than once.")
        self._initialize_storage()

    def put_packet(self, name:NonStrictName, data:bytes) -> None:
        if self.initialized != True:
            raise self.UninitializedError("The storage is not initialized.")

        _, meta_info, _, _ = parse_data(data)
        expire_time_ms:int = self._time_ms()
        if meta_info.freshness_period:
            expire_time_ms += meta_info.freshness_period

        name = Name.normalize(name)
        if self.write_period > 0:
            self.cache[name] = (data, expire_time_ms)
        else:
            self._put(self._get_name_bytes_wo_tl(name), data, expire_time_ms)

    def get_packet(self, name:NonStrictName, can_be_prefix:bool=False, must_be_fresh:bool=False) -> Optional[bytes]:
        if self.initialized != True:
            raise self.UninitializedError("The storage is not initialized.")

        name = Name.normalize(name)
        # memory lookup
        try:
            if self.write_period > 0:
                if not can_be_prefix:
                    data, expire_time_ms = self.cache[name]
                    if not must_be_fresh or expire_time_ms > self._time_ms():
                        return data
                else:
                    it = self.cache.itervalues(prefix=name, shallow=True)
                    while True:
                        data, expire_time_ms = next(it)
                        if not must_be_fresh or expire_time_ms > self._time_ms():
                            return data
            else:
                raise KeyError
        # storage lookup
        except (KeyError, StopIteration):
            key = self._get_name_bytes_wo_tl(name)
            return self._get(key, can_be_prefix, must_be_fresh)

    def remove_packet(self, name:NonStrictName) -> bool:
        if self.initialized != True:
            raise self.UninitializedError("The storage is not initialized.")

        removed = False
        name = Name.normalize(name)
        try:
            if self.write_period > 0:
                del self.cache[name]
                removed = True
            else:
                raise KeyError
        except KeyError:
            pass
        if self._remove(self._get_name_bytes_wo_tl(name)):
            removed = True
        return removed