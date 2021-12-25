#    @Author: Justin C Presley
#    @Author-Email: justincpresley@gmail.com
#    @Project: NDN Storage
#    @Source-Code: https://github.com/justincpresley/ndn-python-storage
#    @Pip-Library: https://pypi.org/project/ndn-storage

# Change Notice! This file oringinates from another repository.
# Please look at CHANGES.rst for more details.

import asyncio as aio
from contextlib import suppress
from ndn.encoding.tlv_var import parse_tl_num
from ndn.encoding import Name, parse_data, NonStrictName
from ndn.name_tree import NameTrie
from typing import List, Optional
from abc import ABC, abstractmethod

class DiskStorage(ABC):
    def __init__(self, write_period:int=10) -> None:
        super().__init__()
        self.write_period:int = write_period
        if write_period > 0:
            self.write_back_task = aio.create_task(self._periodic_write_back())
    def __del__(self) -> None:
        if write_period > 0:
            self.write_back_task.cancel()

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

    def put_data_packet(self, name:NonStrictName, data:bytes) -> None:
        _, meta_info, _, _ = parse_data(data)
        expire_time_ms:int = self._time_ms()
        if meta_info.freshness_period:
            expire_time_ms += meta_info.freshness_period

        name = Name.normalize(name)
        if write_period > 0:
            self.cache[name] = (data, expire_time_ms)
        else:
            self._put(self._get_name_bytes_wo_tl(name), data, expire_time_ms)

    def get_data_packet(self, name:NonStrictName, can_be_prefix:bool=False, must_be_fresh:bool=False) -> Optional[bytes]:
        name = Name.normalize(name)
        # memory lookup
        try:
            if write_period > 0:
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

    def remove_data_packet(self, name:NonStrictName) -> bool:
        removed = False
        name = Name.normalize(name)
        try:
            if write_period > 0:
                del self.cache[name]
                removed = True
            else:
                raise KeyError
        except KeyError:
            pass
        if self._remove(self._get_name_bytes_wo_tl(name)):
            removed = True
        return removed