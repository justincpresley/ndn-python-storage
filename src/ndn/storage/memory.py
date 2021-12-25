#    @Author: Justin C Presley
#    @Author-Email: justincpresley@gmail.com
#    @Project: NDN Storage
#    @Source-Code: https://github.com/justincpresley/ndn-python-storage
#    @Pip-Library: https://pypi.org/project/ndn-storage

# Basic Libraries
from typing import Optional
# NDN Imports
from ndn.encoding import Name, parse_data, NonStrictName
# Custom Imports
from .storage import Storage

class MemoryStorage(Storage):
    def __init__(self) -> None:
        super().__init__()
    def put_packet(self, name:NonStrictName, packet:bytes) -> None:
        _, meta_info, _, _ = parse_data(packet)
        expire_time_ms = self._time_ms()
        if meta_info.freshness_period:
            expire_time_ms += meta_info.freshness_period
        name = Name.normalize(name)
        self.cache[name] = (packet, expire_time_ms)
    def get_packet(self, name:NonStrictName, can_be_prefix:bool=False, must_be_fresh:bool=False) -> Optional[bytes]:
        name = Name.normalize(name)
        try:
            if not can_be_prefix:
                data, expire_time_ms = self.cache[name]
                if not must_be_fresh or expire_time_ms > self.time_ms():
                    return data
            else:
                it = self.cache.itervalues(prefix=name, shallow=True)
                while 1:
                    data, expire_time_ms = next(it)
                    if not must_be_fresh or expire_time_ms > self.time_ms():
                        return data
        except (KeyError, StopIteration):
            return None
    def remove_packet(self, name:NonStrictName) -> bool:
        removed = False
        name = Name.normalize(name)
        try:
            del self.cache[name]
            removed = True
        except KeyError:
            pass
        return removed