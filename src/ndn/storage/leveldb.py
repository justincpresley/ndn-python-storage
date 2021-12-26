#    @Author: Justin C Presley
#    @Author-Email: justincpresley@gmail.com
#    @Project: NDN Storage
#    @Source-Code: https://github.com/justincpresley/ndn-python-storage
#    @Pip-Library: https://pypi.org/project/ndn-storage

# Change Notice! This file oringinates from another repository.
# Please look at CHANGES.rst for more details.

# Basic Libraries
import os
import pickle
import plyvel
from typing import List, Optional
# Custom Imports
from .disk import DiskStorage

class LevelDBStorage(DiskStorage):
    def __init__(self, directory:str, write_period:int=10, initialize:bool=True) -> None:
        self.db_dir = os.path.expanduser(directory)
        if not os.path.exists(self.db_dir):
            try:
                os.makedirs(self.db_dir)
            except PermissionError:
                raise PermissionError(f'Could not create database directory: {self.db_dir}') from None
        super().__init__(write_period, initialize)

    def _initialize_storage(self) -> None:
        self.db = plyvel.DB(self.db_dir, create_if_missing=True)
        self._start_writing()

    def _put(self, key:bytes, value:bytes, expire_time_ms:Optional[int]=None) -> None:
        self.db.put(key, pickle.dumps((value, expire_time_ms)))

    def _put_batch(self, keys:List[bytes], values:List[bytes], expire_time_mss:List[Optional[int]]) -> None:
        with self.db.write_batch() as b:
            for key, value, expire_time_ms in zip(keys, values, expire_time_mss):
                b.put(key, pickle.dumps((value, expire_time_ms)))

    def _get(self, key:bytes, can_be_prefix:bool=False, must_be_fresh:bool=False) -> bytes:
        if not can_be_prefix:
            record = self.db.get(key)
            if record == None:
                return None
            value, expire_time_ms = pickle.loads(record)
            if not must_be_fresh or expire_time_ms != None and expire_time_ms > int(time.time() * 1000):
                return value
            return None
        for _, v_e in self.db.iterator(prefix=key):
            value, expire_time_ms = pickle.loads(v_e)
            if not must_be_fresh or expire_time_ms != None and expire_time_ms > self.time_ms():
                return value
        return None

    def _remove(self, key:bytes) -> bool:
        if self._get(key) != None:
            self.db.delete(key)
            return True
        return False