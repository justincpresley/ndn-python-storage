#    @Author: Justin C Presley
#    @Author-Email: justincpresley@gmail.com
#    @Project: NDN Storage
#    @Source-Code: https://github.com/justincpresley/ndn-python-storage
#    @Pip-Library: https://pypi.org/project/ndn-storage

# Change Notice! This file oringinates from another repository.
# Please look at CHANGES.rst for more details.

import base64
from pymongo import MongoClient, ReplaceOne
from .disk import DiskStorage
from typing import List, Optional

class MongoDBStorage(DiskStorage):
    def __init__(self, db:str, collection:str, write_period:int=10) -> None:
        super().__init__(write_period)
        self._db = db
        self._collection = collection
        self._uri = 'mongodb://127.0.0.1:27017/'
        self.client = MongoClient(self._uri)
        self.c_db = self.client[self._db]
        self.c_collection = self.c_db[self._collection]

        client = MongoClient(self._uri)
        client.server_info()    # will throw an exception if not connected
        c_db = client[self._db]
        c_collection = c_db[self._collection]
        c_collection.create_index('key', unique=True)

    def _put(self, key:bytes, value:bytes, expire_time_ms:Optional[int]=None) -> None:
        key = base64.b16encode(key).decode()
        replace = {
            'key': key,
            'value': value,
            'expire_time_ms': expire_time_ms,
        }
        self.c_collection.replace_one({'key': key}, replace, upsert=True)

    def _put_batch(self, keys:List[bytes], values:List[bytes], expire_time_mss:List[Optional[int]]) -> None:
        keys = [base64.b16encode(key).decode() for key in keys]
        replaces = []
        for key, value, expire_time_ms in zip(keys, values, expire_time_mss):
            replaces.append(ReplaceOne({'key': key}, {
                'key': key,
                'value': value,
                'expire_time_ms': expire_time_ms,
            }, upsert=True))
        self.c_collection.bulk_write(replaces, ordered=False)

    def _get(self, key:bytes, can_be_prefix:bool=False, must_be_fresh:bool=False) -> Optional[bytes]:
        key = base64.b16encode(key).decode()
        query = dict()
        if not can_be_prefix:
            query.update({'key': key})
        else:
            query.update({'key': {'$regex': '^' + key}})
        if must_be_fresh:
            query.update({'expire_time_ms': {'$gt': self.time_ms()}})
        ret = self.c_collection.find_one(query)
        if ret:
            return ret['value']
        return None

    def _remove(self, key:bytes) -> bool:
        key = base64.b16encode(key).decode()
        return self.c_collection.delete_one({"key": key}).deleted_count > 0