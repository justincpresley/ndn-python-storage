#    @Author: Justin C Presley
#    @Author-Email: justincpresley@gmail.com
#    @Project: NDN Storage
#    @Source-Code: https://github.com/justincpresley/ndn-python-storage
#    @Pip-Library: https://pypi.org/project/ndn-storage

# Change Notice! This file oringinates from another repository.
# Please look at CHANGES.rst for more details.

# Basic Libraries
import os
import sqlite3
from typing import List, Optional
# Custom Imports
from .disk import DiskStorage

class SqliteStorage(DiskStorage):
    def __init__(self, db_path:str, write_period:int=10, initialize:bool=True):
        self.db_path = os.path.expanduser(db_path)
        if len(os.path.dirname(self.db_path)) > 0 and not os.path.exists(os.path.dirname(self.db_path)):
            try:
                os.makedirs(os.path.dirname(self.db_path))
            except PermissionError:
                raise PermissionError(f'Could not create database directory: {self.db_path}') from None
        super().__init__(write_period, initialize)

    def _initialize_storage(self) -> None:
        self.conn = sqlite3.connect(os.path.expanduser(self.db_path), check_same_thread=False)
        c = self.conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS data (
                key BLOB PRIMARY KEY,
                value BLOB,
                expire_time_ms INTEGER
            )
        """)
        self.conn.commit()
        self._start_writing()

    def _put(self, key:bytes, value:bytes, expire_time_ms:Optional[int]=None):
        c = self.conn.cursor()
        c.execute('INSERT OR REPLACE INTO data (key, value, expire_time_ms) VALUES (?, ?, ?)',
            (key, value, expire_time_ms))
        self.conn.commit()

    def _put_batch(self, keys:List[bytes], values:List[bytes], expire_time_mss:List[Optional[int]]):
        c = self.conn.cursor()
        c.executemany('INSERT OR REPLACE INTO data (key, value, expire_time_ms) VALUES (?, ?, ?)',
            zip(keys, values, expire_time_mss))
        self.conn.commit()

    def _get(self, key:bytes, can_be_prefix:bool=False, must_be_fresh:bool=False) -> Optional[bytes]:
        c = self.conn.cursor()
        query = 'SELECT value FROM data WHERE '
        if must_be_fresh:
            query += f'(expire_time_ms > {time_ms()}) AND '
        if can_be_prefix:
            query += 'hex(key) LIKE ?'
            c.execute(query, (key.hex() + '%', ))
        else:
            query += 'key = ?'
            c.execute(query, (key, ))
        ret = c.fetchone()
        return ret[0] if ret else None

    def _remove(self, key:bytes) -> bool:
        c = self.conn.cursor()
        n_removed = c.execute('DELETE FROM data WHERE key = ?', (key, )).rowcount
        self.conn.commit()
        return n_removed > 0