#    @Author: Justin C Presley
#    @Author-Email: justincpresley@gmail.com
#    @Project: NDN Storage
#    @Source-Code: https://github.com/justincpresley/ndn-python-storage
#    @Pip-Library: https://pypi.org/project/ndn-storage

# API classes
from .disk import DiskStorage
from .leveldb import LevelDBStoage
from .memory import MemoryStorage
from .monogodb import MonogoDBStorage
from .sqlite import SqliteStorage
from .storage import Storage