ndn-python-storage
==================

This python library provides **different storage APIs** for **Named Data Networking (NDN) applications**. At first,
it was thought that only ndn-python-repo_ would need such API; but with the ever-growing NDN applications such
as variations of NDN repositories (repo, nr-archway, hydra, etc), NDN sync protocols (svs, psync, etc), and more; it is imperative that
an API is provided.

The Named Data Networking Storage Library is implemented using and simply adds to python-ndn_, nicknamed
``ndn-python-storage`` obtained by the pip library ndn-storage_.

The implementation is on our github-repo_.

There is no documentation at this moment. However, as more storage solutions are added, a documentation will be formed.

To install, please use the pip library ndn-storage_. To import, use ndn.storage like you would with ndn.encoding.

.. _ndn-python-repo: https://github.com/UCLA-IRL/ndn-python-repo
.. _python-ndn: https://github.com/named-data/python-ndn
.. _github-repo: https://github.com/justincpresley/ndn-python-storage
.. _ndn-storage: https://pypi.org/project/ndn-storage