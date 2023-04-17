"""
Imports from this file follows the contents in `oss2``
"""

# flake8: noqa
from oss2 import AnonymousAuth, Auth, StsAuth

from .api import AioBucket, AioService
from .http import AioSession
from .iterators import AioBucketIterator, AioObjectIterator
from .resumable import resumable_download, resumable_upload

__all__ = [
    "AioBucket",
    "AioService",
    "AioSession",
    "AioBucketIterator",
    "AioObjectIterator",
    "resumable_upload",
    "resumable_download",
]
