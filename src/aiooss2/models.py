"""
Module for all input and output classes for the Python SDK API
"""
import copy
import logging
from typing import TYPE_CHECKING

from oss2.exceptions import ClientError
from oss2.headers import (
    DEPRECATED_CLIENT_SIDE_ENCRYPTION_KEY,
    KMS_ALI_WRAP_ALGORITHM,
    OSS_CLIENT_SIDE_ENCRYPTION_KEY,
)
from oss2.models import ContentCryptoMaterial, HeadObjectResult, _hget

from aiooss2.utils import make_adapter

if TYPE_CHECKING:
    from .http import AioResponse


logger = logging.getLogger(__name__)


class AioGetObjectResult(HeadObjectResult):
    """class for the result of api get_object"""

    resp: "AioResponse"

    def __init__(  # pylint: disable=too-many-arguments
        self,
        resp: "AioResponse",
        progress_callback=None,
        crc_enabled=False,
        crypto_provider=None,
        discard=0,
    ):
        super().__init__(resp)
        self.__crypto_provider = crypto_provider
        self.__crc_enabled = crc_enabled

        self.content_range = _hget(resp.headers, "Content-Range")
        if self.content_range:
            byte_range = self._parse_range_str(self.content_range)

        self.stream = make_adapter(
            self.resp,
            progress_callback=progress_callback,
            size=self.content_length,
            enable_crc=crc_enabled,
            discard=discard,
        )

        if self.__crypto_provider:
            self._make_crypto(byte_range, discard)
        else:
            if (
                OSS_CLIENT_SIDE_ENCRYPTION_KEY in resp.headers
                or DEPRECATED_CLIENT_SIDE_ENCRYPTION_KEY in resp.headers
            ):
                logger.warning(
                    "Using Bucket to get an encrypted object will return "
                    "raw data, please confirm if you really want to do this"
                )

    def _make_crypto(self, byte_range, discard):
        content_crypto_material = ContentCryptoMaterial(
            self.__crypto_provider.cipher, self.__crypto_provider.wrap_alg
        )
        content_crypto_material.from_object_meta(self.resp.headers)

        if content_crypto_material.is_unencrypted():
            logger.info(
                "The object is not encrypted, use crypto provider is not recommended"
            )
        else:
            crypto_provider = self.__crypto_provider
            if content_crypto_material.mat_desc != self.__crypto_provider.mat_desc:
                logger.warning(
                    "The material description of the object "
                    "and the provider is inconsistent"
                )
                encryption_materials = self.__crypto_provider.get_encryption_materials(
                    content_crypto_material.mat_desc
                )
                if encryption_materials:
                    crypto_provider = self.__crypto_provider.reset_encryption_materials(
                        encryption_materials
                    )
                else:
                    raise ClientError(
                        "There is no encryption materials "
                        "match the material description of the object"
                    )

            plain_key = crypto_provider.decrypt_encrypted_key(
                content_crypto_material.encrypted_key
            )
            if content_crypto_material.deprecated:
                if content_crypto_material.wrap_alg == KMS_ALI_WRAP_ALGORITHM:
                    plain_counter = int(
                        crypto_provider.decrypt_encrypted_iv(
                            content_crypto_material.encrypted_iv, True
                        )
                    )
                else:
                    plain_counter = int(
                        crypto_provider.decrypt_encrypted_iv(
                            content_crypto_material.encrypted_iv
                        )
                    )
            else:
                plain_iv = crypto_provider.decrypt_encrypted_iv(
                    content_crypto_material.encrypted_iv
                )

            offset = 0
            if self.content_range:
                start, _ = crypto_provider.adjust_range(byte_range[0], byte_range[1])
                offset = content_crypto_material.cipher.calc_offset(start)

            cipher = copy.copy(content_crypto_material.cipher)
            if content_crypto_material.deprecated:
                cipher.initial_by_counter(plain_key, plain_counter + offset)
            else:
                cipher.initialize(plain_key, plain_iv, offset)
            self.stream = crypto_provider.make_decrypt_adapter(
                self.stream, cipher, discard
            )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.resp.release()

    async def __aiter__(self):
        async for data in self.stream:
            return data

    @staticmethod
    def _parse_range_str(content_range):
        # :param str content_range: sample 'bytes 0-128/1024'
        range_data = (content_range.split(" ", 2)[1]).split("/", 2)[0]
        range_start, range_end = range_data.split("-", 2)
        return int(range_start), int(range_end)

    def close(self):
        """close the response response"""
        self.resp.response.close()

    @property
    def client_crc(self):
        """the client crc"""
        if self.__crc_enabled:
            return self.stream.crc
        return None

    async def read(self, amt: int = -1) -> bytes:
        """async read data from stream

        Args:
            amt int: batch size of the data to read

        Returns:
        Awaitable[bytes]:
        """
        return await self.stream.read(amt)
