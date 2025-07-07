"""S3 test utilities for QueueIO with LocalStack support."""

import os
import hashlib
import tempfile
import threading
from typing import Optional, Tuple, Dict, Any, BinaryIO
from contextlib import contextmanager

try:
    import boto3
    from botocore.exceptions import ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    boto3 = None
    ClientError = None
    BOTO3_AVAILABLE = False

from queuepipeio import QueueIO, LimitedQueueIO


class S3TestConfig:
    """LocalStack S3 configuration."""

    ENDPOINT_URL = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
    REGION = "us-east-1"
    ACCESS_KEY = "test"
    SECRET_KEY = "test"

    # S3 multipart upload constraints
    MIN_PART_SIZE = 5 * 1024 * 1024  # 5MB minimum part size
    MAX_PART_SIZE = 5 * 1024 * 1024 * 1024  # 5GB maximum part size
    DEFAULT_PART_SIZE = 8 * 1024 * 1024  # 8MB default part size


class TestFileGenerator:
    """Utilities for generating test files with known content and hashes."""

    @staticmethod
    def generate_test_data(size: int, pattern: bytes = b"A") -> bytes:
        """Generate test data of specified size with repeating pattern."""
        if len(pattern) == 0:
            raise ValueError("Pattern cannot be empty")

        full_repeats = size // len(pattern)
        remainder = size % len(pattern)

        return pattern * full_repeats + pattern[:remainder]

    @staticmethod
    def generate_random_data(size: int) -> bytes:
        """Generate random test data of specified size."""
        return os.urandom(size)

    @staticmethod
    def create_test_file(size: int, pattern: bytes = b"A") -> Tuple[str, str, str]:
        """
        Create a test file with known content and return path, MD5, and SHA256.

        Returns:
            Tuple of (file_path, md5_hex, sha256_hex)
        """
        data = TestFileGenerator.generate_test_data(size, pattern)

        # Calculate hashes
        md5 = hashlib.md5(data).hexdigest()
        sha256 = hashlib.sha256(data).hexdigest()

        # Write to temporary file
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(data)
            return f.name, md5, sha256

    @staticmethod
    def create_multipart_test_file(size: int) -> Tuple[str, str, str, list]:
        """
        Create a test file suitable for multipart upload with part hashes.

        Returns:
            Tuple of (file_path, md5_hex, sha256_hex, part_md5s)
        """
        if size < S3TestConfig.MIN_PART_SIZE:
            min_size = S3TestConfig.MIN_PART_SIZE
            raise ValueError(
                f"File size must be at least {min_size} bytes for multipart upload"
            )

        data = TestFileGenerator.generate_random_data(size)

        # Calculate full file hashes
        md5 = hashlib.md5(data).hexdigest()
        sha256 = hashlib.sha256(data).hexdigest()

        # Calculate part hashes
        part_size = S3TestConfig.DEFAULT_PART_SIZE
        part_md5s = []

        for i in range(0, size, part_size):
            part_data = data[i : i + part_size]
            part_md5s.append(hashlib.md5(part_data).hexdigest())

        # Write to temporary file
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(data)
            return f.name, md5, sha256, part_md5s


class HashUtils:
    """Utilities for computing file hashes."""

    @staticmethod
    def compute_md5(file_path: str, chunk_size: int = 8192) -> str:
        """Compute MD5 hash of a file."""
        md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                md5.update(chunk)
        return md5.hexdigest()

    @staticmethod
    def compute_sha256(file_path: str, chunk_size: int = 8192) -> str:
        """Compute SHA256 hash of a file."""
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                sha256.update(chunk)
        return sha256.hexdigest()

    @staticmethod
    def compute_etag_multipart(part_md5s: list) -> str:
        """
        Compute S3 ETag for multipart upload.

        Format: "{md5_of_md5s}-{num_parts}"
        """
        if not part_md5s:
            raise ValueError("No part MD5s provided")

        # Concatenate binary MD5s and compute MD5 of that
        combined = b"".join(bytes.fromhex(md5) for md5 in part_md5s)
        final_md5 = hashlib.md5(combined).hexdigest()

        return f"{final_md5}-{len(part_md5s)}"


class S3ClientHelper:
    """Helper for creating and managing S3 clients."""

    @staticmethod
    def create_client(**kwargs):
        """Create S3 client configured for LocalStack."""
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for S3 tests. Install with: pip install boto3")
        
        config = {
            "endpoint_url": S3TestConfig.ENDPOINT_URL,
            "region_name": S3TestConfig.REGION,
            "aws_access_key_id": S3TestConfig.ACCESS_KEY,
            "aws_secret_access_key": S3TestConfig.SECRET_KEY,
        }
        config.update(kwargs)

        return boto3.client("s3", **config)

    @staticmethod
    @contextmanager
    def temporary_bucket(s3_client: boto3.client, bucket_name: str):
        """Context manager for creating and cleaning up a test bucket."""
        try:
            # Create bucket
            s3_client.create_bucket(Bucket=bucket_name)
            yield bucket_name
        finally:
            # Clean up bucket
            try:
                # Delete all objects first
                response = s3_client.list_objects_v2(Bucket=bucket_name)
                if "Contents" in response:
                    for obj in response["Contents"]:
                        s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])

                # Delete bucket
                s3_client.delete_bucket(Bucket=bucket_name)
            except ClientError:
                pass  # Ignore errors during cleanup


class S3StreamHandler:
    """Handler for streaming uploads/downloads using QueueIO."""

    def __init__(self, s3_client: boto3.client, chunk_size: int = 8 * 1024 * 1024):
        """
        Initialize S3 stream handler.

        Args:
            s3_client: Boto3 S3 client
            chunk_size: Size of chunks for streaming (default 8MB)
        """
        self.s3_client = s3_client
        self.chunk_size = chunk_size

    def upload_stream(
        self,
        source: BinaryIO,
        bucket: str,
        key: str,
        use_multipart: bool = True,
        part_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Upload from a stream to S3.

        Args:
            source: Input stream
            bucket: S3 bucket name
            key: S3 object key
            use_multipart: Whether to use multipart upload for large files
            part_size: Part size for multipart upload (default: DEFAULT_PART_SIZE)

        Returns:
            Upload response metadata
        """
        if part_size is None:
            part_size = S3TestConfig.DEFAULT_PART_SIZE

        # For small files or when multipart is disabled, use simple upload
        if not use_multipart:
            data = source.read()
            return self.s3_client.put_object(Bucket=bucket, Key=key, Body=data)

        # Use multipart upload
        return self._multipart_upload_stream(source, bucket, key, part_size)

    def _multipart_upload_stream(
        self, source: BinaryIO, bucket: str, key: str, part_size: int
    ) -> Dict[str, Any]:
        """Perform multipart upload from stream."""
        # Ensure part size meets S3 requirements
        if part_size < S3TestConfig.MIN_PART_SIZE:
            part_size = S3TestConfig.MIN_PART_SIZE

        # Initiate multipart upload
        response = self.s3_client.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = response["UploadId"]

        parts = []
        part_number = 1

        try:
            while True:
                # Read part data
                data = source.read(part_size)
                if not data:
                    break

                # Upload part
                part_response = self.s3_client.upload_part(
                    Bucket=bucket,
                    Key=key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=data,
                )

                parts.append({"ETag": part_response["ETag"], "PartNumber": part_number})

                part_number += 1

            # Complete multipart upload
            return self.s3_client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

        except Exception:
            # Abort on error
            self.s3_client.abort_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload_id
            )
            raise

    def download_stream(
        self,
        bucket: str,
        key: str,
        destination: BinaryIO,
        chunk_size: Optional[int] = None,
    ) -> int:
        """
        Download from S3 to a stream.

        Args:
            bucket: S3 bucket name
            key: S3 object key
            destination: Output stream
            chunk_size: Size of chunks for streaming

        Returns:
            Total bytes downloaded
        """
        if chunk_size is None:
            chunk_size = self.chunk_size

        response = self.s3_client.get_object(Bucket=bucket, Key=key)
        body = response["Body"]

        total_bytes = 0
        while True:
            chunk = body.read(chunk_size)
            if not chunk:
                break
            destination.write(chunk)
            total_bytes += len(chunk)

        return total_bytes

    def stream_with_queueio(
        self,
        bucket: str,
        key: str,
        file_path: str,
        upload: bool = True,
        memory_limit: Optional[int] = None,
    ) -> QueueIO:
        """
        Stream upload/download using QueueIO in a separate thread.

        Args:
            bucket: S3 bucket name
            key: S3 object key
            file_path: Local file path
            upload: True for upload, False for download
            memory_limit: Memory limit for LimitedQueueIO (None for unlimited)

        Returns:
            QueueIO instance for streaming
        """
        # Create appropriate QueueIO instance
        if memory_limit:
            queue_io = LimitedQueueIO(
                chunk_size=self.chunk_size, memory_limit=memory_limit
            )
        else:
            queue_io = QueueIO(chunk_size=self.chunk_size)

        # Start streaming thread
        if upload:
            thread = threading.Thread(
                target=self._upload_worker, args=(queue_io, bucket, key, file_path)
            )
        else:
            thread = threading.Thread(
                target=self._download_worker, args=(queue_io, bucket, key, file_path)
            )

        thread.daemon = True
        thread.start()

        return queue_io

    def _upload_worker(self, queue_io: QueueIO, bucket: str, key: str, file_path: str):
        """Worker thread for upload streaming."""
        try:
            with open(file_path, "rb") as f:
                while True:
                    chunk = f.read(self.chunk_size)
                    if not chunk:
                        break
                    queue_io.write(chunk)
        finally:
            queue_io.close()

        # Perform the upload
        self.upload_stream(queue_io, bucket, key)

    def _download_worker(
        self, queue_io: QueueIO, bucket: str, key: str, file_path: str
    ):
        """Worker thread for download streaming."""
        try:
            with open(file_path, "wb") as f:
                self.download_stream(bucket, key, f)
        finally:
            queue_io.close()


# Convenience functions
def create_test_environment():
    """Create a complete test environment with S3 client and test bucket."""
    if not BOTO3_AVAILABLE:
        raise ImportError("boto3 is required for S3 tests. Install with: pip install boto3")
    client = S3ClientHelper.create_client()
    return client


def cleanup_test_files(*file_paths):
    """Clean up temporary test files."""
    for path in file_paths:
        try:
            if path and os.path.exists(path):
                os.remove(path)
        except OSError:
            pass
