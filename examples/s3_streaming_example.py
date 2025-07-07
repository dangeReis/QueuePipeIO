#!/usr/bin/env python3
"""
Example: Stream files between S3 buckets using the new pipe architecture

This example demonstrates how to efficiently stream large files from one S3 bucket
to another while computing a SHA256 hash, all with limited memory usage.
"""

import threading
import time
import sys
import os

# Add parent directory to path if running from examples directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queuepipeio import PipeWriter, PipeReader, HashingFilter


def stream_s3_to_s3_with_hash(
    s3_client, source_bucket, source_key, dest_bucket, dest_key, memory_limit_mb=100
):
    """
    Stream a file from one S3 bucket to another while computing SHA256 hash.

    Args:
        s3_client: Boto3 S3 client
        source_bucket: Source bucket name
        source_key: Source object key
        dest_bucket: Destination bucket name
        dest_key: Destination object key
        memory_limit_mb: Memory limit in MB (default: 100MB)

    Returns:
        tuple: (sha256_hash, bytes_transferred, duration_seconds)
    """
    start_time = time.time()

    # Create pipeline: writer -> hasher -> reader
    writer = PipeWriter(
        memory_limit=memory_limit_mb * 1024 * 1024,
        chunk_size=5 * 1024 * 1024,  # 5MB chunks (S3 multipart minimum)
        show_progress=True,  # Show progress bar
    )
    hasher = HashingFilter(algorithm='sha256')
    reader = PipeReader()
    
    # Connect components
    writer | hasher | reader

    bytes_transferred = 0
    exception = None

    def download_worker():
        """Download from S3 and write to pipe"""
        nonlocal exception
        try:
            print(f"Downloading {source_bucket}/{source_key}...")
            
            # Get object and stream it
            response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
            body = response['Body']
            
            # Stream chunks to pipe
            while True:
                chunk = body.read(1024 * 1024)  # Read 1MB at a time
                if not chunk:
                    break
                writer.write(chunk)
                
        except Exception as e:
            exception = e
            print(f"Download error: {e}")
        finally:
            writer.close()

    def upload_worker():
        """Read from pipe and upload to S3"""
        nonlocal bytes_transferred, exception
        try:
            print(f"Uploading to {dest_bucket}/{dest_key}...")
            
            # Initialize multipart upload
            response = s3_client.create_multipart_upload(
                Bucket=dest_bucket,
                Key=dest_key
            )
            upload_id = response['UploadId']
            
            parts = []
            part_number = 1
            
            # Upload parts
            while True:
                # Read 5MB chunks for multipart upload
                chunk = reader.read(5 * 1024 * 1024)
                if not chunk:
                    break
                
                # Upload part
                response = s3_client.upload_part(
                    Bucket=dest_bucket,
                    Key=dest_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=chunk
                )
                
                parts.append({
                    'ETag': response['ETag'],
                    'PartNumber': part_number
                })
                
                bytes_transferred += len(chunk)
                part_number += 1
            
            # Complete multipart upload
            s3_client.complete_multipart_upload(
                Bucket=dest_bucket,
                Key=dest_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            
            print(f"Upload complete: {bytes_transferred / (1024*1024):.1f} MB")
            
        except Exception as e:
            exception = e
            print(f"Upload error: {e}")
            # Abort multipart upload on error
            if 'upload_id' in locals():
                s3_client.abort_multipart_upload(
                    Bucket=dest_bucket,
                    Key=dest_key,
                    UploadId=upload_id
                )

    # Start workers
    download_thread = threading.Thread(target=download_worker)
    upload_thread = threading.Thread(target=upload_worker)
    
    download_thread.start()
    upload_thread.start()
    
    # Wait for completion
    download_thread.join()
    upload_thread.join()
    
    if exception:
        raise exception
    
    duration = time.time() - start_time
    sha256_hash = hasher.get_hash()
    
    return sha256_hash, bytes_transferred, duration


def main():
    """Example usage"""
    import boto3
    
    # Configure S3 client (adjust for your environment)
    s3 = boto3.client('s3',
        endpoint_url='http://localhost:4566',  # LocalStack endpoint
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )
    
    # Example usage
    try:
        hash_value, bytes_transferred, duration = stream_s3_to_s3_with_hash(
            s3,
            source_bucket='my-source-bucket',
            source_key='large-file.bin',
            dest_bucket='my-dest-bucket', 
            dest_key='large-file-copy.bin',
            memory_limit_mb=50  # Use only 50MB of memory
        )
        
        print(f"\nTransfer complete!")
        print(f"SHA256: {hash_value}")
        print(f"Bytes transferred: {bytes_transferred:,}")
        print(f"Duration: {duration:.2f} seconds")
        print(f"Throughput: {bytes_transferred / (1024*1024) / duration:.2f} MB/s")
        
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()