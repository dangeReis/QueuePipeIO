#!/usr/bin/env python3
"""
Example: Stream files between S3 buckets with custom hash using QueuePipeIO

This example demonstrates how to efficiently stream large files from one S3 bucket 
to another while computing a SHA256 hash, all with limited memory usage.
"""

import hashlib
import threading
import time
from queuepipeio import QueueIO, LimitedQueueIO


def stream_s3_to_s3_with_hash(s3_client, source_bucket, source_key, 
                              dest_bucket, dest_key, memory_limit_mb=100):
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
    
    # Create QueueIO with memory limit
    qio = LimitedQueueIO(
        memory_limit=memory_limit_mb * 1024 * 1024,
        chunk_size=5 * 1024 * 1024,  # 5MB chunks (S3 multipart minimum)
        show_progress=True,  # Show progress bar
        write_timeout=30  # 30 second timeout if queue is full
    )
    
    # Hash object for computing SHA256
    hasher = hashlib.sha256()
    bytes_processed = [0]  # Use list to avoid nonlocal in nested class
    exception = [None]
    
    def download_worker():
        """Download from S3 and write to queue"""
        try:
            print(f"Downloading {source_bucket}/{source_key}...")
            response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
            
            # Stream the body in chunks
            for chunk in response['Body'].iter_chunks(chunk_size=1024 * 1024):
                qio.write(chunk)
                
        except Exception as e:
            exception[0] = e
            print(f"Download error: {e}")
        finally:
            qio.close()
    
    def upload_worker():
        """Read from queue and upload to S3 with multipart"""
        try:
            print(f"Uploading to {dest_bucket}/{dest_key}...")
            
            # Start multipart upload
            upload = s3_client.create_multipart_upload(
                Bucket=dest_bucket, 
                Key=dest_key
            )
            upload_id = upload['UploadId']
            parts = []
            part_number = 1
            
            try:
                while True:
                    # Read 5MB chunks for multipart
                    chunk = qio.read(5 * 1024 * 1024)
                    if not chunk:
                        break
                    
                    # Update hash
                    hasher.update(chunk)
                    bytes_processed[0] += len(chunk)
                    
                    # Upload part
                    response = s3_client.upload_part(
                        Bucket=dest_bucket,
                        Key=dest_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=chunk
                    )
                    
                    parts.append({
                        'PartNumber': part_number,
                        'ETag': response['ETag']
                    })
                    part_number += 1
                
                # Complete multipart upload
                s3_client.complete_multipart_upload(
                    Bucket=dest_bucket,
                    Key=dest_key,
                    UploadId=upload_id,
                    MultipartUpload={'Parts': parts}
                )
                print(f"Upload complete: {part_number-1} parts")
                
            except Exception as e:
                # Abort multipart upload on error
                s3_client.abort_multipart_upload(
                    Bucket=dest_bucket,
                    Key=dest_key,
                    UploadId=upload_id
                )
                raise e
                
        except Exception as e:
            exception[0] = e
            print(f"Upload error: {e}")
    
    # Start workers
    download_thread = threading.Thread(target=download_worker)
    upload_thread = threading.Thread(target=upload_worker)
    
    download_thread.start()
    upload_thread.start()
    
    # Wait for completion
    download_thread.join()
    upload_thread.join()
    
    # Check for errors
    if exception[0]:
        raise exception[0]
    
    duration = time.time() - start_time
    
    # Calculate throughput
    throughput_mbps = (bytes_processed[0] / (1024 * 1024)) / duration
    print(f"\nTransfer complete!")
    print(f"  Bytes: {bytes_processed[0]:,}")
    print(f"  Duration: {duration:.2f}s")
    print(f"  Throughput: {throughput_mbps:.2f} MB/s")
    print(f"  SHA256: {hasher.hexdigest()}")
    
    return hasher.hexdigest(), bytes_processed[0], duration


def example_usage():
    """Example usage with boto3"""
    import boto3
    
    # Configure S3 client
    # For LocalStack: endpoint_url='http://localhost:4566'
    # For AWS: use your credentials
    s3 = boto3.client('s3')
    
    # Example parameters
    source_bucket = 'my-source-bucket'
    source_key = 'large-file.zip'
    dest_bucket = 'my-dest-bucket'
    dest_key = 'large-file-copy.zip'
    
    try:
        # Stream with 50MB memory limit
        hash_value, total_bytes, duration = stream_s3_to_s3_with_hash(
            s3, 
            source_bucket, 
            source_key,
            dest_bucket, 
            dest_key,
            memory_limit_mb=50
        )
        
        print(f"\nSuccess! File hash: {hash_value}")
        
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    print(__doc__)
    print("\nNote: This is an example. Configure the S3 client and bucket names for your environment.")
    print("To run the example, uncomment the line below and update the parameters.\n")
    # example_usage()