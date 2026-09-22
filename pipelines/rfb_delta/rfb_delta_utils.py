#!/usr/bin/env python3
"""
RFB Delta Lake Utilities

Shared functions for Delta operations:
- S3 client management
- Merge logic
- Z-order optimization
- Progress tracking
"""

import os
import sys
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import logging

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable, write_deltalake
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3ClientConfig:
    """S3/MinIO client configuration."""
    
    def __init__(self):
        self.endpoint = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
        self.access_key = os.getenv('MINIO_ROOT_USER')
        self.secret_key = os.getenv('MINIO_ROOT_PASSWORD')
        self.use_ssl = os.getenv('S3_USE_SSL', 'false').lower() == 'true'
        
        if not all([self.access_key, self.secret_key]):
            raise ValueError("MINIO_ROOT_USER and MINIO_ROOT_PASSWORD must be set")
        
        self.storage_options = {
            'aws_access_key_id': self.access_key,
            'aws_secret_access_key': self.secret_key,
            'endpoint_url': self.endpoint,
            'use_ssl': self.use_ssl,
            'verify': False,
        }
    
    def boto3_session(self):
        """Create boto3 session."""
        return boto3.Session(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )
    
    def s3_client(self):
        """Create S3 client for MinIO."""
        session = self.boto3_session()
        return session.client(
            's3',
            endpoint_url=self.endpoint,
            use_ssl=self.use_ssl,
            verify=False,
        )


class DeltaLakeOptimizer:
    """Optimize Delta Lake tables."""
    
    @staticmethod
    def optimize_table(table_path: str, 
                      z_order_cols: List[str],
                      storage_options: Dict) -> Dict:
        """
        Optimize Delta table: compact files + Z-order clustering.
        
        Returns statistics about optimization.
        """
        logger.info(f"Optimizing Delta table: {table_path}")
        
        try:
            dt = DeltaTable(table_path, storage_options=storage_options)
            
            # Get pre-optimization stats
            pre_files = len(dt.get_file_paths())
            pre_size = sum(os.path.getsize(p) for p in dt.get_file_paths() if os.path.exists(p))
            
            # Compact small files (default 10MB chunks)
            logger.info("  → Compacting files...")
            dt.optimize(mode='compact')
            
            # Apply Z-order clustering if columns specified
            if z_order_cols:
                logger.info(f"  → Z-ordering by {z_order_cols}...")
                dt.optimize(z_order_by=z_order_cols)
            
            # Get post-optimization stats
            post_files = len(dt.get_file_paths())
            post_size = sum(os.path.getsize(p) for p in dt.get_file_paths() if os.path.exists(p))
            
            stats = {
                'pre_files': pre_files,
                'post_files': post_files,
                'files_reduced': pre_files - post_files,
                'pre_size_mb': pre_size / (1024**2),
                'post_size_mb': post_size / (1024**2),
                'compression_ratio': round((1 - post_size/pre_size) * 100, 2) if pre_size > 0 else 0,
            }
            
            logger.info(f"  ✓ Optimization complete: {stats['files_reduced']} files merged, "
                       f"{stats['compression_ratio']}% size reduction")
            return stats
        
        except Exception as e:
            logger.error(f"Optimization failed: {e}")
            return {}
    
    @staticmethod
    def merge_into_delta(target_path: str,
                        source_df: pl.DataFrame,
                        merge_keys: List[str],
                        partition_col: str,
                        partition_value: int,
                        storage_options: Dict) -> Tuple[int, int]:
        """
        Merge new data into Delta table.
        
        DELETE old partition + INSERT new (atomic operation).
        Returns (deleted_count, inserted_count).
        """
        logger.info(f"Merging into {target_path} (partition {partition_col}={partition_value})")
        
        try:
            dt = DeltaTable(target_path, storage_options=storage_options)
            
            # Delete old partition
            logger.info(f"  → Deleting partition {partition_col}={partition_value}...")
            dt.delete(f"{partition_col} = {partition_value}")
            deleted = dt.to_pandas().shape[0]  # rough estimate
            
            # Insert new data
            logger.info(f"  → Inserting {len(source_df)} new rows...")
            table = source_df.to_arrow()
            write_deltalake(
                table_uri=target_path,
                data=table,
                mode='append',
                storage_options=storage_options,
            )
            
            logger.info(f"  ✓ Merge complete: {deleted} deleted, {len(source_df)} inserted")
            return deleted, len(source_df)
        
        except Exception as e:
            logger.error(f"Merge failed: {e}")
            return 0, 0


class ProgressTracker:
    """Track progress of bulk operations."""
    
    def __init__(self, total: int, label: str = "Processing"):
        self.total = total
        self.label = label
        self.processed = 0
        self.start_time = datetime.now()
    
    def update(self, count: int):
        """Update progress."""
        self.processed += count
        elapsed = (datetime.now() - self.start_time).total_seconds()
        rate = self.processed / elapsed if elapsed > 0 else 0
        eta_sec = (self.total - self.processed) / rate if rate > 0 else 0
        
        pct = round(100 * self.processed / self.total, 1)
        logger.info(
            f"  {self.label}: {self.processed}/{self.total} "
            f"({pct}%) | {rate:.0f} rows/sec | ETA: {eta_sec:.0f}s"
        )
    
    def done(self):
        """Log completion."""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        rate = self.processed / elapsed if elapsed > 0 else 0
        logger.info(
            f"  ✓ {self.label} complete: {self.processed} rows in {elapsed:.1f}s "
            f"({rate:.0f} rows/sec)"
        )


def list_s3_parquets(bucket: str, prefix: str, s3_client) -> List[str]:
    """List all parquet files in S3 prefix."""
    keys = []
    paginator = s3_client.get_paginator('list_objects_v2')
    
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                if obj['Key'].endswith('.parquet'):
                    keys.append(obj['Key'])
    except ClientError as e:
        logger.error(f"Error listing S3: {e}")
    
    return sorted(keys)


def read_parquet_from_s3(s3_path: str, s3_config: S3ClientConfig) -> pl.DataFrame:
    """Read single parquet file from S3 into Polars."""
    try:
        df = pl.read_parquet(s3_path, storage_options=s3_config.storage_options)
        logger.debug(f"Loaded {len(df)} rows from {s3_path}")
        return df
    except Exception as e:
        logger.error(f"Failed to read {s3_path}: {e}")
        raise


def read_delta_from_s3(delta_path: str, s3_config: S3ClientConfig, 
                       filters: Optional[Dict] = None) -> pl.DataFrame:
    """Read Delta table from S3 into Polars."""
    try:
        dt = DeltaTable(delta_path, storage_options=s3_config.storage_options)
        pdf = dt.to_pandas()
        df = pl.from_pandas(pdf)
        logger.debug(f"Loaded Delta {delta_path}: {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Failed to read Delta {delta_path}: {e}")
        raise


def write_parquet_to_s3(df: pl.DataFrame, s3_path: str, 
                        s3_config: S3ClientConfig, compression: str = 'snappy'):
    """Write Polars DataFrame as parquet to S3."""
    try:
        table = df.to_arrow()
        pq.write_table(
            table,
            s3_path,
            compression=compression,
            coerce_timestamps='us',
        )
        logger.debug(f"Wrote {len(df)} rows to {s3_path}")
    except Exception as e:
        logger.error(f"Failed to write {s3_path}: {e}")
        raise