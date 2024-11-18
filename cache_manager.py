# cache_manager.py
import asyncio
import redis.asyncio as redis
from datetime import datetime, timedelta
import orjson
import zlib
from typing import Dict, List, Optional, Any
import polars as pl
from dataclasses import dataclass
import aiologger
from concurrent.futures import ProcessPoolExecutor
import signal
from functools import partial

@dataclass
class CacheConfig:
    chunk_size: int = 50000
    compression_level: int = 6
    cache_ttl: int = 3600  # 1 hour
    background_refresh: bool = True
    refresh_threshold: int = 300  # 5 minutes
    max_parallel_chunks: int = 4

class AsyncCacheWorker:
    def __init__(
        self,
        redis_url: str,
        config: CacheConfig = CacheConfig(),
        logger: Optional[aiologger.Logger] = None
    ):
        self.redis = redis.from_url(
            redis_url,
            decode_responses=False,
            socket_timeout=5,
            socket_connect_timeout=5
        )
        self.config = config
        self.logger = logger or aiologger.Logger.with_default_handlers()
        self.process_pool = ProcessPoolExecutor()
        self._cache_tasks = set()
        self._shutdown = False

    async def start(self):
        """Start the cache worker"""
        asyncio.create_task(self._background_refresh_loop())
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        """Handle graceful shutdown"""
        self._shutdown = True
        asyncio.create_task(self._cleanup())

    async def _cleanup(self):
        """Cleanup resources"""
        await self.logger.info("Shutting down cache worker...")
        for task in self._cache_tasks:
            task.cancel()
        await self.redis.close()
        self.process_pool.shutdown(wait=True)
        await self.logger.info("Cache worker shutdown complete")

    async def _background_refresh_loop(self):
        """Background loop to refresh cache entries"""
        while not self._shutdown:
            try:
                await self._refresh_expired_entries()
                await asyncio.sleep(self.config.refresh_threshold)
            except Exception as e:
                await self.logger.error(f"Error in background refresh: {e}")

    async def _refresh_expired_entries(self):
        """Refresh cache entries approaching expiration"""
        pattern = b"metadata:*"
        cursor = 0
        while True:
            cursor, keys = await self.redis.scan(
                cursor,
                match=pattern,
                count=100
            )
            for key in keys:
                try:
                    ttl = await self.redis.ttl(key)
                    if ttl < self.config.refresh_threshold:
                        # Decode key from bytes to string for processing
                        key_str = key.decode('utf-8')
                        symbol, timeframe = key_str.split(":")[1:3]
                        self._schedule_refresh(symbol, timeframe)
                except Exception as e:
                    await self.logger.error(f"Error refreshing key {key}: {e}")
            
            if cursor == 0:
                break

    def _schedule_refresh(self, symbol: str, timeframe: str):
        """Schedule a cache refresh task"""
        task = asyncio.create_task(self._refresh_cache(symbol, timeframe))
        self._cache_tasks.add(task)
        task.add_done_callback(self._cache_tasks.remove)

    async def _refresh_cache(self, symbol: str, timeframe: str):
        """Refresh cache for a specific symbol and timeframe"""
        # Implementation depends on your data fetching logic
        pass

    async def cache_data(
        self,
        symbol: str,
        timeframe: str,
        data: Dict[str, List],
        metadata: Optional[Dict] = None
    ) -> None:
        """Cache data asynchronously"""
        if not data or not data.get("timestamp"):
            return

        try:
            df = pl.DataFrame(data)
            chunks = await self._process_chunks(df)
            
            pipe = self.redis.pipeline()
            
            # Store metadata
            metadata = metadata or {
                "last_updated": datetime.utcnow().isoformat(),
                "start_date": data["timestamp"][0],
                "end_date": data["timestamp"][-1],
                "chunk_count": len(chunks)
            }
            
            metadata_key = f"metadata:{symbol}:{timeframe}".encode('utf-8')
            pipe.set(
                metadata_key,
                await self._compress_data(metadata),
                ex=self.config.cache_ttl
            )

            # Store chunks in parallel
            chunk_tasks = []
            for i, chunk in enumerate(chunks):
                chunk_key = f"data:{symbol}:{timeframe}:chunk:{i}".encode('utf-8')
                chunk_tasks.append(
                    self._store_chunk(pipe, chunk_key, chunk)
                )
            
            await asyncio.gather(*chunk_tasks)
            await pipe.execute()
            
        except Exception as e:
            await self.logger.error(f"Error caching data: {e}")
            raise

    async def _process_chunks(self, df: pl.DataFrame) -> List[Dict[str, List]]:
        """Process dataframe into chunks using process pool"""
        total_rows = len(df)
        chunk_count = (total_rows + self.config.chunk_size - 1) // self.config.chunk_size
        
        chunk_frames = []
        for i in range(chunk_count):
            start_idx = i * self.config.chunk_size
            end_idx = min((i + 1) * self.config.chunk_size, total_rows)
            chunk_frames.append(df.slice(start_idx, end_idx - start_idx))

        loop = asyncio.get_event_loop()
        chunks = await asyncio.gather(*[
            loop.run_in_executor(
                self.process_pool,
                partial(self._process_chunk, chunk)
            )
            for chunk in chunk_frames
        ])
        
        return chunks

    @staticmethod
    def _process_chunk(chunk_df: pl.DataFrame) -> Dict[str, List]:
        """Process a single chunk in a separate process"""
        return {
            col: chunk_df[col].to_list()
            for col in chunk_df.columns
        }

    async def _store_chunk(
        self,
        pipe: redis.client.Pipeline,
        key: bytes,
        data: Dict[str, List]
    ) -> None:
        """Store a compressed chunk in Redis"""
        compressed_data = await self._compress_data(data)
        pipe.set(key, compressed_data, ex=self.config.cache_ttl)

    async def _compress_data(self, data: Any) -> bytes:
        """Compress data using process pool"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.process_pool,
            partial(
                zlib.compress,
                orjson.dumps(data),
                self.config.compression_level
            )
        )

    async def get_cached_data(
        self,
        symbol: str,
        timeframe: str,
        start_date: datetime,
        end_date: datetime
    ) -> Optional[Dict[str, List]]:
        """Retrieve cached data"""
        try:
            metadata = await self._get_metadata(symbol, timeframe)
            if not metadata:
                return None

            metadata_start = datetime.fromisoformat(metadata["start_date"])
            metadata_end = datetime.fromisoformat(metadata["end_date"])

            if not (metadata_start <= start_date and metadata_end >= end_date):
                return None

            chunk_indices = self._calculate_chunk_indices(
                start_date,
                end_date,
                metadata_start,
                metadata_end,
                metadata["chunk_count"]
            )

            chunk_data = await self._fetch_chunks(symbol, timeframe, chunk_indices)
            if not chunk_data:
                return None

            return await self._merge_and_filter_chunks(
                chunk_data,
                start_date,
                end_date
            )

        except Exception as e:
            await self.logger.error(f"Error retrieving cached data: {e}")
            return None

    async def _get_metadata(
        self,
        symbol: str,
        timeframe: str
    ) -> Optional[Dict]:
        """Get metadata for cached data"""
        metadata_key = f"metadata:{symbol}:{timeframe}".encode('utf-8')
        metadata_bytes = await self.redis.get(metadata_key)
        
        if not metadata_bytes:
            return None
            
        try:
            return orjson.loads(zlib.decompress(metadata_bytes))
        except Exception as e:
            await self.logger.error(f"Error decoding metadata: {e}")
            return None

    def _calculate_chunk_indices(
        self,
        start_date: datetime,
        end_date: datetime,
        metadata_start: datetime,
        metadata_end: datetime,
        chunk_count: int
    ) -> List[int]:
        """Calculate required chunk indices"""
        total_seconds = (metadata_end - metadata_start).total_seconds()
        seconds_per_chunk = total_seconds / chunk_count

        start_chunk = max(0, int(
            (start_date - metadata_start).total_seconds() / seconds_per_chunk
        ))
        end_chunk = min(
            chunk_count - 1,
            int((end_date - metadata_start).total_seconds() / seconds_per_chunk)
        )

        return list(range(start_chunk, end_chunk + 1))

    async def _fetch_chunks(
        self,
        symbol: str,
        timeframe: str,
        chunk_indices: List[int]
    ) -> List[Dict[str, List]]:
        """Fetch multiple chunks in parallel"""
        chunk_keys = [
            f"data:{symbol}:{timeframe}:chunk:{i}".encode('utf-8')
            for i in chunk_indices
        ]
        
        chunks = []
        for i in range(0, len(chunk_keys), self.config.max_parallel_chunks):
            batch_keys = chunk_keys[i:i + self.config.max_parallel_chunks]
            batch_data = await asyncio.gather(*[
                self._fetch_chunk(key) for key in batch_keys
            ])
            chunks.extend(batch_data)
        
        return [chunk for chunk in chunks if chunk is not None]

    async def _fetch_chunk(self, key: bytes) -> Optional[Dict[str, List]]:
        """Fetch and decompress a single chunk"""
        try:
            chunk_bytes = await self.redis.get(key)
            if not chunk_bytes:
                return None
            
            loop = asyncio.get_event_loop()
            decompressed = await loop.run_in_executor(
                self.process_pool,
                zlib.decompress,
                chunk_bytes
            )
            return orjson.loads(decompressed)
        except Exception as e:
            await self.logger.error(f"Error fetching chunk {key}: {e}")
            return None

    async def _merge_and_filter_chunks(
        self,
        chunks: List[Dict[str, List]],
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, List]:
        """Merge and filter chunks efficiently"""
        try:
            df = pl.concat([pl.DataFrame(chunk) for chunk in chunks])
            
            mask = (
                (pl.col("timestamp") >= start_date.isoformat()) &
                (pl.col("timestamp") <= end_date.isoformat())
            )
            filtered_df = df.filter(mask).sort("timestamp")
            
            return {
                col: filtered_df[col].to_list()
                for col in filtered_df.columns
            }
        except Exception as e:
            await self.logger.error(f"Error merging chunks: {e}")
            return {
                "timestamp": [], "open": [], "high": [],
                "low": [], "close": [], "volume": []
            }