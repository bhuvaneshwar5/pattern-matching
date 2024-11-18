# import polars as pl
# from motor.motor_asyncio import AsyncIOMotorClient
# from datetime import datetime, timedelta
# import pytz
# from typing import List, Dict, Any, Tuple, Optional, Iterator
# from pymongo import ReplaceOne, ASCENDING, DESCENDING
# import asyncio
# import numpy as np
# from collections import deque
# from datetime import timezone
# import orjson
# from functools import lru_cache
# import io
# from concurrent.futures import ThreadPoolExecutor
# from itertools import islice


# class DataProcessor:
#     def __init__(self, db_url: str):
#         self.client = AsyncIOMotorClient(
#             db_url,
#             maxPoolSize=60,
#             minPoolSize=30,
#             serverSelectionTimeoutMS=2000,
#             connectTimeoutMS=2000,
#             maxIdleTimeMS=200000,
#             retryWrites=True,
#             w=1,
#             journal=False
#         )
#         self.db = self.client["Historical_data"]
#         self.chunk_size = 200000
#         self.upload_chunk_size = 100000
#         self.max_workers = 4
#         self.bulk_write_size = 100000
        
#         self.timeframes = {
#             '1T': timedelta(minutes=1),
#             '5T': timedelta(minutes=5),
#             '15T': timedelta(minutes=15),
#             '30T': timedelta(minutes=30),
#             '1H': timedelta(hours=1),
#             '4H': timedelta(hours=4),
#             '1D': timedelta(days=1),
#             '1W': timedelta(weeks=1),
#             '1M': timedelta(days=30)
#         }
        
#         self.polars_timeframes = {
#             '1T': '1m',
#             '5T': '5m',
#             '15T': '15m',
#             '30T': '30m',
#             '1H': '1h',
#             '4H': '4h',
#             '1D': '1d',
#             '1W': '1w',
#             '1M': '1mo'
#         }
#         self.index_cache = set()

#     @lru_cache(maxsize=100)
#     def _get_collection_name(self, symbol: str, timeframe: str) -> str:
#         """Cache collection names to avoid string operations"""
#         return f"{symbol}_{timeframe}"

#     async def _ensure_indexes(self, collection_name: str) -> None:
#         """Ensure minimal required indexes exist"""
#         if collection_name in self.index_cache:
#             return
#         await self.db[collection_name].create_index([
#             ("timestamp", ASCENDING),
#             ("symbol", ASCENDING)
#         ], unique=True, background=True)
#         self.index_cache.add(collection_name)

#     def _chunk_file_content(self, content: bytes, chunk_size: int) -> Iterator[List[str]]:
#         """Split file content into chunks efficiently"""
#         buffer = io.StringIO(content.decode('utf-8'))
#         while True:
#             chunk = list(islice(buffer, chunk_size))
#             if not chunk:
#                 break
#             yield chunk

#     def _parse_chunk(self, lines: List[str], symbol: str) -> List[Dict[str, Any]]:
#         """Parse a chunk of lines into data records"""
#         data = []
#         for line in lines:
#             if line.strip():
#                 try:
#                     parts = line.split(';')
#                     timestamp = datetime.strptime(
#                         parts[0], '%Y%m%d %H%M%S'
#                     ).replace(tzinfo=pytz.UTC)
#                     data.append({
#                         'timestamp': timestamp,
#                         'open': float(parts[1]),
#                         'high': float(parts[2]),
#                         'low': float(parts[3]),
#                         'close': float(parts[4]),
#                         'volume': int(parts[5]),
#                         'symbol': symbol
#                     })
#                 except (ValueError, IndexError) as e:
#                     print(f"Skipping invalid line: {line.strip()} - Error: {str(e)}")
#         return data

#     def _create_polars_frame(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
#         """Create a Polars DataFrame with proper column types"""
#         if not data:
#             return pl.DataFrame()
        
#         return pl.DataFrame(data).with_columns([
#             pl.col('timestamp').cast(pl.Datetime),
#             pl.col('open').cast(pl.Float64),
#             pl.col('high').cast(pl.Float64),
#             pl.col('low').cast(pl.Float64),
#             pl.col('close').cast(pl.Float64),
#             pl.col('volume').cast(pl.Int64),
#             pl.col('symbol').cast(pl.Categorical)
#         ])

#     async def _store_raw_data(self, data: List[Dict[str, Any]], symbol: str) -> None:
#         """Store raw 1-minute data without aggregation"""
#         collection_name = self._get_collection_name(symbol, '1T')
#         await self._ensure_indexes(collection_name)
        
#         bulk_operations = [
#             ReplaceOne(
#                 {"timestamp": item["timestamp"], "symbol": symbol},
#                 item,
#                 upsert=True
#             ) for item in data
#         ]
        
#         try:
#             collection = self.db[collection_name]
#             for i in range(0, len(bulk_operations), self.bulk_write_size):
#                 batch = bulk_operations[i:i + self.bulk_write_size]
#                 result = await collection.bulk_write(batch, ordered=False)
#                 print(f"Raw data batch {i//self.bulk_write_size + 1}: Inserted/Updated {result.upserted_count + result.modified_count} documents for {collection_name}")
#         except Exception as e:
#             print(f"Error storing raw data for {collection_name}: {str(e)}")

#     def aggregate_dataframe(self, df: pl.DataFrame, timeframe: str) -> pl.DataFrame:
#         """Aggregate data into specified timeframe"""
#         return df.group_by_dynamic(
#             index_column="timestamp",
#             every=timeframe,
#             by="symbol"
#         ).agg([
#             pl.col("open").first().alias("open"),
#             pl.col("high").max().alias("high"),
#             pl.col("low").min().alias("low"),
#             pl.col("close").last().alias("close"),
#             pl.col("volume").sum().alias("volume")
#         ]).drop_nulls()

#     async def _aggregate_and_store(
#         self,
#         df: pl.DataFrame,
#         symbol: str,
#         mongo_tf: str,
#         polars_tf: str
#     ) -> None:
#         """Aggregate and store data for a specific timeframe"""
#         collection_name = self._get_collection_name(symbol, mongo_tf)
#         await self._ensure_indexes(collection_name)
        
#         agg_df = self.aggregate_dataframe(df, polars_tf)
#         if agg_df.is_empty():
#             return

#         agg_data = agg_df.to_dicts()
        
#         for i in range(0, len(agg_data), self.bulk_write_size):
#             batch = agg_data[i:i + self.bulk_write_size]
#             bulk_operations = [
#                 ReplaceOne(
#                     {"timestamp": item["timestamp"], "symbol": symbol},
#                     item,
#                     upsert=True
#                 ) for item in batch
#             ]
            
#             try:
#                 collection = self.db[collection_name]
#                 result = await collection.bulk_write(bulk_operations, ordered=False)
#                 print(f"Batch {i//self.bulk_write_size + 1}: Inserted/Updated {result.upserted_count + result.modified_count} documents for {collection_name}")
#             except Exception as e:
#                 print(f"Error during bulk write for {collection_name} batch {i//self.bulk_write_size + 1}: {str(e)}")

#     async def _process_chunk_and_aggregate(
#         self,
#         chunk_data: List[Dict[str, Any]],
#         symbol: str,
#         semaphore: asyncio.Semaphore
#     ) -> None:
#         """Process aggregations for timeframes larger than 1 minute"""
#         if not chunk_data:
#             return

#         df = self._create_polars_frame(chunk_data)
#         if df.is_empty():
#             return

#         async with semaphore:
#             tasks = []
#             # Skip 1T timeframe as it's already stored in raw form
#             for mongo_tf, polars_tf in {k: v for k, v in self.polars_timeframes.items() if k != '1T'}.items():
#                 task = self._aggregate_and_store(df, symbol, mongo_tf, polars_tf)
#                 tasks.append(task)
            
#             await asyncio.gather(*tasks)

#     async def process_file(self, file: Any, symbol: str) -> None:
#         """Process a single file with chunking and parallel processing"""
#         content = await file.read()
        
#         semaphore = asyncio.Semaphore(self.max_workers)
#         tasks = []
        
#         with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
#             for lines in self._chunk_file_content(content, self.upload_chunk_size):
#                 chunk_data = await asyncio.get_event_loop().run_in_executor(
#                     executor,
#                     self._parse_chunk,
#                     lines,
#                     symbol
#                 )
                
#                 if chunk_data:
#                     # Store raw 1-minute data first
#                     await self._store_raw_data(chunk_data, symbol)
                    
#                     # Then process aggregations
#                     task = asyncio.create_task(
#                         self._process_chunk_and_aggregate(chunk_data, symbol, semaphore)
#                     )
#                     tasks.append(task)
                
#                 if len(tasks) >= self.max_workers * 2:
#                     completed, tasks = await asyncio.wait(
#                         tasks,
#                         return_when=asyncio.FIRST_COMPLETED
#                     )
        
#         if tasks:
#             await asyncio.gather(*tasks)

#     async def process_multiple_files(self, files: List[Any], symbol: str) -> Dict[str, Any]:
#         """Process multiple files with progress tracking"""
#         start_time = datetime.now()
#         total_files = len(files)
#         processed_files = 0
#         errors = []

#         async def process_with_progress(file):
#             try:
#                 await self.process_file(file, symbol)
#                 return True, None
#             except Exception as e:
#                 return False, str(e)

#         tasks = [process_with_progress(file) for file in files]
#         results = await asyncio.gather(*tasks, return_exceptions=True)

#         for i, (success, error) in enumerate(results):
#             if success:
#                 processed_files += 1
#             elif error:
#                 errors.append(f"Error processing file {i + 1}: {error}")

#         end_time = datetime.now()
#         processing_time = (end_time - start_time).total_seconds()

#         return {
#             "total_files": total_files,
#             "processed_files": processed_files,
#             "failed_files": total_files - processed_files,
#             "processing_time_seconds": processing_time,
#             "errors": errors
#         }

#     async def get_data(
#         self,
#         symbol: str,
#         start_date: datetime,
#         end_date: datetime,
#         timeframe: str
#     ) -> Dict[str, List]:
#         """Ultra-fast data retrieval with optimized parallel processing"""
#         collection_name = self._get_collection_name(symbol, timeframe)
#         await self._ensure_indexes(collection_name)
        
#         time_diff = (end_date - start_date).total_seconds()
#         chunk_duration = min(time_diff / 10, 86400)
#         num_chunks = max(1, min(10, int(time_diff / chunk_duration)))
        
#         time_chunks = []
#         chunk_size = time_diff / num_chunks
#         for i in range(num_chunks):
#             chunk_start = start_date + timedelta(seconds=i * chunk_size)
#             chunk_end = start_date + timedelta(seconds=(i + 1) * chunk_size)
#             time_chunks.append((chunk_start, chunk_end))

#         async with asyncio.TaskGroup() as tg:
#             tasks = [
#                 tg.create_task(self._fetch_chunk_fast(collection_name, start, end))
#                 for start, end in time_chunks
#             ]
        
#         chunks = [task.result() for task in tasks]
#         return self._combine_chunks_fast(chunks)

#     async def _fetch_chunk_fast(
#         self,
#         collection_name: str,
#         start_date: datetime,
#         end_date: datetime
#     ) -> Dict[str, List]:
#         """Optimized chunk retrieval with pre-allocated memory"""
#         collection = self.db[collection_name]
        
#         projection = {
#             "_id": 0,
#             "timestamp": 1,
#             "open": 1,
#             "high": 1,
#             "low": 1,
#             "close": 1,
#             "volume": 1
#         }
        
#         estimated_size = int((end_date - start_date).total_seconds() / 60) + 100
#         data = {
#             "timestamp": [] * estimated_size,
#             "open": [] * estimated_size,
#             "high": [] * estimated_size,
#             "low": [] * estimated_size,
#             "close": [] * estimated_size,
#             "volume": [] * estimated_size
#         }
        
#         cursor = collection.find(
#             {
#                 "timestamp": {
#                     "$gte": start_date,
#                     "$lt": end_date
#                 }
#             },
#             projection,
#             batch_size=10000
#         ).sort("timestamp", 1)
        
#         async for doc in cursor:
#             data["timestamp"].append(doc["timestamp"].isoformat())
#             data["open"].append(float(doc["open"]))
#             data["high"].append(float(doc["high"]))
#             data["low"].append(float(doc["low"]))
#             data["close"].append(float(doc["close"]))
#             data["volume"].append(int(doc["volume"]))
        
#         return data

#     def _combine_chunks_fast(self, chunks: List[Dict[str, List]]) -> Dict[str, List]:
#         """Fast chunk combination with pre-allocation"""
#         if not chunks:
#             return {
#                 "timestamp": [], "open": [], "high": [],
#                 "low": [], "close": [], "volume": []
#             }
        
#         total_length = sum(len(chunk["timestamp"]) for chunk in chunks)
        
#         result = {
#             "timestamp": [None] * total_length,
#             "open": [None] * total_length,
#             "high": [None] * total_length,
#             "low": [None] * total_length,
#             "close": [None] * total_length,
#             "volume": [None] * total_length
#         }
        
#         position = 0
#         for chunk in chunks:
#             chunk_length = len(chunk["timestamp"])
#             for field in result:
#                 result[field][position:position + chunk_length] = chunk[field]
#             position += chunk_length
        
#         return result


#     async def search_symbols(self, query: str = "") -> List[str]:
#         """Fast symbol search with caching"""
#         collections = await self.db.list_collection_names()
#         symbols = {
#             coll.rsplit('_', 1)[0] 
#             for coll in collections 
#             if coll.endswith("_1T") and (not query or query.lower() in coll.lower())
#         }
#         return sorted(list(symbols))

# def calculate_ema(data: pl.Series, period: int) -> pl.Series:
#     """Calculate EMA efficiently"""
#     return data.ewm_mean(span=period, adjust=False)

# def normalize_diff(data: pl.Series) -> pl.Series:
#     """Calculate normalized differences efficiently"""
#     return (data.pct_change().fill_null(0) + 1).shift(-1).drop_nulls()

# def calculate_similarity(a: pl.Series, b: pl.Series) -> float:
#     """Calculate similarity score efficiently"""
#     dot_product = (a * b).sum()
#     norm_a = (a ** 2).sum().sqrt()
#     norm_b = (b ** 2).sum().sqrt()
#     return float(dot_product / (norm_a * norm_b))

# def find_consecutive_patterns(
#     sample_data: pl.Series,
#     historical_data: pl.DataFrame,
#     ema_period: int = 14,
#     top_n: int = 5
# ) -> List[Dict[str, Any]]:
#     """Find patterns efficiently"""
#     if len(sample_data) < 2 or historical_data.is_empty():
#         return []
        
#     sample_ema = calculate_ema(sample_data, ema_period)
#     historical_ema = calculate_ema(historical_data['close'], ema_period)
    
#     input_size = len(sample_data)
#     sample_norm = normalize_diff(sample_data)
#     sample_ema_norm = normalize_diff(sample_ema)
    
#     similarities = []
#     historical_close = historical_data['close']
    
#     for i in range(len(historical_data) - input_size):
#         historical_segment = historical_close.slice(i, input_size)
#         historical_ema_segment = historical_ema.slice(i, input_size)
        
#         similarity = (
#             calculate_similarity(sample_norm, normalize_diff(historical_segment)) +
#             calculate_similarity(sample_ema_norm, normalize_diff(historical_ema_segment))
#         ) / 2
        
#         similarities.append((i, i + input_size - 1, similarity))
    
#     return sorted(similarities, key=lambda x: -x[2])[:top_n]

# import polars as pl
# from motor.motor_asyncio import AsyncIOMotorClient
# from datetime import datetime, timedelta
# import pytz
# from typing import List, Dict, Any, Tuple, Optional, Iterator
# from pymongo import ReplaceOne, ASCENDING, DESCENDING
# import asyncio
# import numpy as np
# from collections import deque
# from datetime import timezone
# import orjson
# from functools import lru_cache
# import io
# from concurrent.futures import ThreadPoolExecutor
# from itertools import islice


# class DataProcessor:
#     def __init__(self, db_url: str):
#         self.client = AsyncIOMotorClient(
#             db_url,
#             maxPoolSize=60,
#             minPoolSize=30,
#             serverSelectionTimeoutMS=2000,
#             connectTimeoutMS=2000,
#             maxIdleTimeMS=200000,
#             retryWrites=True,
#             w=1,
#             journal=False
#         )
#         self.db = self.client["Historical_data"]
#         self.chunk_size = 200000
#         self.upload_chunk_size = 100000
#         self.max_workers = 4
#         self.bulk_write_size = 100000
        
#         self.timeframes = {
#             '1T': timedelta(minutes=1),
#             '5T': timedelta(minutes=5),
#             '15T': timedelta(minutes=15),
#             '30T': timedelta(minutes=30),
#             '1H': timedelta(hours=1),
#             '4H': timedelta(hours=4),
#             '1D': timedelta(days=1),
#             '1W': timedelta(weeks=1),
#             '1M': timedelta(days=30)
#         }
        
#         self.polars_timeframes = {
#             '1T': '1m',
#             '5T': '5m',
#             '15T': '15m',
#             '30T': '30m',
#             '1H': '1h',
#             '4H': '4h',
#             '1D': '1d',
#             '1W': '1w',
#             '1M': '1mo'
#         }
#         self.index_cache = set()

#     @lru_cache(maxsize=100)
#     def _get_collection_name(self, symbol: str, timeframe: str) -> str:
#         """Cache collection names to avoid string operations"""
#         return f"{symbol}_{timeframe}"

#     async def _ensure_indexes(self, collection_name: str) -> None:
#         """Ensure minimal required indexes exist"""
#         if collection_name in self.index_cache:
#             return
#         await self.db[collection_name].create_index([
#             ("timestamp", ASCENDING),
#             ("symbol", ASCENDING)
#         ], unique=True, background=True)
#         self.index_cache.add(collection_name)

#     def _chunk_file_content(self, content: bytes, chunk_size: int) -> Iterator[List[str]]:
#         """Split file content into chunks efficiently"""
#         buffer = io.StringIO(content.decode('utf-8'))
#         while True:
#             chunk = list(islice(buffer, chunk_size))
#             if not chunk:
#                 break
#             yield chunk

#     def _parse_chunk(self, lines: List[str], symbol: str) -> List[Dict[str, Any]]:
#         """Parse a chunk of lines into data records"""
#         data = []
#         for line in lines:
#             if line.strip():
#                 try:
#                     parts = line.split(';')
#                     timestamp = datetime.strptime(
#                         parts[0], '%Y%m%d %H%M%S'
#                     ).replace(tzinfo=pytz.UTC)
#                     data.append({
#                         'timestamp': timestamp,
#                         'open': float(parts[1]),
#                         'high': float(parts[2]),
#                         'low': float(parts[3]),
#                         'close': float(parts[4]),
#                         'volume': int(parts[5]),
#                         'symbol': symbol
#                     })
#                 except (ValueError, IndexError) as e:
#                     print(f"Skipping invalid line: {line.strip()} - Error: {str(e)}")
#         return data

#     def _create_polars_frame(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
#         """Create a Polars DataFrame with proper column types"""
#         if not data:
#             return pl.DataFrame()
        
#         return pl.DataFrame(data).with_columns([
#             pl.col('timestamp').cast(pl.Datetime),
#             pl.col('open').cast(pl.Float64),
#             pl.col('high').cast(pl.Float64),
#             pl.col('low').cast(pl.Float64),
#             pl.col('close').cast(pl.Float64),
#             pl.col('volume').cast(pl.Int64),
#             pl.col('symbol').cast(pl.Categorical)
#         ])

#     async def _store_raw_data(self, data: List[Dict[str, Any]], symbol: str) -> None:
#         """Store raw 1-minute data without aggregation"""
#         collection_name = self._get_collection_name(symbol, '1T')
#         await self._ensure_indexes(collection_name)
        
#         bulk_operations = [
#             ReplaceOne(
#                 {"timestamp": item["timestamp"], "symbol": symbol},
#                 item,
#                 upsert=True
#             ) for item in data
#         ]
        
#         try:
#             collection = self.db[collection_name]
#             for i in range(0, len(bulk_operations), self.bulk_write_size):
#                 batch = bulk_operations[i:i + self.bulk_write_size]
#                 result = await collection.bulk_write(batch, ordered=False)
#                 print(f"Raw data batch {i//self.bulk_write_size + 1}: Inserted/Updated {result.upserted_count + result.modified_count} documents for {collection_name}")
#         except Exception as e:
#             print(f"Error storing raw data for {collection_name}: {str(e)}")

#     def aggregate_dataframe(self, df: pl.DataFrame, timeframe: str) -> pl.DataFrame:
#         """Aggregate data into specified timeframe"""
#         return df.group_by_dynamic(
#             index_column="timestamp",
#             every=timeframe,
#             by="symbol"
#         ).agg([
#             pl.col("open").first().alias("open"),
#             pl.col("high").max().alias("high"),
#             pl.col("low").min().alias("low"),
#             pl.col("close").last().alias("close"),
#             pl.col("volume").sum().alias("volume")
#         ]).drop_nulls()

#     async def _aggregate_and_store(
#         self,
#         df: pl.DataFrame,
#         symbol: str,
#         mongo_tf: str,
#         polars_tf: str
#     ) -> None:
#         """Aggregate and store data for a specific timeframe"""
#         collection_name = self._get_collection_name(symbol, mongo_tf)
#         await self._ensure_indexes(collection_name)
        
#         agg_df = self.aggregate_dataframe(df, polars_tf)
#         if agg_df.is_empty():
#             return

#         agg_data = agg_df.to_dicts()
        
#         for i in range(0, len(agg_data), self.bulk_write_size):
#             batch = agg_data[i:i + self.bulk_write_size]
#             bulk_operations = [
#                 ReplaceOne(
#                     {"timestamp": item["timestamp"], "symbol": symbol},
#                     item,
#                     upsert=True
#                 ) for item in batch
#             ]
            
#             try:
#                 collection = self.db[collection_name]
#                 result = await collection.bulk_write(bulk_operations, ordered=False)
#                 print(f"Batch {i//self.bulk_write_size + 1}: Inserted/Updated {result.upserted_count + result.modified_count} documents for {collection_name}")
#             except Exception as e:
#                 print(f"Error during bulk write for {collection_name} batch {i//self.bulk_write_size + 1}: {str(e)}")

#     async def _process_chunk_and_aggregate(
#         self,
#         chunk_data: List[Dict[str, Any]],
#         symbol: str,
#         semaphore: asyncio.Semaphore
#     ) -> None:
#         """Process aggregations for timeframes larger than 1 minute"""
#         if not chunk_data:
#             return

#         df = self._create_polars_frame(chunk_data)
#         if df.is_empty():
#             return

#         async with semaphore:
#             tasks = []
#             # Skip 1T timeframe as it's already stored in raw form
#             for mongo_tf, polars_tf in {k: v for k, v in self.polars_timeframes.items() if k != '1T'}.items():
#                 task = self._aggregate_and_store(df, symbol, mongo_tf, polars_tf)
#                 tasks.append(task)
            
#             await asyncio.gather(*tasks)

#     async def process_file(self, file: Any, symbol: str) -> None:
#         """Process a single file with chunking and parallel processing"""
#         content = await file.read()
        
#         semaphore = asyncio.Semaphore(self.max_workers)
#         tasks = []
        
#         with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
#             for lines in self._chunk_file_content(content, self.upload_chunk_size):
#                 chunk_data = await asyncio.get_event_loop().run_in_executor(
#                     executor,
#                     self._parse_chunk,
#                     lines,
#                     symbol
#                 )
                
#                 if chunk_data:
#                     # Store raw 1-minute data first
#                     await self._store_raw_data(chunk_data, symbol)
                    
#                     # Then process aggregations
#                     task = asyncio.create_task(
#                         self._process_chunk_and_aggregate(chunk_data, symbol, semaphore)
#                     )
#                     tasks.append(task)
                
#                 if len(tasks) >= self.max_workers * 2:
#                     completed, tasks = await asyncio.wait(
#                         tasks,
#                         return_when=asyncio.FIRST_COMPLETED
#                     )
        
#         if tasks:
#             await asyncio.gather(*tasks)

#     async def process_multiple_files(self, files: List[Any], symbol: str) -> Dict[str, Any]:
#         """Process multiple files with progress tracking"""
#         start_time = datetime.now()
#         total_files = len(files)
#         processed_files = 0
#         errors = []

#         async def process_with_progress(file):
#             try:
#                 await self.process_file(file, symbol)
#                 return True, None
#             except Exception as e:
#                 return False, str(e)

#         tasks = [process_with_progress(file) for file in files]
#         results = await asyncio.gather(*tasks, return_exceptions=True)

#         for i, (success, error) in enumerate(results):
#             if success:
#                 processed_files += 1
#             elif error:
#                 errors.append(f"Error processing file {i + 1}: {error}")

#         end_time = datetime.now()
#         processing_time = (end_time - start_time).total_seconds()

#         return {
#             "total_files": total_files,
#             "processed_files": processed_files,
#             "failed_files": total_files - processed_files,
#             "processing_time_seconds": processing_time,
#             "errors": errors
#         }

#     async def get_data(
#         self,
#         symbol: str,
#         start_date: datetime,
#         end_date: datetime,
#         timeframe: str
#     ) -> Dict[str, List]:
#         """Ultra-fast data retrieval with optimized parallel processing"""
#         collection_name = self._get_collection_name(symbol, timeframe)
#         await self._ensure_indexes(collection_name)
        
#         time_diff = (end_date - start_date).total_seconds()
#         chunk_duration = min(time_diff / 10, 86400)
#         num_chunks = max(1, min(10, int(time_diff / chunk_duration)))
        
#         time_chunks = []
#         chunk_size = time_diff / num_chunks
#         for i in range(num_chunks):
#             chunk_start = start_date + timedelta(seconds=i * chunk_size)
#             chunk_end = start_date + timedelta(seconds=(i + 1) * chunk_size)
#             time_chunks.append((chunk_start, chunk_end))

#         async with asyncio.TaskGroup() as tg:
#             tasks = [
#                 tg.create_task(self._fetch_chunk_fast(collection_name, start, end))
#                 for start, end in time_chunks
#             ]
        
#         chunks = [task.result() for task in tasks]
#         return self._combine_chunks_fast(chunks)

#     async def _fetch_chunk_fast(
#         self,
#         collection_name: str,
#         start_date: datetime,
#         end_date: datetime
#     ) -> Dict[str, List]:
#         """Optimized chunk retrieval with pre-allocated memory"""
#         collection = self.db[collection_name]
        
#         projection = {
#             "_id": 0,
#             "timestamp": 1,
#             "open": 1,
#             "high": 1,
#             "low": 1,
#             "close": 1,
#             "volume": 1
#         }
        
#         estimated_size = int((end_date - start_date).total_seconds() / 60) + 100
#         data = {
#             "timestamp": [] * estimated_size,
#             "open": [] * estimated_size,
#             "high": [] * estimated_size,
#             "low": [] * estimated_size,
#             "close": [] * estimated_size,
#             "volume": [] * estimated_size
#         }
        
#         cursor = collection.find(
#             {
#                 "timestamp": {
#                     "$gte": start_date,
#                     "$lt": end_date
#                 }
#             },
#             projection,
#             batch_size=10000
#         ).sort("timestamp", 1)
        
#         async for doc in cursor:
#             data["timestamp"].append(doc["timestamp"].isoformat())
#             data["open"].append(float(doc["open"]))
#             data["high"].append(float(doc["high"]))
#             data["low"].append(float(doc["low"]))
#             data["close"].append(float(doc["close"]))
#             data["volume"].append(int(doc["volume"]))
        
#         return data

#     def _combine_chunks_fast(self, chunks: List[Dict[str, List]]) -> Dict[str, List]:
#         """Fast chunk combination with pre-allocation"""
#         if not chunks:
#             return {
#                 "timestamp": [], "open": [], "high": [],
#                 "low": [], "close": [], "volume": []
#             }
        
#         total_length = sum(len(chunk["timestamp"]) for chunk in chunks)
        
#         result = {
#             "timestamp": [None] * total_length,
#             "open": [None] * total_length,
#             "high": [None] * total_length,
#             "low": [None] * total_length,
#             "close": [None] * total_length,
#             "volume": [None] * total_length
#         }
        
#         position = 0
#         for chunk in chunks:
#             chunk_length = len(chunk["timestamp"])
#             for field in result:
#                 result[field][position:position + chunk_length] = chunk[field]
#             position += chunk_length
        
#         return result


#     async def search_symbols(self, query: str = "") -> List[str]:
#         """Fast symbol search with caching"""
#         collections = await self.db.list_collection_names()
#         symbols = {
#             coll.rsplit('_', 1)[0] 
#             for coll in collections 
#             if coll.endswith("_1T") and (not query or query.lower() in coll.lower())
#         }
#         return sorted(list(symbols))


# def calculate_ema(data: pl.Series, period: int) -> pl.Series:
#     """Calculate EMA efficiently"""
#     return data.ewm_mean(span=period, adjust=False)

# def normalize_diff(data: pl.Series) -> pl.Series:
#     """Calculate normalized differences efficiently"""
#     return (data.pct_change().fill_null(0) + 1).shift(-1).fill_null(1)

# def calculate_similarity(a: pl.Series, b: pl.Series) -> float:
#     """Calculate similarity score efficiently using vector operations"""
#     if len(a) != len(b):
#         return 0.0
    
#     # Convert to numpy arrays for efficient computation
#     a_np = a.to_numpy()
#     b_np = b.to_numpy()
    
#     # Calculate dot product
#     dot_product = np.sum(a_np * b_np)
    
#     # Calculate norms
#     norm_a = np.sqrt(np.sum(a_np * a_np))
#     norm_b = np.sqrt(np.sum(b_np * b_np))
    
#     if norm_a == 0 or norm_b == 0:
#         return 0.0
    
#     return float(dot_product / (norm_a * norm_b))

# def find_consecutive_patterns(
#     pattern_df: pl.DataFrame,
#     historical_df: pl.DataFrame,
#     ema_period: int = 14,
#     top_n: int = 5
# ) -> List[Tuple[int, int, float]]:
#     """
#     Find patterns in historical data based on a pattern defined by start and end dates.
    
#     Args:
#         pattern_df: DataFrame containing the pattern to search for
#         historical_df: DataFrame containing the historical data to search in
#         ema_period: Period for EMA calculation
#         top_n: Number of top matches to return
    
#     Returns:
#         List of tuples containing (start_index, end_index, similarity_score)
#     """
#     if len(pattern_df) < 2 or historical_df.is_empty():
#         return []
    
#     # Extract closing prices
#     pattern_close = pattern_df["close"]
#     historical_close = historical_df["close"]
    
#     # Calculate EMAs
#     pattern_ema = calculate_ema(pattern_close, ema_period)
#     historical_ema = calculate_ema(historical_close, ema_period)
    
#     # Get pattern size
#     pattern_size = len(pattern_df)
    
#     # Normalize pattern data
#     pattern_norm = normalize_diff(pattern_close)
#     pattern_ema_norm = normalize_diff(pattern_ema)
    
#     # Pre-calculate historical normalized differences
#     historical_norm = normalize_diff(historical_close)
#     historical_ema_norm = normalize_diff(historical_ema)
    
#     # Find similarities
#     similarities = []
#     for i in range(len(historical_df) - pattern_size + 1):
#         # Get historical segments
#         historical_segment = historical_norm.slice(i, pattern_size)
#         historical_ema_segment = historical_ema_norm.slice(i, pattern_size)
        
#         # Calculate similarity scores
#         try:
#             price_similarity = calculate_similarity(
#                 pattern_norm,
#                 historical_segment
#             )
            
#             ema_similarity = calculate_similarity(
#                 pattern_ema_norm,
#                 historical_ema_segment
#             )
            
#             # Average the similarities
#             combined_similarity = (price_similarity + ema_similarity) / 2
            
#             similarities.append((i, i + pattern_size - 1, combined_similarity))
#         except Exception as e:
#             print(f"Error calculating similarity at index {i}: {str(e)}")
#             continue
    
#     # Sort by similarity score and return top matches
#     return sorted(similarities, key=lambda x: -x[2])[:top_n]




import polars as pl
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timedelta
import pytz
from typing import List, Dict, Any, Tuple, Optional, Iterator
from pymongo import ReplaceOne, ASCENDING, DESCENDING
import asyncio
import numpy as np
from collections import deque
from datetime import timezone
import orjson
from functools import lru_cache
import io
from concurrent.futures import ThreadPoolExecutor
from itertools import islice

class DataProcessor:
    def __init__(self, db_url: str):
        self.client = AsyncIOMotorClient(
            db_url,
            maxPoolSize=60,
            minPoolSize=30,
            serverSelectionTimeoutMS=2000,
            connectTimeoutMS=2000,
            maxIdleTimeMS=200000,
            retryWrites=True,
            w=1,
            journal=False
        )
        self.db = self.client["Historical_data"]
        self.categories_collection = self.db["categories"]
        self.chunk_size = 200000
        self.upload_chunk_size = 100000
        self.max_workers = 4
        self.bulk_write_size = 100000
        
        self.timeframes = {
            '1T': timedelta(minutes=1),
            '5T': timedelta(minutes=5),
            '15T': timedelta(minutes=15),
            '30T': timedelta(minutes=30),
            '1H': timedelta(hours=1),
            '4H': timedelta(hours=4),
            '1D': timedelta(days=1),
            '1W': timedelta(weeks=1),
            '1M': timedelta(days=30)
        }
        
        self.polars_timeframes = {
            '1T': '1m',
            '5T': '5m',
            '15T': '15m',
            '30T': '30m',
            '1H': '1h',
            '4H': '4h',
            '1D': '1d',
            '1W': '1w',
            '1M': '1mo'
        }
        self.index_cache = set()

    async def initialize(self):
        """Initialize required indexes for categories"""
        await self.categories_collection.create_index([("category", ASCENDING)], unique=True)
        await self.categories_collection.create_index([
            ("category", ASCENDING),
            ("symbols.symbol", ASCENDING)
        ])

    def _chunk_file_content(self, content: bytes, chunk_size: int) -> Iterator[List[str]]:
        """Split file content into chunks efficiently"""
        buffer = io.StringIO(content.decode('utf-8'))
        while True:
            chunk = list(islice(buffer, chunk_size))
            if not chunk:
                break
            yield chunk

    def _parse_chunk(self, lines: List[str], symbol: str) -> List[Dict[str, Any]]:
        """Parse a chunk of lines into data records"""
        data = []
        for line in lines:
            if line.strip():
                try:
                    parts = line.split(';')
                    timestamp = datetime.strptime(
                        parts[0], '%Y%m%d %H%M%S'
                    ).replace(tzinfo=pytz.UTC)
                    data.append({
                        'timestamp': timestamp,
                        'open': float(parts[1]),
                        'high': float(parts[2]),
                        'low': float(parts[3]),
                        'close': float(parts[4]),
                        'volume': int(parts[5]),
                        'symbol': symbol
                    })
                except (ValueError, IndexError) as e:
                    print(f"Skipping invalid line: {line.strip()} - Error: {str(e)}")
        return data

    @lru_cache(maxsize=100)
    def _get_collection_name(self, category: str, symbol: str, timeframe: str) -> str:
        """Cache collection names to avoid string operations"""
        return f"{category}_{symbol}_{timeframe}"

    async def _ensure_indexes(self, collection_name: str) -> None:
        """Ensure minimal required indexes exist"""
        if collection_name in self.index_cache:
            return
        await self.db[collection_name].create_index([
            ("timestamp", ASCENDING),
            ("symbol", ASCENDING)
        ], unique=True, background=True)
        self.index_cache.add(collection_name)

    def _create_polars_frame(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """Create a Polars DataFrame with proper column types"""
        if not data:
            return pl.DataFrame()
        
        return pl.DataFrame(data).with_columns([
            pl.col('timestamp').cast(pl.Datetime),
            pl.col('open').cast(pl.Float64),
            pl.col('high').cast(pl.Float64),
            pl.col('low').cast(pl.Float64),
            pl.col('close').cast(pl.Float64),
            pl.col('volume').cast(pl.Int64),
            pl.col('symbol').cast(pl.Categorical)
        ])

    async def _store_raw_data(self, data: List[Dict[str, Any]], category: str, symbol: str) -> None:
        """Store raw 1-minute data without aggregation"""
        collection_name = self._get_collection_name(category, symbol, '1T')
        await self._ensure_indexes(collection_name)
        
        bulk_operations = [
            ReplaceOne(
                {"timestamp": item["timestamp"], "symbol": symbol},
                item,
                upsert=True
            ) for item in data
        ]
        
        try:
            collection = self.db[collection_name]
            for i in range(0, len(bulk_operations), self.bulk_write_size):
                batch = bulk_operations[i:i + self.bulk_write_size]
                result = await collection.bulk_write(batch, ordered=False)
                print(f"Raw data batch {i//self.bulk_write_size + 1}: Inserted/Updated {result.upserted_count + result.modified_count} documents for {collection_name}")
        except Exception as e:
            print(f"Error storing raw data for {collection_name}: {str(e)}")

    def aggregate_dataframe(self, df: pl.DataFrame, timeframe: str) -> pl.DataFrame:
        """Aggregate data into specified timeframe"""
        return df.group_by_dynamic(
            index_column="timestamp",
            every=timeframe,
            by="symbol"
        ).agg([
            pl.col("open").first().alias("open"),
            pl.col("high").max().alias("high"),
            pl.col("low").min().alias("low"),
            pl.col("close").last().alias("close"),
            pl.col("volume").sum().alias("volume")
        ]).drop_nulls()

    async def _aggregate_and_store(
        self,
        df: pl.DataFrame,
        category: str,
        symbol: str,
        mongo_tf: str,
        polars_tf: str
    ) -> None:
        """Aggregate and store data for a specific timeframe"""
        collection_name = self._get_collection_name(category, symbol, mongo_tf)
        await self._ensure_indexes(collection_name)
        
        agg_df = self.aggregate_dataframe(df, polars_tf)
        if agg_df.is_empty():
            return

        agg_data = agg_df.to_dicts()
        
        for i in range(0, len(agg_data), self.bulk_write_size):
            batch = agg_data[i:i + self.bulk_write_size]
            bulk_operations = [
                ReplaceOne(
                    {"timestamp": item["timestamp"], "symbol": symbol},
                    item,
                    upsert=True
                ) for item in batch
            ]
            
            try:
                collection = self.db[collection_name]
                result = await collection.bulk_write(bulk_operations, ordered=False)
                print(f"Batch {i//self.bulk_write_size + 1}: Inserted/Updated {result.upserted_count + result.modified_count} documents for {collection_name}")
            except Exception as e:
                print(f"Error during bulk write for {collection_name} batch {i//self.bulk_write_size + 1}: {str(e)}")

    async def _process_chunk_and_aggregate(
        self,
        chunk_data: List[Dict[str, Any]],
        category: str,
        symbol: str,
        semaphore: asyncio.Semaphore
    ) -> None:
        """Process aggregations for timeframes larger than 1 minute"""
        if not chunk_data:
            return

        df = self._create_polars_frame(chunk_data)
        if df.is_empty():
            return

        async with semaphore:
            tasks = []
            # Skip 1T timeframe as it's already stored in raw form
            for mongo_tf, polars_tf in {k: v for k, v in self.polars_timeframes.items() if k != '1T'}.items():
                task = self._aggregate_and_store(df, category, symbol, mongo_tf, polars_tf)
                tasks.append(task)
            
            await asyncio.gather(*tasks)

    async def process_file(self, file: Any, category: str, symbol: str) -> None:
        """Process a single file with chunking and parallel processing"""
        content = await file.read()
        
        semaphore = asyncio.Semaphore(self.max_workers)
        tasks = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for lines in self._chunk_file_content(content, self.upload_chunk_size):
                chunk_data = await asyncio.get_event_loop().run_in_executor(
                    executor,
                    self._parse_chunk,
                    lines,
                    symbol
                )
                
                if chunk_data:
                    # Store raw 1-minute data first
                    await self._store_raw_data(chunk_data, category, symbol)
                    
                    # Then process aggregations
                    task = asyncio.create_task(
                        self._process_chunk_and_aggregate(chunk_data, category, symbol, semaphore)
                    )
                    tasks.append(task)
                
                if len(tasks) >= self.max_workers * 2:
                    completed, tasks = await asyncio.wait(
                        tasks,
                        return_when=asyncio.FIRST_COMPLETED
                    )
        
        if tasks:
            await asyncio.gather(*tasks)

    # Category management methods
    async def add_category(self, category: str) -> Dict[str, Any]:
        """Add a new category"""
        try:
            await self.categories_collection.insert_one({
                "category": category,
                "symbols": [],
                "created_at": datetime.utcnow()
            })
            return {"status": "success", "message": f"Category {category} created successfully"}
        except Exception as e:
            if "duplicate key error" in str(e).lower():
                return {"status": "error", "message": f"Category {category} already exists"}
            raise

    async def delete_category(self, category: str) -> Dict[str, Any]:
        """Delete a category and all its associated data"""
        category_doc = await self.categories_collection.find_one({"category": category})
        if not category_doc:
            return {"status": "error", "message": f"Category {category} not found"}

        # Delete all collections for this category's symbols
        for symbol_info in category_doc["symbols"]:
            symbol = symbol_info["symbol"]
            for timeframe in self.timeframes.keys():
                collection_name = self._get_collection_name(category, symbol, timeframe)
                await self.db[collection_name].drop()

        # Delete the category itself
        await self.categories_collection.delete_one({"category": category})
        return {"status": "success", "message": f"Category {category} and all its data deleted"}

    async def add_symbol_to_category(self, category: str, symbol: str) -> Dict[str, Any]:
        """Add a symbol to a category"""
        result = await self.categories_collection.update_one(
            {"category": category},
            {"$addToSet": {"symbols": {
                "symbol": symbol,
                "added_at": datetime.utcnow()
            }}}
        )
        if result.matched_count == 0:
            return {"status": "error", "message": f"Category {category} not found"}
        return {"status": "success", "message": f"Symbol {symbol} added to category {category}"}

    async def remove_symbol_from_category(self, category: str, symbol: str) -> Dict[str, Any]:
        """Remove a symbol from a category"""
        result = await self.categories_collection.update_one(
            {"category": category},
            {"$pull": {"symbols": {"symbol": symbol}}}
        )
        if result.matched_count == 0:
            return {"status": "error", "message": f"Category {category} not found"}
        
        # Drop all collections for this symbol
        for timeframe in self.timeframes.keys():
            collection_name = self._get_collection_name(category, symbol, timeframe)
            await self.db[collection_name].drop()
            
        return {"status": "success", "message": f"Symbol {symbol} removed from category {category}"}

    async def list_categories(self, query: str = "") -> List[Dict[str, Any]]:
        """List all categories with optional search"""
        filter_query = {}
        if query:
            filter_query = {"category": {"$regex": query, "$options": "i"}}
        
        categories = await self.categories_collection.find(
            filter_query,
            {"_id": 0}
        ).to_list(None)
        
        return categories

    async def search_symbols_in_category(self, category: str, query: str = "") -> List[str]:
        """Search symbols within a specific category"""
        category_doc = await self.categories_collection.find_one(
            {"category": category},
            {"_id": 0, "symbols": 1}
        )
        
        if not category_doc:
            return []
            
        symbols = [
            s["symbol"] for s in category_doc["symbols"]
            if not query or query.lower() in s["symbol"].lower()
        ]
        return sorted(symbols)

    async def process_multiple_files(
        self,
        files: List[Any],
        category: str,
        symbol: str
    ) -> Dict[str, Any]:
        """Process multiple files with progress tracking"""
        # First, ensure the category exists and the symbol is added
        await self.add_category(category)
        await self.add_symbol_to_category(category, symbol)
        
        start_time = datetime.now()
        total_files = len(files)
        processed_files = 0
        errors = []

        async def process_with_progress(file):
            try:
                await self.process_file(file, category, symbol)
                return True, None
            except Exception as e:
                return False, str(e)

        tasks = [process_with_progress(file) for file in files]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, (success, error) in enumerate(results):
            if success:
                processed_files += 1
            elif error:
                errors.append(f"Error processing file {i + 1}: {error}")

        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()

        return {
            "total_files": total_files,
            "processed_files": processed_files,
            "failed_files": total_files - processed_files,
            "processing_time_seconds": processing_time,
            "errors": errors
        }

    async def get_data(
        self,
        category: str,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        timeframe: str
    ) -> Dict[str, List]:
        """Ultra-fast data retrieval with optimized parallel processing"""
        # Verify category and symbol exist
        category_doc = await self.categories_collection.find_one({
            "category": category,
            "symbols.symbol": symbol
        })
        
        if not category_doc:
            raise ValueError(f"Symbol {symbol} not found in category {category}")
        
        collection_name = self._get_collection_name(category, symbol, timeframe)
        await self._ensure_indexes(collection_name)
        
        # Calculate optimal chunk size
        time_diff = (end_date - start_date).total_seconds()
        chunk_duration = min(time_diff / 10, 86400)  # Max 1 day per chunk
        num_chunks = max(1, min(10, int(time_diff / chunk_duration)))
        
        # Create time chunks
        time_chunks = []
        chunk_size = time_diff / num_chunks
        for i in range(num_chunks):
            chunk_start = start_date + timedelta(seconds=i * chunk_size)
            chunk_end = start_date + timedelta(seconds=(i + 1) * chunk_size)
            if i == num_chunks - 1:
                chunk_end = end_date  # Ensure we cover the entire range
            time_chunks.append((chunk_start, chunk_end))
        
        # Process chunks in parallel using TaskGroup
        try:
            async with asyncio.TaskGroup() as tg:
                tasks = [
                    tg.create_task(self._fetch_chunk_fast(collection_name, start, end))
                    for start, end in time_chunks
                ]
            chunks = [task.result() for task in tasks]
        except AttributeError:
            # Fallback for Python versions without TaskGroup
            tasks = [
                self._fetch_chunk_fast(collection_name, start, end)
                for start, end in time_chunks
            ]
            chunks = await asyncio.gather(*tasks)
        
        return self._combine_chunks_fast(chunks)

    async def _fetch_chunk_fast(
        self,
        collection_name: str,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, List]:
        """Optimized chunk retrieval with pre-allocated memory"""
        collection = self.db[collection_name]
        
        projection = {
            "_id": 0,
            "timestamp": 1,
            "open": 1,
            "high": 1,
            "low": 1,
            "close": 1,
            "volume": 1
        }
        
        # Pre-allocate arrays with estimated size
        estimated_size = int((end_date - start_date).total_seconds() / 60) + 100
        data = {
            "timestamp": [None] * estimated_size,
            "open": [None] * estimated_size,
            "high": [None] * estimated_size,
            "low": [None] * estimated_size,
            "close": [None] * estimated_size,
            "volume": [None] * estimated_size
        }
        
        # Use raw command for better performance
        pipeline = [
            {
                "$match": {
                    "timestamp": {
                        "$gte": start_date,
                        "$lt": end_date
                    }
                }
            },
            {
                "$sort": {"timestamp": 1}
            },
            {
                "$project": projection
            }
        ]
        
        idx = 0
        async for doc in collection.aggregate(pipeline, batchSize=10000):
            for field in data:
                if field == "timestamp":
                    data[field][idx] = doc[field].isoformat()
                elif field == "volume":
                    data[field][idx] = int(doc[field])
                else:
                    data[field][idx] = float(doc[field])
            idx += 1
        
        # Trim arrays to actual size
        for field in data:
            data[field] = data[field][:idx]
        
        return data

    def _combine_chunks_fast(self, chunks: List[Dict[str, List]]) -> Dict[str, List]:
        """Fast chunk combination with pre-allocation"""
        if not chunks:
            return {
                "timestamp": [],
                "open": [],
                "high": [],
                "low": [],
                "close": [],
                "volume": []
            }
        
        # Calculate total length for pre-allocation
        total_length = sum(len(chunk["timestamp"]) for chunk in chunks)
        
        # Pre-allocate result arrays
        result = {
            "timestamp": [None] * total_length,
            "open": [None] * total_length,
            "high": [None] * total_length,
            "low": [None] * total_length,
            "close": [None] * total_length,
            "volume": [None] * total_length
        }
        
        # Fast chunk combination
        position = 0
        for chunk in chunks:
            chunk_length = len(chunk["timestamp"])
            for field in result:
                result[field][position:position + chunk_length] = chunk[field]
            position += chunk_length
        
        return result

    async def search_symbols(self, query: str = "") -> List[str]:
        """Fast symbol search with caching"""
        collections = await self.db.list_collection_names()
        symbols = {
            coll.rsplit('_', 1)[0] 
            for coll in collections 
            if coll.endswith("_1T") and (not query or query.lower() in coll.lower())
        }
        return sorted(list(symbols))

def calculate_ema(data: pl.Series, period: int) -> pl.Series:
    """Calculate EMA efficiently"""
    return data.ewm_mean(span=period, adjust=False)

def normalize_diff(data: pl.Series) -> pl.Series:
    """Calculate normalized differences efficiently"""
    return (data.pct_change().fill_null(0) + 1).shift(-1).drop_nulls()

def calculate_similarity(a: pl.Series, b: pl.Series) -> float:
    """Calculate similarity score efficiently using numpy"""
    dot_product = (a * b).sum()
    # Use numpy.sqrt instead of calling sqrt directly on float
    norm_a = np.sqrt((a ** 2).sum())
    norm_b = np.sqrt((b ** 2).sum())
    # Avoid division by zero
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(dot_product / (norm_a * norm_b))

def find_consecutive_patterns(
    sample_data: pl.Series,
    historical_data: pl.DataFrame,
    ema_period: int = 14,
    top_n: int = 5
) -> List[Tuple[int, int, float]]:
    """Find patterns efficiently"""
    if len(sample_data) < 2 or historical_data.is_empty():
        return []
        
    sample_ema = calculate_ema(sample_data, ema_period)
    historical_ema = calculate_ema(historical_data['close'], ema_period)
    
    input_size = len(sample_data)
    sample_norm = normalize_diff(sample_data)
    sample_ema_norm = normalize_diff(sample_ema)
    
    similarities = []
    historical_close = historical_data['close']
    
    # Add error handling for the pattern matching process
    try:
        for i in range(len(historical_data) - input_size):
            historical_segment = historical_close.slice(i, input_size)
            historical_ema_segment = historical_ema.slice(i, input_size)
            
            # Add error handling for similarity calculation
            try:
                price_similarity = calculate_similarity(sample_norm, normalize_diff(historical_segment))
                ema_similarity = calculate_similarity(sample_ema_norm, normalize_diff(historical_ema_segment))
                overall_similarity = (price_similarity + ema_similarity) / 2
                
                similarities.append((i, i + input_size - 1, overall_similarity))
            except Exception as e:
                print(f"Warning: Error calculating similarity at index {i}: {str(e)}")
                continue
                
        # Sort by similarity score in descending order and return top N
        return sorted(similarities, key=lambda x: -x[2])[:top_n]
        
    except Exception as e:
        print(f"Error in pattern matching process: {str(e)}")
        return []