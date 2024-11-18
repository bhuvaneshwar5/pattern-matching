import polars as pl
import pandas as pd
from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
import pytz
from typing import List, Dict, Any
from pymongo import UpdateOne
import asyncio

import polars as pl
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timedelta
import pytz
from typing import List, Dict, Any
from pymongo import ReplaceOne
import asyncio

class uploadDataProcessor:
    def __init__(self, db_url: str):
        self.client = AsyncIOMotorClient(db_url)
        self.db = self.client["Historical_data"]
        self.timeframes = {
            '1T': '1m', '5T': '5m', '15T': '15m', '30T': '30m',
            '1H': '1h', '4H': '4h', '1D': '1d', '1W': '1w',
            '1M': '1mo', '1Y': '1y', '5Y': '5y'
        }

    async def process_file(self, file: Any, symbol: str) -> None:
        content = await file.read()
        lines = content.decode('utf-8').split('\n')
        data = []
        for line in lines:
            if line.strip():
                parts = line.split(';')
                timestamp = datetime.strptime(parts[0], '%Y%m%d %H%M%S').replace(tzinfo=pytz.UTC)
                data.append({
                    'timestamp': timestamp,
                    'open': float(parts[1]),
                    'high': float(parts[2]),
                    'low': float(parts[3]),
                    'close': float(parts[4]),
                    'volume': int(parts[5]),
                    'symbol': symbol
                })
        
        if data:
            await self.insert_and_aggregate_data(symbol, data)

    async def process_multiple_files(self, files: List[Any], symbol: str) -> None:
        tasks = [self.process_file(file, symbol) for file in files]
        await asyncio.gather(*tasks)

    async def insert_and_aggregate_data(self, symbol: str, data: List[Dict[str, Any]]) -> None:
        df = pl.DataFrame(data)
        df = df.with_columns([
            pl.col('timestamp').cast(pl.Datetime),
            pl.col('symbol').cast(pl.Categorical)
        ])
        
        for mongo_tf, polars_tf in self.timeframes.items():
            collection = self.db[f"{symbol}_{mongo_tf}"]
            
            # Create a compound index on timestamp and symbol for faster queries
            await collection.create_index([("timestamp", 1), ("symbol", 1)], unique=True)
            
            # Aggregate data based on timeframe using Polars
            agg_df = self.aggregate_dataframe(df, polars_tf)
            
            # Convert aggregated data to list of dicts
            agg_data = agg_df.to_dicts()
            
            # Use ReplaceOne with upsert=True to handle duplicates
            bulk_operations = [
                ReplaceOne(
                    {"timestamp": item["timestamp"], "symbol": symbol},
                    item,
                    upsert=True
                ) for item in agg_data
            ]
            
            try:
                result = await collection.bulk_write(bulk_operations, ordered=False)
                print(f"Inserted/Updated {result.upserted_count + result.modified_count} documents for {symbol}_{mongo_tf}")
            except Exception as e:
                print(f"Error during bulk write for {symbol}_{mongo_tf}: {str(e)}")

    def aggregate_dataframe(self, df: pl.DataFrame, timeframe: str) -> pl.DataFrame:
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

    async def get_data(self, symbol: str, start_date: datetime, end_date: datetime, timeframe: str) -> pl.DataFrame:
        collection = self.db[f"{symbol}_{timeframe}"]
        query = {
            "timestamp": {
                "$gte": start_date,
                "$lte": end_date
            },
            "symbol": symbol
        }
        
        cursor = collection.find(query).sort("timestamp", 1)
        data = await cursor.to_list(length=None)
        
        if not data:
            return pl.DataFrame()
        
        # Convert to Polars DataFrame
        df = pl.DataFrame(data)
        df = df.with_columns([
            pl.col('timestamp').cast(pl.Datetime),
            pl.col('open').cast(pl.Float64),
            pl.col('high').cast(pl.Float64),
            pl.col('low').cast(pl.Float64),
            pl.col('close').cast(pl.Float64),
            pl.col('volume').cast(pl.Int64)
        ]).drop('_id')
        
        return df

    async def list_timeframes(self, symbol: str) -> List[str]:
        collections = await self.db.list_collection_names()
        return [coll.split('_')[1] for coll in collections if coll.startswith(f"{symbol}_")]