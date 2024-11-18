# # from fastapi import FastAPI, File, UploadFile, HTTPException, Query
# # from fastapi.middleware.cors import CORSMiddleware
# # from pydantic import BaseModel, Field
# # from datetime import datetime
# # import polars as pl
# # from data_proessor import DataProcessor, find_consecutive_patterns, calculate_ema
# # from typing import List, Dict, Optional

# # app = FastAPI()

# # # Add CORS middleware
# # app.add_middleware(
# #     CORSMiddleware,
# #     allow_origins=["*"],
# #     allow_credentials=True,
# #     allow_methods=["*"],
# #     allow_headers=["*"],
# # )

# # data_processor = DataProcessor("mongodb://localhost:27017")

# # class PatternSearchRequest(BaseModel):
# #     symbol: str = Field(..., description="Trading symbol to search patterns for")
# #     start_date: datetime = Field(..., description="Start date for historical data")
# #     end_date: datetime = Field(..., description="End date for historical data")
# #     sample_data: List[float] = Field(..., description="Sample price data to find patterns for")
# #     ema_period: int = Field(default=14, description="Period for EMA calculation")
# #     include_volume: bool = Field(default=False, description="Whether to include volume in pattern matching")

# # @app.post("/upload/{symbol}")
# # async def upload_files(
# #     symbol: str,
# #     files: List[UploadFile] = File(...),
# #     chunk_size: Optional[int] = Query(50000, description="Number of lines to process in each chunk"),
# #     max_workers: Optional[int] = Query(4, description="Maximum number of parallel workers")
# # ):
# #     try:
# #         # Update processor settings if provided
# #         data_processor.upload_chunk_size = chunk_size
# #         data_processor.max_workers = max_workers

# #         # Process files with progress tracking
# #         result = await data_processor.process_multiple_files(files, symbol)
        
# #         return {
# #             "status": "completed",
# #             "details": result,
# #             "message": f"Processed {result['processed_files']} out of {result['total_files']} files in {result['processing_time_seconds']:.2f} seconds"
# #         }
# #     except Exception as e:
# #         raise HTTPException(
# #             status_code=500,
# #             detail={
# #                 "status": "error",
# #                 "message": f"An error occurred during file processing: {str(e)}"
# #             }
# #         )

# # @app.get("/historical_data/{symbol}")
# # async def get_historical_data(
# #     symbol: str,
# #     start_date: datetime = Query(...),
# #     end_date: datetime = Query(...),
# #     timeframe: str = Query("1T", regex="^(1T|5T|15T|30T|1H|4H|1D|1W|1M)$")
# # ):
# #     try:
# #         data = await data_processor.get_data(symbol, start_date, end_date, timeframe)
        
# #         if not data or not any(len(v) for v in data.values()):
# #             raise HTTPException(status_code=404, detail="No data found for this symbol and date range")
        
# #         return {"data": data}
        
# #     except Exception as e:
# #         raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

# # @app.post("/search_patterns/")
# # async def search_patterns(request: PatternSearchRequest):
# #     historical_data = await data_processor.get_data(request.symbol, request.start_date, request.end_date, "1T")
    
# #     if historical_data.is_empty():
# #         raise HTTPException(status_code=404, detail="No data found for this symbol and date range")

# #     sample_data = pl.Series(request.sample_data)
# #     sample_ema = calculate_ema(sample_data, request.ema_period)
# #     historical_ema = calculate_ema(historical_data['close'], request.ema_period)

# #     sample_volume = None
# #     historical_volume = None
# #     if request.include_volume:
# #         sample_volume = pl.Series(pl.random.normal(len(sample_data)))
# #         historical_volume = historical_data['volume']

# #     top_matches = find_consecutive_patterns(sample_data, sample_ema,
# #                                             historical_data, historical_ema,
# #                                             sample_volume, historical_volume,
# #                                             request.include_volume)

# #     results = [
# #         {
# #             "start_date": historical_data['timestamp'][start].to_datetime().isoformat(),
# #             "end_date": historical_data['timestamp'][end].to_datetime().isoformat(),
# #             "similarity": float(similarity)
# #         }
# #         for start, end, similarity in top_matches
# #     ]

# #     return results

# # @app.get("/search_symbols")
# # async def search_symbols(query: str = ""):
# #     try:
# #         symbols = await data_processor.search_symbols(query)
# #         return {"symbols": symbols}
# #     except Exception as e:
# #         raise HTTPException(status_code=500, detail=str(e))

# # if __name__ == "__main__":
# #     import uvicorn
# #     uvicorn.run(app, host="0.0.0.0", port=8000)



# from fastapi import FastAPI, File, UploadFile, HTTPException, Query
# from fastapi.middleware.cors import CORSMiddleware
# from pydantic import BaseModel, Field
# from datetime import datetime
# import polars as pl
# from data_proessor import DataProcessor, find_consecutive_patterns, calculate_ema
# from typing import List, Dict, Optional

# app = FastAPI()

# # Add CORS middleware
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# data_processor = DataProcessor("mongodb://localhost:27017")

# class PatternSearchRequest(BaseModel):
#     symbol: str = Field(..., description="Trading symbol to search patterns for")
#     start_date: datetime = Field(..., description="Start date for historical data")
#     end_date: datetime = Field(..., description="End date for historical data")
#     sample_data: List[float] = Field(..., description="Sample price data to find patterns for")
#     ema_period: int = Field(default=14, description="Period for EMA calculation")
#     include_volume: bool = Field(default=False, description="Whether to include volume in pattern matching")

# @app.post("/upload/{symbol}")
# async def upload_files(
#     symbol: str,
#     files: List[UploadFile] = File(...),
#     chunk_size: Optional[int] = Query(50000, description="Number of lines to process in each chunk"),
#     max_workers: Optional[int] = Query(4, description="Maximum number of parallel workers")
# ):
#     try:
#         # Update processor settings if provided
#         data_processor.upload_chunk_size = chunk_size
#         data_processor.max_workers = max_workers

#         # Process files with progress tracking
#         result = await data_processor.process_multiple_files(files, symbol)
        
#         return {
#             "status": "completed",
#             "details": result,
#             "message": f"Processed {result['processed_files']} out of {result['total_files']} files in {result['processing_time_seconds']:.2f} seconds"
#         }
#     except Exception as e:
#         raise HTTPException(
#             status_code=500,
#             detail={
#                 "status": "error",
#                 "message": f"An error occurred during file processing: {str(e)}"
#             }
#         )

# @app.get("/historical_data/{symbol}")
# async def get_historical_data(
#     symbol: str,
#     start_date: datetime = Query(...),
#     end_date: datetime = Query(...),
#     timeframe: str = Query("1T", regex="^(1T|5T|15T|30T|1H|4H|1D|1W|1M)$")
# ):
#     try:
#         data = await data_processor.get_data(symbol, start_date, end_date, timeframe)
        
#         if not data or not any(len(v) for v in data.values()):
#             raise HTTPException(status_code=404, detail="No data found for this symbol and date range")
        
#         return {"data": data}
        
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

# class PatternSearchRequest(BaseModel):
#     symbol: str = Field(..., description="Trading symbol to search patterns for")
#     pattern_start: datetime = Field(..., description="Start date for pattern data")
#     pattern_end: datetime = Field(..., description="End date for pattern data")
#     search_start: datetime = Field(..., description="Start date for historical search data")
#     search_end: datetime = Field(..., description="End date for historical search data")
#     ema_period: int = Field(default=14, description="Period for EMA calculation")
#     include_volume: bool = Field(default=False, description="Whether to include volume in pattern matching")

#     class Config:
#         json_encoders = {
#             datetime: lambda v: v.isoformat()
#         }

# from fastapi import FastAPI, File, UploadFile, HTTPException, Query
# from fastapi.middleware.cors import CORSMiddleware
# from pydantic import BaseModel, Field
# from datetime import datetime, timedelta
# from typing import List, Dict, Optional, Union

# class TimeframePatternRequest(BaseModel):
#     symbol: str = Field(..., description="Trading symbol to search patterns for")
#     sample_start: datetime = Field(..., description="Start date for pattern data")
#     sample_end: datetime = Field(..., description="End date for pattern data")
#     search_start: datetime = Field(..., description="Start date for historical search data")
#     search_end: datetime = Field(..., description="End date for historical search data")
#     selected_timeframes: List[str] = Field(..., description="List of selected timeframes for pattern matching")
#     ema_period: int = Field(default=14, description="Period for EMA calculation")

# class MainChartPatternRequest(BaseModel):
#     symbol: str = Field(..., description="Trading symbol to search patterns for")
#     main_timeframe: str = Field(..., description="Main chart timeframe")
#     sample_start: datetime = Field(..., description="Start timestamp for sample data")
#     sample_end: datetime = Field(..., description="End timestamp for sample data")
#     search_start: datetime = Field(..., description="Start date for historical search data")
#     search_end: datetime = Field(..., description="End date for historical search data")
#     ema_period: int = Field(default=14, description="Period for EMA calculation")

# @app.post("/search_patterns/main_chart")
# async def search_patterns_from_main_chart(request: MainChartPatternRequest):
#     try:
#         # Fetch sample data from database using timestamps
#         sample_data = await data_processor.get_data(
#             request.symbol,
#             request.sample_start,
#             request.sample_end,
#             request.main_timeframe
#         )
        
#         if not sample_data or not any(len(v) for v in sample_data.values()):
#             raise HTTPException(
#                 status_code=404,
#                 detail="No sample data found for the specified time range"
#             )

#         # Convert sample data to Polars DataFrame
#         pattern_df = pl.DataFrame({
#             "timestamp": pl.Series(sample_data["timestamp"]),
#             "open": pl.Series(sample_data["open"], dtype=pl.Float64),
#             "high": pl.Series(sample_data["high"], dtype=pl.Float64),
#             "low": pl.Series(sample_data["low"], dtype=pl.Float64),
#             "close": pl.Series(sample_data["close"], dtype=pl.Float64),
#             "volume": pl.Series(sample_data["volume"], dtype=pl.Int64)
#         })

#         # Fetch historical data
#         historical_data = await data_processor.get_data(
#             request.symbol,
#             request.search_start,
#             request.search_end,
#             request.main_timeframe
#         )
        
#         if not historical_data or not any(len(v) for v in historical_data.values()):
#             raise HTTPException(
#                 status_code=404,
#                 detail="No historical data found for the specified search range"
#             )

#         # Convert historical data to Polars DataFrame
#         historical_df = pl.DataFrame({
#             "timestamp": pl.Series(historical_data["timestamp"]),
#             "open": pl.Series(historical_data["open"], dtype=pl.Float64),
#             "high": pl.Series(historical_data["high"], dtype=pl.Float64),
#             "low": pl.Series(historical_data["low"], dtype=pl.Float64),
#             "close": pl.Series(historical_data["close"], dtype=pl.Float64),
#             "volume": pl.Series(historical_data["volume"], dtype=pl.Int64)
#         })

#         # Find patterns
#         matches = find_consecutive_patterns(
#             pattern_df,
#             historical_df,
#             request.ema_period
#         )

#         # Format results
#         results = [
#             {
#                 "timeframe": request.main_timeframe,
#                 "start_date": historical_data["timestamp"][start],
#                 "end_date": historical_data["timestamp"][end],
#                 "similarity": float(similarity),
#                 "pattern": {
#                     "timestamps": historical_data["timestamp"][start:end+1],
#                     "prices": historical_data["close"][start:end+1],
#                     "open": historical_data["open"][start:end+1],
#                     "high": historical_data["high"][start:end+1],
#                     "low": historical_data["low"][start:end+1],
#                     "volume": historical_data["volume"][start:end+1]
#                 }
#             }
#             for start, end, similarity in matches
#         ]

#         return {
#             "status": "success",
#             "matches": results,
#             "pattern_info": {
#                 "symbol": request.symbol,
#                 "timeframe": request.main_timeframe,
#                 "sample_data": {
#                     "timestamps": sample_data["timestamp"],
#                     "prices": sample_data["close"],
#                     "open": sample_data["open"],
#                     "high": sample_data["high"],
#                     "low": sample_data["low"],
#                     "volume": sample_data["volume"]
#                 }
#             }
#         }
        
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

# @app.post("/search_patterns/timeframe")
# async def search_patterns_by_timeframe(request: TimeframePatternRequest):
#     try:
#         all_matches = []
        
#         for timeframe in request.selected_timeframes:
#             # Fetch pattern data for the selected timeframe
#             pattern_data = await data_processor.get_data(
#                 request.symbol,
#                 request.sample_start,
#                 request.sample_end,
#                 timeframe
#             )
            
#             if not pattern_data or not any(len(v) for v in pattern_data.values()):
#                 continue

#             # Fetch historical data for the same timeframe
#             historical_data = await data_processor.get_data(
#                 request.symbol,
#                 request.search_start,
#                 request.search_end,
#                 timeframe
#             )
            
#             if not historical_data or not any(len(v) for v in historical_data.values()):
#                 continue

#             # Convert data to Polars DataFrame
#             pattern_df = pl.DataFrame({
#                 "timestamp": pl.Series(pattern_data["timestamp"]),
#                 "open": pl.Series(pattern_data["open"], dtype=pl.Float64),
#                 "high": pl.Series(pattern_data["high"], dtype=pl.Float64),
#                 "low": pl.Series(pattern_data["low"], dtype=pl.Float64),
#                 "close": pl.Series(pattern_data["close"], dtype=pl.Float64),
#                 "volume": pl.Series(pattern_data["volume"], dtype=pl.Int64)
#             })

#             historical_df = pl.DataFrame({
#                 "timestamp": pl.Series(historical_data["timestamp"]),
#                 "open": pl.Series(historical_data["open"], dtype=pl.Float64),
#                 "high": pl.Series(historical_data["high"], dtype=pl.Float64),
#                 "low": pl.Series(historical_data["low"], dtype=pl.Float64),
#                 "close": pl.Series(historical_data["close"], dtype=pl.Float64),
#                 "volume": pl.Series(historical_data["volume"], dtype=pl.Int64)
#             })

#             # Find patterns
#             matches = find_consecutive_patterns(
#                 pattern_df,
#                 historical_df,
#                 request.ema_period
#             )

#             # Format results for this timeframe
#             timeframe_results = [
#                 {
#                     "timeframe": timeframe,
#                     "start_date": historical_data["timestamp"][start],
#                     "end_date": historical_data["timestamp"][end],
#                     "similarity": float(similarity),
#                     "pattern": {
#                         "timestamps": historical_data["timestamp"][start:end+1],
#                         "prices": historical_data["close"][start:end+1],
#                         "open": historical_data["open"][start:end+1],
#                         "high": historical_data["high"][start:end+1],
#                         "low": historical_data["low"][start:end+1],
#                         "volume": historical_data["volume"][start:end+1]
#                     }
#                 }
#                 for start, end, similarity in matches
#             ]
            
#             all_matches.extend(timeframe_results)

#         # Sort all matches by similarity score
#         all_matches.sort(key=lambda x: x["similarity"], reverse=True)

#         return {
#             "status": "success",
#             "matches": all_matches,
#             "pattern_info": {
#                 "start": request.sample_start.isoformat(),
#                 "end": request.sample_end.isoformat(),
#                 "symbol": request.symbol,
#                 "timeframes": request.selected_timeframes
#             }
#         }
        
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

# @app.get("/search_symbols")
# async def search_symbols(query: str = ""):
#     try:
#         symbols = await data_processor.search_symbols(query)
#         return {"symbols": symbols}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)


# main.py

from fastapi import FastAPI, File, UploadFile, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from datetime import datetime
import polars as pl
from data_proessor import DataProcessor, find_consecutive_patterns, calculate_ema
from typing import List, Dict, Optional
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

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

data_processor = DataProcessor("mongodb://localhost:27017")

# Initialize data processor on startup
@app.on_event("startup")
async def startup_event():
    await data_processor.initialize()

# Pydantic models
class CategoryCreate(BaseModel):
    category: str = Field(..., description="Name of the category to create")

class SymbolAdd(BaseModel):
    symbol: str = Field(..., description="Symbol to add to the category")

class PatternSearchRequest(BaseModel):
    category: str = Field(..., description="Category containing the symbol")
    symbol: str = Field(..., description="Trading symbol to search patterns for")
    start_date: datetime = Field(..., description="Start date for historical data")
    end_date: datetime = Field(..., description="End date for historical data")
    sample_data: List[float] = Field(..., description="Sample price data to find patterns for")
    ema_period: int = Field(default=14, description="Period for EMA calculation")
    include_volume: bool = Field(default=False, description="Whether to include volume in pattern matching")

# Category management endpoints
@app.post("/categories")
async def create_category(category_data: CategoryCreate):
    result = await data_processor.add_category(category_data.category)
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result

@app.delete("/categories/{category}")
async def delete_category(category: str):
    result = await data_processor.delete_category(category)
    if result["status"] == "error":
        raise HTTPException(status_code=404, detail=result["message"])
    return result

@app.post("/categories/{category}/symbols")
async def add_symbol(category: str, symbol_data: SymbolAdd):
    result = await data_processor.add_symbol_to_category(category, symbol_data.symbol)
    if result["status"] == "error":
        raise HTTPException(status_code=404, detail=result["message"])
    return result

@app.delete("/categories/{category}/symbols/{symbol}")
async def remove_symbol(category: str, symbol: str):
    result = await data_processor.remove_symbol_from_category(category, symbol)
    if result["status"] == "error":
        raise HTTPException(status_code=404, detail=result["message"])
    return result

@app.get("/categories")
async def list_categories(query: str = ""):
    return {"categories": await data_processor.list_categories(query)}

@app.get("/categories/{category}/symbols")
async def search_symbols(category: str, query: str = ""):
    symbols = await data_processor.search_symbols_in_category(category, query)
    if not symbols and query == "":
        raise HTTPException(status_code=404, detail=f"Category {category} not found")
    return {"symbols": symbols}

# main.py (continued)

@app.post("/upload/{category}/{symbol}")
async def upload_files(
    category: str,
    symbol: str,
    files: List[UploadFile] = File(...),
    chunk_size: Optional[int] = Query(50000, description="Number of lines to process in each chunk"),
    max_workers: Optional[int] = Query(4, description="Maximum number of parallel workers")
):
    try:
        # Update processor settings if provided
        data_processor.upload_chunk_size = chunk_size
        data_processor.max_workers = max_workers

        # Process files with progress tracking
        result = await data_processor.process_multiple_files(files, category, symbol)
        
        return {
            "status": "completed",
            "details": result,
            "message": f"Processed {result['processed_files']} out of {result['total_files']} files in {result['processing_time_seconds']:.2f} seconds"
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": f"An error occurred during file processing: {str(e)}"
            }
        )

@app.get("/historical_data/{category}/{symbol}")
async def get_historical_data(
    category: str,
    symbol: str,
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    timeframe: str = Query("1T", regex="^(1T|5T|15T|30T|1H|4H|1D|1W|1M)$")
):
    try:
        data = await data_processor.get_data(category, symbol, start_date, end_date, timeframe)
        
        if not data or not any(len(v) for v in data.values()):
            raise HTTPException(
                status_code=404, 
                detail=f"No data found for symbol {symbol} in category {category} for this date range"
            )
        
        return {"data": data}
        
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

class PatternSearchRequest(BaseModel):
    category: str = Field(..., description="Category containing the symbol")
    symbol: str = Field(..., description="Trading symbol to search patterns for")
    sample_start_date: datetime = Field(..., description="Start date for sample pattern")
    sample_end_date: datetime = Field(..., description="End date for sample pattern")
    search_start_date: datetime = Field(..., description="Start date for pattern search")
    search_end_date: datetime = Field(..., description="End date for pattern search")
    ema_period: int = Field(default=14, description="Period for EMA calculation")
    include_volume: bool = Field(default=False, description="Whether to include volume in pattern matching")

@app.post("/search_patterns/")
async def search_patterns(request: PatternSearchRequest):
    try:
        # Validate date ranges
        if request.sample_end_date <= request.sample_start_date:
            raise ValueError("Sample end date must be after sample start date")
        if request.search_end_date <= request.search_start_date:
            raise ValueError("Search end date must be after search start date")

        print(f"Fetching sample data for {request.symbol} from {request.sample_start_date} to {request.sample_end_date}")
        # Get sample data
        sample_data = await data_processor.get_data(
            request.category,
            request.symbol,
            request.sample_start_date,
            request.sample_end_date,
            "5T"
        )
        
        if not sample_data or not any(len(v) for v in sample_data.values()):
            raise HTTPException(
                status_code=404,
                detail=f"No sample data found for symbol {request.symbol} in category {request.category} for the sample date range"
            )

        print(f"Fetching historical data for {request.symbol} from {request.search_start_date} to {request.search_end_date}")
        # Get historical data for searching
        historical_data = await data_processor.get_data(
            request.category,
            request.symbol,
            request.search_start_date,
            request.search_end_date,
            "5T"
        )
        
        if not historical_data or not any(len(v) for v in historical_data.values()):
            raise HTTPException(
                status_code=404,
                detail=f"No historical data found for symbol {request.symbol} in category {request.category} for the search date range"
            )

        print("Converting data to DataFrame")
        # Convert the historical data to a DataFrame for pattern matching
        try:
            historical_df = pl.DataFrame({
                'timestamp': [datetime.fromisoformat(ts) for ts in historical_data['timestamp']],
                'close': historical_data['close'],
                'volume': historical_data['volume']
            })
        except Exception as e:
            print(f"Error creating historical DataFrame: {str(e)}")
            raise

        print("Converting sample data to series")
        # Convert sample data to series
        try:
            sample_series = pl.Series(sample_data['close'])
        except Exception as e:
            print(f"Error creating sample series: {str(e)}")
            raise

        print("Finding patterns")
        # Find patterns
        try:
            top_matches = find_consecutive_patterns(
                sample_series,
                historical_df,
                ema_period=request.ema_period,
                top_n=5
            )
        except Exception as e:
            print(f"Error in pattern matching: {str(e)}")
            raise

        print("Formatting results")
        # Format results
        results = []
        for start_idx, end_idx, similarity in top_matches:
            if start_idx >= len(historical_df) or end_idx >= len(historical_df):
                continue
                
            try:
                result = {
                    "start_date": historical_df['timestamp'][start_idx].isoformat(),
                    "end_date": historical_df['timestamp'][end_idx].isoformat(),
                    "similarity": float(similarity),
                    "pattern_data": {
                        "close": historical_df['close'].slice(start_idx, end_idx - start_idx + 1).to_list(),
                        "volume": historical_df['volume'].slice(start_idx, end_idx - start_idx + 1).to_list() if request.include_volume else None
                    }
                }
                results.append(result)
            except Exception as e:
                print(f"Error formatting result at index {start_idx}-{end_idx}: {str(e)}")
                continue

        return {
            "matches": results,
            "category": request.category,
            "symbol": request.symbol,
            "sample_data": {
                "close": sample_data['close'],
                "timestamp": sample_data['timestamp'],
                "volume": sample_data['volume'] if request.include_volume else None
            }
        }
        
    except HTTPException as he:
        raise he
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        import traceback
        print(f"Unexpected error in search_patterns: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred: {str(e)}\nTraceback: {traceback.format_exc()}"
        )
    
# Additional utility endpoints
@app.get("/category_stats/{category}")
async def get_category_stats(category: str):
    """Get statistics for a category including number of symbols and data points"""
    try:
        category_doc = await data_processor.categories_collection.find_one({"category": category})
        if not category_doc:
            raise HTTPException(status_code=404, detail=f"Category {category} not found")
        
        total_symbols = len(category_doc.get("symbols", []))
        symbol_stats = []
        
        for symbol_info in category_doc.get("symbols", []):
            symbol = symbol_info["symbol"]
            collection_name = data_processor._get_collection_name(category, symbol, "1T")
            count = await data_processor.db[collection_name].count_documents({})
            
            first_doc = await data_processor.db[collection_name].find_one(
                sort=[("timestamp", ASCENDING)]
            )
            last_doc = await data_processor.db[collection_name].find_one(
                sort=[("timestamp", DESCENDING)]
            )
            
            symbol_stats.append({
                "symbol": symbol,
                "data_points": count,
                "first_date": first_doc["timestamp"].isoformat() if first_doc else None,
                "last_date": last_doc["timestamp"].isoformat() if last_doc else None
            })
        
        return {
            "category": category,
            "total_symbols": total_symbols,
            "symbols": symbol_stats
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@app.get("/system_info")
async def get_system_info():
    """Get system information including available timeframes and other settings"""
    return {
        "available_timeframes": list(data_processor.timeframes.keys()),
        "chunk_size": data_processor.chunk_size,
        "max_workers": data_processor.max_workers,
        "bulk_write_size": data_processor.bulk_write_size
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)