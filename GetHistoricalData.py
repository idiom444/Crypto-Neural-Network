import ccxt.async_support as ccxt
import asyncio
from influxdb_client import InfluxDBClient, Point, WritePrecision, BucketsApi
from influxdb_client.client.write_api import SYNCHRONOUS
import json
from datetime import datetime, timedelta, timezone
import time
import os
import math
from ccxt.base.errors import RequestTimeout



async def saveMarketsToFile(exchange):
    markets = await exchange.load_markets()
    swap_markets = {symbol: market for symbol, market in markets.items() if market['type'] == 'swap' and market['settle'] == 'USDT'}
    with open('swap_markets.json', 'w') as f:
        json.dump(swap_markets, f)    
    return

def get_time_2000_min_ago(since, exchange):        
    dt = (since - timedelta(minutes=2000)).replace(tzinfo=timezone.utc) 
    dt = dt.isoformat()
    since = exchange.parse8601(dt)
    return since

def get_time_2000_min_ahead(since, exchange):       
    dt = (since + timedelta(minutes=2000)).replace(tzinfo=timezone.utc)
    dt = dt.isoformat()        
    since = exchange.parse8601(dt)
    return since

def get_point_count(symbol, periodicity, client, bucket, org):
    query_api = client.query_api()
    query = f'from(bucket: "{bucket}") |> range(start: -inf) |> filter(fn: (r) => r._measurement == "ohlcv" and r.Symbol == "{symbol}" and r.Periodicity == "{periodicity}") |> count()'
    result = query_api.query(query, org=org)
    if result and result[0].records:
        return result[0].records[0].get_value()
    else:
        return 0

def get_oldest_timestamp(symbol, periodicity, client, bucket, org):
    query_api = client.query_api()
    query = f'from(bucket: "{bucket}") |> range(start: -inf) |> filter(fn: (r) => r._measurement == "ohlcv" and r.Symbol == "{symbol}" and r.Periodicity == "{periodicity}") |> first()'
    result = query_api.query(query, org=org)
    if result:
        return result[0].records[0].get_time()
    else:
        return None
    
def get_most_recent_timestamp(symbol, periodicity, client, bucket, org):
    query_api = client.query_api()
    query = f'from(bucket: "{bucket}") |> range(start: -inf) |> filter(fn: (r) => r._measurement == "ohlcv" and r.Symbol == "{symbol}" and r.Periodicity == "{periodicity}") |> last()'
    result = query_api.query(query, org=org)
    if result:
        return result[0].records[0].get_time()
    else:
        return None

async def fetch_ohlcv(exchange, symbol, since, periodicity, client, bucket, org):
    point_count = get_point_count(symbol, periodicity, client, bucket, org)
    i = math.ceil(point_count / 2000)
    if i != 0:
        print('starting collection of ' + symbol + ' ' + periodicity + ' data from load ' + str(i) + ' onwards.')
    if i == 0:        
        print('starting collection of ' + symbol + ' ' + periodicity + ' data')        
    while True:
        try:            
            if i != 0:
                print(symbol + ' ' + periodicity + ' load ' + str(i))
            ohlcv_data = await exchange.fetch_ohlcv(symbol, periodicity, since, limit=2000)
            if len(ohlcv_data) == 0:
                break
            # Prepare the data for the bucket
            write_api = client.write_api(write_options=SYNCHRONOUS)
            points = []
            for entry in reversed(ohlcv_data):
                point = Point("ohlcv").tag("Symbol", symbol).tag("Periodicity", periodicity)\
                    .field("Open", entry[1]).field("High", entry[2]).field("Low", entry[3])\
                    .field("Close", entry[4]).field("Volume", entry[5])\
                    .time(entry[0], WritePrecision.MS)
                points.append(point)
            write_api.write(bucket=bucket, org=org, record=points)
            since = get_time_2000_min_ago(get_oldest_timestamp(symbol, periodicity, client, bucket, org), exchange)
            
            point_count = get_point_count(symbol, periodicity, client, bucket, org)
            i = math.ceil(point_count / 2000)
        except ccxt.RateLimitExceeded:
            print(f"Rate limit exceeded for {symbol}. Waiting for 10 seconds before retrying.")
            await time.sleep(10)
            continue       
        except TimeoutError:
            print(f"Timeout error for {symbol}. Waiting for 10 seconds before retrying.")
            await time.sleep(10)
            continue
        except RequestTimeout:
            print(f"Request timed out for {symbol}. Retrying in 10 seconds.")
            await time.sleep(10)
            continue            
        except Exception as e:
            print(f"An error occurred: {e}")
            break
    print('finished collection of ' + symbol + ' ' + periodicity + ' data. ' + str(i) + ' loads.')

async def fetch_ohlcv_until_now(exchange, symbol, starttime, periodicity, now, client, bucket, org):    
    i = 0
    print('starting update of ' + symbol + ' ' + periodicity + ' data')      
    while True:
        try:
            if i != 0:
                print(symbol + ' load ' + str(i))
            ohlcv_data = await exchange.fetch_ohlcv(symbol, periodicity, since=starttime, limit=2000)            
            if len(ohlcv_data) == 0:
                break                  
            
            # Prepare the data for the bucket
            write_api = client.write_api(write_options=SYNCHRONOUS)
            points = []
            for entry in ohlcv_data:
                point = Point("ohlcv").tag("Symbol", symbol).tag("Periodicity", periodicity)\
                    .field("Open", entry[1]).field("High", entry[2]).field("Low", entry[3])\
                    .field("Close", entry[4]).field("Volume", entry[5])\
                    .time(entry[0], WritePrecision.MS)
                points.append(point)
            write_api.write(bucket=bucket, org=org, record=points)
            
            starttime = get_time_2000_min_ahead(get_most_recent_timestamp(symbol, periodicity, client, bucket, org), exchange)
            
            if starttime >= now:
                break
            i += 1
        except TimeoutError:
            print(f"Timeout error for {symbol}. Waiting for 10 seconds before retrying.")
            await time.sleep(10)
            continue
        except ccxt.RateLimitExceeded:
            print(f"Rate limit exceeded for {symbol}. Waiting for 10 seconds before retrying.")
            time.sleep(10)
            continue
        except RequestTimeout:
            print(f"Request timed out for {symbol}. Retrying in 10 seconds.")
            await time.sleep(10)
            continue
        except Exception as e:
            print(f"An error occurred: {e}")
            break
    print('finished update of ' + symbol + ' ' + periodicity +  ' data. ' + str(i) + ' loads.')     
        
async def fetch_data_for_symbol(exchange, symbol, periodicity): 
    bucket_name = "PHEMEX Contract HD"
    token = os.environ.get("INFLUXDB_TOKEN")
    org = "CryptoNN"   
    now = datetime.now() 
    
    client = InfluxDBClient(url="http://localhost:8086", token=token) 
       
    buckets_api = client.buckets_api()    
    buckets = buckets_api.find_buckets().buckets
    bucket_exists = any(b.name == bucket_name for b in buckets)
    if not bucket_exists:
        bucket = buckets_api.create_bucket(bucket_name=bucket_name, org_id=org)
        print(f'Bucket "{bucket}" created.')   
       
    try:        
        oldest_timestamp = get_oldest_timestamp(symbol, periodicity, client, bucket_name, org)
        if oldest_timestamp is not None:
            since = get_time_2000_min_ago(oldest_timestamp, exchange)
        else:
            since = get_time_2000_min_ago(now, exchange)          
        await fetch_ohlcv(exchange, symbol, since, periodicity, client, bucket_name, org)
        
        most_recent_timestamp = get_most_recent_timestamp(symbol, periodicity, client, bucket_name, org)
        if most_recent_timestamp is not None:            
            if most_recent_timestamp < (now - timedelta(minutes=2000)).replace(tzinfo=timezone.utc):
                most_recent_timestamp = exchange.parse8601((most_recent_timestamp + timedelta(minutes=1)).replace(tzinfo=timezone.utc).isoformat())
                await fetch_ohlcv_until_now(exchange, symbol, most_recent_timestamp, periodicity, (now.replace(tzinfo=timezone.utc).timestamp() * 1000), client, bucket_name, org)
        print('Data for ' + symbol + ' is up to date.')
    except ccxt.BadSymbol:
        print(f"Could not fetch data for {symbol}. Moving on to next symbol.")
    except Exception as e:
            print(f"An error occurred: {e}")
            return
          
async def getAllSymbolsHistory(periodicity):
    try:    
        with open('swap_markets.json', 'r') as f:
            swap_markets = json.load(f)    
        tasks = []     
        exchange = ccxt.phemex({
        'enableRateLimit': True   
        })  
        #exchange.verbose = True      
        for symbol in swap_markets:        
            if swap_markets[symbol]['info']['status'] != 'Listed':
                continue      
            tasks.append(fetch_data_for_symbol(exchange, symbol, periodicity))
        await asyncio.gather(*tasks)
        print('All data has been collected.')            
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        await exchange.close()

async def main():
    exchange = ccxt.phemex({'enableRateLimit': True,})
    await saveMarketsToFile(exchange)
    await exchange.close()    
    periodicity = '1m'
    await getAllSymbolsHistory(periodicity)

asyncio.run(main())