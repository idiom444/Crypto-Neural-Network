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

#Save all swap market symbols to file
async def saveMarketsToFile(exchange):
    markets = await exchange.load_markets()
    swap_markets = {symbol: market for symbol, market in markets.items() if market['type'] == 'swap' and market['settle'] == 'USDT'}
    with open('swap_markets.json', 'w') as f:
        json.dump(swap_markets, f)    
    return

#Get the timedelta for initial setup of old data determined by the periodicity
def get_td(periodicity):
    if periodicity.endswith('m'):
        multiplier = int(periodicity[:-1])
        td = timedelta(minutes=2000 * multiplier)
    return td

#Get the timedelta for initial setup of new data dertermined by the periodicity
def get_add_td(periodicity):
    if periodicity.endswith('m'):
        multiplier = int(periodicity[:-1])
        td = timedelta(minutes=multiplier)
    return td

#Get the time ago determined by periodicity
def get_time_ago(since, exchange, periodicity):          
    dt = (since - get_td(periodicity)).replace(tzinfo=timezone.utc) 
    dt = dt.isoformat()
    since = exchange.parse8601(dt)
    return since

#Get the time ahead determined by periodicity
def get_time_ahead(since, exchange, periodicity):           
    dt = (since + get_td(periodicity)).replace(tzinfo=timezone.utc)
    dt = dt.isoformat()        
    since = exchange.parse8601(dt)
    return since

#Get the number of points stored for a symbol and periodicity
def get_point_count(symbol, periodicity, client, bucket, org):
    query_api = client.query_api()
    query = f'from(bucket: "{bucket}") |> range(start: -inf) |> filter(fn: (r) => r._measurement == "ohlcv" and r.Symbol == "{symbol}" and r.Periodicity == "{periodicity}") |> count()'
    result = query_api.query(query, org=org)
    if result and result[0].records:
        return result[0].records[0].get_value()
    else:
        return 0

#Get the oldest InfluxDB timestamp for a symbol and periodicity
def get_oldest_timestamp(symbol, periodicity, client, bucket, org):
    query_api = client.query_api()
    query = f'from(bucket: "{bucket}") |> range(start: -inf) |> filter(fn: (r) => r._measurement == "ohlcv" and r.Symbol == "{symbol}" and r.Periodicity == "{periodicity}") |> first()'
    result = query_api.query(query, org=org)
    if result:
        return result[0].records[0].get_time()
    else:
        return None
 
#Get the most recent InfluxDB timestamp for a symbol and periodicity   
def get_most_recent_timestamp(symbol, periodicity, client, bucket, org):
    query_api = client.query_api()
    query = f'from(bucket: "{bucket}") |> range(start: -inf) |> filter(fn: (r) => r._measurement == "ohlcv" and r.Symbol == "{symbol}" and r.Periodicity == "{periodicity}") |> last()'
    result = query_api.query(query, org=org)
    if result:
        return result[0].records[0].get_time()
    else:
        return None

#Set up InfluxDB connection and bucket if it doesn't exist
def influxdb_setup():
    bucket_name = "PHEMEX Contract HD"
    token = os.environ.get("INFLUXDB_TOKEN")
    org = "CryptoNN"       
    client = InfluxDBClient(url="http://localhost:8086", token=token)       
    buckets_api = client.buckets_api()    
    buckets = buckets_api.find_buckets().buckets
    bucket_exists = any(b.name == bucket_name for b in buckets)
    if not bucket_exists:
        bucket = buckets_api.create_bucket(bucket_name=bucket_name, org_id=org)
        print(f'Bucket "{bucket}" created.')
    return bucket_name, org, client

#Get time to initially start collection of historical data
def get_since(exchange, symbol, period, now, client, bucket_name, org):
    oldest_timestamp = get_oldest_timestamp(symbol, period, client, bucket_name, org)
    if oldest_timestamp is not None:
        since = get_time_ago(oldest_timestamp, exchange, period)
    else:
        since = get_time_ago(now, exchange, period)
    return since

#Update the timestamps if new data is outside the timeframe
def update_setup(most_recent_timestamp, now, exchange, period):
    if most_recent_timestamp < (now - get_td(period)).replace(tzinfo=timezone.utc):
        most_recent_timestamp = exchange.parse8601((most_recent_timestamp + get_add_td(period)).replace(tzinfo=timezone.utc).isoformat())
    now_timestamp = (now.replace(tzinfo=timezone.utc).timestamp() * 1000)
    return most_recent_timestamp, now_timestamp

#Fetch all historical data until there is no more
async def fetch_ohlcv(exchange, symbol, since, periodicity, client, bucket, org):
    
    #How many loads have already been stored
    point_count = get_point_count(symbol, periodicity, client, bucket, org)
    i = math.ceil(point_count / 2000)
    if i != 0:
        print('starting collection of ' + symbol + ' ' + periodicity + ' data from load ' + str(i) + ' onwards.')
    if i == 0:        
        print('starting collection of ' + symbol + ' ' + periodicity + ' data')        
    while True:
        try:
            
            #Load tracker            
            if i != 0:
                print(symbol + ' ' + periodicity + ' load ' + str(i))
                
            #Fetch data
            ohlcv_data = await exchange.fetch_ohlcv(symbol, periodicity, since, limit=2000)
            
            #If no data is returned we are done
            if len(ohlcv_data) == 0:
                break
            
            #Prepare the data for the bucket and write to InfluxDB
            write_api = client.write_api(write_options=SYNCHRONOUS)
            points = []
            for entry in ohlcv_data:
                point = Point("ohlcv").tag("Symbol", symbol).tag("Periodicity", periodicity)\
                    .field("Open", entry[1]).field("High", entry[2]).field("Low", entry[3])\
                    .field("Close", entry[4]).field("Volume", entry[5])\
                    .time(entry[0], WritePrecision.MS)
                points.append(point)
            write_api.write(bucket=bucket, org=org, record=points)
            
            #Update timestamp
            since = get_time_ago(get_oldest_timestamp(symbol, periodicity, client, bucket, org), exchange, periodicity) 
            
            #Update load tracker           
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

#Fetch new data until current time
async def fetch_ohlcv_until_now(exchange, symbol, starttime, periodicity, now, client, bucket, org):            
    i = 0
    print('starting update of ' + symbol + ' ' + periodicity + ' data')      
    while True:
        try:
            
            #Load tracker
            if i != 0:
                print(symbol + ' load ' + str(i))
            
            #Fetch data
            ohlcv_data = await exchange.fetch_ohlcv(symbol, periodicity, since=starttime, limit=2000)            
            if len(ohlcv_data) == 0:
                break                  
            
            # Prepare the data for the bucket and write to InfluxDB
            write_api = client.write_api(write_options=SYNCHRONOUS)
            points = []
            for entry in ohlcv_data:
                point = Point("ohlcv").tag("Symbol", symbol).tag("Periodicity", periodicity)\
                    .field("Open", entry[1]).field("High", entry[2]).field("Low", entry[3])\
                    .field("Close", entry[4]).field("Volume", entry[5])\
                    .time(entry[0], WritePrecision.MS)
                points.append(point)
            write_api.write(bucket=bucket, org=org, record=points)
            
            #Update timestamp and check if we are up to date
            starttime = get_time_ahead(get_most_recent_timestamp(symbol, periodicity, client, bucket, org), exchange, periodicity)            
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

#Handles initial logic for fetching data for a symbol        
async def fetch_data_for_symbol(exchange, symbol):   
    bucket_name, org, client = influxdb_setup()        
    now = datetime.now()   
        
    #Periodicities to fetch 
    periods = ['1m', '5m']   
    try:
        for period in periods:
                        
            #Get historical all historical data        
            since = get_since(exchange, symbol, period, now, client, bucket_name, org)          
            await fetch_ohlcv(exchange, symbol, since, period, client, bucket_name, org)
            
            #If new data is outside timeframe update database
            most_recent_timestamp = get_most_recent_timestamp(symbol, period, client, bucket_name, org)
            if most_recent_timestamp is not None:       
                most_recent_timestamp, now_timestamp = update_setup(most_recent_timestamp, now, exchange, period)
                await fetch_ohlcv_until_now(exchange, symbol, most_recent_timestamp, period, now_timestamp, client, bucket_name, org)
                    
            print('Data for ' + symbol + ' ' + period + 'is up to date.')
    except ccxt.BadSymbol:
        print(f"Could not fetch data for {symbol}. Moving on to next symbol.")
    except Exception as e:
            print(f"An error occurred: {e}")
            return

#Handles which symbols to fetch          
async def getAllSymbolsHistory(exchange):
    try:
         
        #Load all swap symbols from file   
        with open('swap_markets.json', 'r') as f:
            swap_markets = json.load(f)    
        tasks = []        
         
        #All swap symbols that have status of listed on Phemex are fetched     
        for symbol in swap_markets:        
            if swap_markets[symbol]['info']['status'] != 'Listed':
                continue      
            tasks.append(fetch_data_for_symbol(exchange, symbol))
        await asyncio.gather(*tasks)
        
        print('All data has been collected.')            
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        await exchange.close()

#Main function
async def main():
    
    #Get swap markets and save to file
    exchange = ccxt.phemex({'enableRateLimit': True,})
    await saveMarketsToFile(exchange)
           
    #Init data collection   
    await getAllSymbolsHistory()

#Run main function
asyncio.run(main())