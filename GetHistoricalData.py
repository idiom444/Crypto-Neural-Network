import ccxt.async_support as ccxt
import json
import os
from datetime import datetime, timedelta, timezone
import pytz
import time
import csv
import math
import asyncio

async def saveMarketsToFile(exchange):
    markets = await exchange.load_markets()
    swap_markets = {symbol: market for symbol, market in markets.items() if market['type'] == 'swap' and market['settle'] == 'USDT'}
    with open('swap_markets.json', 'w') as f:
        json.dump(swap_markets, f)    
    return

def creatFolder(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
    return

def get_time_2000_min_ago(since, exchange):    
    dt = datetime.fromtimestamp(since / 1000, tz=timezone.utc)    
    dt -= timedelta(minutes=2000)    
    dt = dt.isoformat()
    since = exchange.parse8601(dt)
    return since

async def fetch_ohlcv(exchange, symbol_without_usdt, since, file_path, periodicity):
    if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                row_count = sum(1 for row in reader)
                i = math.ceil(row_count / 2000)
                print('starting collection of ' + symbol_without_usdt + ' ' + periodicity + ' data from load ' + str(i) + ' onwards.')
    else:
        i = 0
        print('starting collection of ' + symbol_without_usdt + ' ' + periodicity + ' data')        
    while True:
        try:            
            if i != 0:
                print(symbol_without_usdt + ' load ' + str(i))
            ohlcv_data = await exchange.fetch_ohlcv(symbol_without_usdt, periodicity, since, limit=2000)
            if len(ohlcv_data) == 0:
                break
            # Write the data to the file
            with open(file_path, 'a', newline='') as f:
                writer = csv.writer(f)
                for entry in reversed(ohlcv_data):
                    writer.writerow(entry)
            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                last_line = list(reader)[-1]
                since = get_time_2000_min_ago(float(last_line[0]),exchange)            
            i += 1
        except ccxt.RateLimitExceeded:
            print(f"Rate limit exceeded for {symbol_without_usdt}. Waiting for 10 seconds before retrying.")
            time.sleep(10)
            continue
        except Exception as e:
            print(f"An error occurred: {e}")
            break
    print('finished collection of ' + symbol_without_usdt + ' ' + periodicity + ' data. ' + str(i) + ' loads.')
    
def get_time_2000_min_ahead(since, exchange):
    dt = datetime.fromtimestamp(since / 1000, tz=timezone.utc)    
    dt += timedelta(minutes=2000)
    dt = dt.isoformat()        
    since = exchange.parse8601(dt)
    return since

async def fetch_ohlcv_until_now(exchange, symbol_without_usdt, starttime, file_path, periodicity, now):    
    i = 0
    print('starting update of ' + symbol_without_usdt + ' ' + periodicity + ' data')
    file_number = 1
    while os.path.exists(f"{file_path}_{file_number}.csv"):
                file_number += 1 
       
    while True:
        try:
            if i != 0:
                print(symbol_without_usdt + ' load ' + str(i))
            ohlcv_data = await exchange.fetch_ohlcv(symbol_without_usdt, periodicity, since=starttime, limit=2000)
            
            if len(ohlcv_data) == 0:
                break                  
            
            with open(f"{file_path}_{file_number}.csv", 'a', newline='') as f:
                writer = csv.writer(f)
                for entry in reversed(ohlcv_data):
                    writer.writerow(entry)          
            
            with open(f"{file_path}_{file_number}.csv", 'r') as f:
                reader = csv.reader(f)
                last_line = list(reader)[-1]
                since = get_time_2000_min_ahead(float(last_line[0]), exchange)
            
            if since >= now:
                break
            i += 1
        except ccxt.RateLimitExceeded:
            print(f"Rate limit exceeded for {symbol_without_usdt}. Waiting for 10 seconds before retrying.")
            time.sleep(10)
            continue
        except Exception as e:
            print(f"An error occurred: {e}")
            break
    print('finished update of ' + symbol_without_usdt + ' ' + periodicity +  ' data. ' + str(i) + ' loads.')     
        
async def fetch_data_for_symbol(exchange, symbol, directory, periodicity):    
    symbol_without_usdt = symbol.replace(':USDT', '')
    now = datetime.now(timezone.utc)    
    try:        
        filename = symbol_without_usdt.replace('/', '_').replace(':', '_')
        directory = directory + '/' + filename + '/' + periodicity
        creatFolder(directory)        
        file_path = os.path.join(directory, f'{filename}_ohlcv_data.csv')        
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                reader = csv.reader(f)                
                last_line = list(reader)[-1]                
                if last_line is not None:
                    since = get_time_2000_min_ago(float(last_line[0]), exchange)
        else:
            since = exchange.parse8601((now - timedelta(minutes=2001)).isoformat())           
        await fetch_ohlcv(exchange, symbol, since, file_path, periodicity)
        
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:                
                reader = csv.reader(f)
                first_line = next(reader, None)
                if first_line is not None:                    
                    first_cell = first_line[0]  # Convert from milliseconds to seconds                    
                    if first_cell is not None and datetime.now(timezone.utc) > (datetime.fromtimestamp(float(first_cell)/1000, tz=timezone.utc) + timedelta(minutes=2000)):
                        first_cell_date = datetime.fromtimestamp(int(first_cell)/1000, tz=timezone.utc)
                        first_cell_date_parse = exchange.parse8601((first_cell_date + timedelta(minutes=1)).isoformat())                              
                        await fetch_ohlcv_until_now(exchange, symbol, first_cell_date_parse, file_path, periodicity, exchange.parse8601(now.isoformat()))
                    print('Data for ' + symbol_without_usdt + ' is up to date.')
    
    except ccxt.BadSymbol:
        print(f"Could not fetch data for {symbol_without_usdt}. Moving on to next symbol.")      

async def getAllSymbolsHistory(directory, periodicity):    
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
        tasks.append(fetch_data_for_symbol(exchange, symbol, directory, periodicity))
    await asyncio.gather(*tasks)            
    await exchange.close()

# Run the async function
exchange = ccxt.phemex({'enableRateLimit': True,})
asyncio.run(saveMarketsToFile(exchange))
exchange.close()
directory = 'ohlcv_data'
periodicity = '1m'
asyncio.run(getAllSymbolsHistory(directory, periodicity))

