import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

token = os.environ.get("INFLUXDB_TOKEN")
org = "CryptoNN"
url = "http://localhost:8086"

write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

bucket="Crypto"

query_api = write_client.query_api()

query = """
from(bucket: "PHEMEX Contract HD")
  |> range(start: -30d)
  |> filter(fn: (r) => r._measurement == "ohlcv" and r._field == "BTC/USDT:USDT")
"""
tables = query_api.query(query, org="CryptoNN")

for table in tables:
    for record in table.records:
        print(record)