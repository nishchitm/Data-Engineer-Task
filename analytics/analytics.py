import json
import math
import asyncio
from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import text
from geopy.distance import geodesic

print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')

# Write the solution here
# Connect to MySQL
while True:
    try:
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to MySQL successful.')

async def etl_task():
    while True:
        # Pull data from PostgreSQL
        psql_conn = psql_engine.connect()
        psql_query = text("""
            SELECT device_id, temperature, location, time
            FROM devices
            WHERE CAST(time AS TIMESTAMP) >= NOW() - INTERVAL '1 HOUR'
        """)
        psql_result = psql_conn.execute(psql_query).fetchall()
        psql_conn.close()

       # Perform data transformations and aggregations
        aggregated_data = {}
        for row in psql_result:
            device_id = str(row['device_id'])
            timestamp = row['time']
            temperature = row['temperature']
            location = json.loads(row['location'])

            # Extract latitude and longitude from the location
            latitude = location['latitude']
            longitude = location['longitude']

            # Calculate distance
            distance = 0
            if device_id in aggregated_data:
                last_lat, last_lon = aggregated_data[device_id]['last_location']

            if device_id in aggregated_data:
                last_lat, last_lon = aggregated_data[device_id]['last_location']
                last_lat = math.radians(last_lat)
                last_lon = math.radians(last_lon)
                distance = math.acos(
                    math.sin(last_lat) * math.sin(latitude) +
                    math.cos(last_lat) * math.cos(latitude) * math.cos(longitude - last_lon)
                ) * 6371
                
                # distance = geodesic((last_lat, last_lon), (latitude, longitude)).kilometers           # Another method to calculate the distance

            if device_id not in aggregated_data:
                aggregated_data[device_id] = {
                    'last_location': (latitude, longitude),
                    'max_temperature': temperature,
                    'data_points': 1,
                    'total_distance': distance
                }
            else:
                aggregated_data[device_id]['last_location'] = (latitude, longitude)
                aggregated_data[device_id]['max_temperature'] = max(aggregated_data[device_id]['max_temperature'], temperature)
                aggregated_data[device_id]['data_points'] += 1
                aggregated_data[device_id]['total_distance'] += distance

        # Store aggregated data into MySQL
        mysql_conn = mysql_engine.connect()
        mysql_conn.execute(text("""
            CREATE TABLE IF NOT EXISTS analytics (
                device_id VARCHAR(36) NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                max_temperature INTEGER,
                data_points INTEGER,
                total_distance FLOAT,
                PRIMARY KEY (device_id, timestamp)
            )
        """))

        for device_id, data in aggregated_data.items():
            mysql_conn.execute(text("""
                INSERT INTO analytics (device_id, timestamp, max_temperature, data_points, total_distance)
                VALUES (:device_id, :timestamp, :max_temperature, :data_points, :total_distance)
            """), device_id=device_id, timestamp=timestamp, max_temperature=data['max_temperature'],
               data_points=data['data_points'], total_distance=data['total_distance'])

        mysql_conn.close()

        await asyncio.sleep(3600)  # Run the task every hour

loop = asyncio.get_event_loop()
loop.run_until_complete(etl_task())
