import json
import math
from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
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

def calculate_distance(location1, location2):
    lat1, lon1 = json.loads(location1).values()
    lat2, lon2 = json.loads(location2).values()
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    distance = geodesic((lat1, lon1), (lat2, lon2)).kilometers
    return distance

with mysql_engine.connect() as conn:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS max_temperatures (
            device_id VARCHAR(255) NOT NULL,
            hour TIMESTAMP NOT NULL,
            max_temperature INTEGER,
            PRIMARY KEY (device_id, hour)
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS data_points_count (
            device_id VARCHAR(255) NOT NULL,
            hour TIMESTAMP NOT NULL,
            count INTEGER,
            PRIMARY KEY (device_id, hour)
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS total_distance (
            device_id VARCHAR(255) NOT NULL,
            hour TIMESTAMP NOT NULL,
            distance FLOAT,
            PRIMARY KEY (device_id, hour)
        )
        """
    )

while True:
    try:
        with mysql_engine.connect() as mysql_conn, psql_engine.connect() as psql_conn:
            # Calculate and store maximum temperatures
            max_temps_query = """
            INSERT INTO max_temperatures (device_id, hour, max_temperature)
            SELECT device_id, date_trunc('hour', time) as hour, max(temperature)
            FROM devices
            GROUP BY device_id, hour
            ON DUPLICATE KEY UPDATE max_temperature = VALUES(max_temperature)
            """
            mysql_conn.execute(max_temps_query)

            # Calculate and store data points count
            count_query = """
            INSERT INTO data_points_count (device_id, hour, count)
            SELECT device_id, date_trunc('hour', time) as hour, count(*)
            FROM devices
            GROUP BY device_id, hour
            ON DUPLICATE KEY UPDATE count = VALUES(count)
            """
            mysql_conn.execute(count_query)

            # Calculate and store total distance
            distance_query = """
            INSERT INTO total_distance (device_id, hour, distance)
            SELECT d1.device_id, date_trunc('hour', d1.time) as hour,
                SUM(calculate_distance(d1.location, d2.location))
            FROM devices d1
            INNER JOIN devices d2
            ON d1.device_id = d2.device_id
            AND d1.time > d2.time
            GROUP BY d1.device_id, hour
            ON DUPLICATE KEY UPDATE distance = VALUES(distance)
            """
            mysql_conn.execute(distance_query)

    except OperationalError:
        sleep(0.1)