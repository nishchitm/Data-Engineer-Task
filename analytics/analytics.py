import json
from math import sin, cos, acos, radians
from geopy.distance import geodesic
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, select
from sqlalchemy.exc import OperationalError
import pandas as pd
from os import environ
from time import sleep


print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        print("Error connecting PostgresSQL.")
        sleep(0.1)
print('Connection to PostgresSQL successful.')

# Write the solution here

# Connect to MySQL
while True:
    try:
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        print("Error connecting MySQL.")
        sleep(0.1)
print('Connection to MySQL successful.')


def calculate_distance(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    return acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(lon2 - lon1)) * 6371


def aggregate_data(df):
    df['time'] = pd.to_datetime(df['time'], unit='s')
    df['hour'] = df['time'].dt.floor('H')
    df['distance'] = df['location'].apply(
        lambda loc: calculate_distance(
            loc['latitude'], loc['longitude'],
            df['location'].iloc[0]['latitude'], df['location'].iloc[0]['longitude']
        )
    )
    aggregated = df.groupby(['device_id', 'hour']).agg({
        'temperature': 'max',
        'device_id': 'count',
        'distance': 'sum'
    }).rename(columns={
        'device_id': 'data_points',
        'distance': 'total_distance'
    }).reset_index()
    return aggregated


metadata = MetaData()
analytics = Table(
    'analytics', metadata,
    Column('device_id', Integer),
    Column('hour', Integer),
    Column('temperature', Integer),
    Column('data_points', Integer),
    Column('total_distance', Float),
)
metadata.create_all(mysql_engine)

while True:
    try:
        with psql_engine.connect() as psql_conn, mysql_engine.connect() as mysql_conn:

            metadata.reflect(bind=psql_engine)
            devices_table = metadata.tables['devices']
            stmt = devices_table.select()
            result = psql_conn.execute(stmt)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            aggregated = aggregate_data(df)
            mysql_conn.execute(analytics.insert(), aggregated.to_dict('records'))

    except OperationalError:
        sleep(0.1)
