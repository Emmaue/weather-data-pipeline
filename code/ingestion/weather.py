import snowflake.connector
import requests
import json
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Weather API Configuration (using OpenWeatherMap as example)
WEATHER_API_KEY = os.getenv('WEATHER_API_KEY')
WEATHER_API_URL = "https://api.openweathermap.org/data/2.5/weather"

# Snowflake Configuration
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

def get_weather_data(city):
    """Fetch weather data from OpenWeatherMap API"""
    params = {
        'q': city,
        'appid': WEATHER_API_KEY,
        'units': 'metric'  # Use 'imperial' for Fahrenheit
    }
    
    try:
        response = requests.get(WEATHER_API_URL, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None

def connect_to_snowflake():
    """Create Snowflake connection"""
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        print("Successfully connected to Snowflake")
        
        # Explicitly set the context
        cursor = conn.cursor()
        cursor.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
        cursor.close()
        print(f"Context set: {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")
        
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        return None

def create_weather_table(conn):
    """Create weather table if it doesn't exist"""
    create_table_query = """
    CREATE OR REPLACE TABLE IF EXISTS weather_data (
        id INTEGER AUTOINCREMENT,
        city STRING,
        country STRING,
        temperature FLOAT,
        feels_like FLOAT,
        temp_min FLOAT,
        temp_max FLOAT,
        pressure INTEGER,
        humidity INTEGER,
        weather_main STRING,
        weather_description STRING,
        wind_speed FLOAT,
        wind_deg INTEGER,
        clouds INTEGER,
        timestamp TIMESTAMP_NTZ,
        ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (id)
    );
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        print("Weather table created or already exists")
        cursor.close()
    except Exception as e:
        print(f"Error creating table: {e}")

def insert_weather_data(conn, weather_data):
    """Insert weather data into Snowflake"""
    if not weather_data:
        print("No weather data to insert")
        return
    
    insert_query = """
    INSERT INTO weather_data (
        city, country, temperature, feels_like, temp_min, temp_max,
        pressure, humidity, weather_main, weather_description,
        wind_speed, wind_deg, clouds, timestamp
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    );
    """
    
    try:
        cursor = conn.cursor()
        
        # Extract data from API response
        city = weather_data['name']
        country = weather_data['sys']['country']
        temperature = weather_data['main']['temp']
        feels_like = weather_data['main']['feels_like']
        temp_min = weather_data['main']['temp_min']
        temp_max = weather_data['main']['temp_max']
        pressure = weather_data['main']['pressure']
        humidity = weather_data['main']['humidity']
        weather_main = weather_data['weather'][0]['main']
        weather_description = weather_data['weather'][0]['description']
        wind_speed = weather_data['wind']['speed']
        wind_deg = weather_data['wind'].get('deg', 0)
        clouds = weather_data['clouds']['all']
        timestamp = datetime.fromtimestamp(weather_data['dt'])
        
        cursor.execute(insert_query, (
            city, country, temperature, feels_like, temp_min, temp_max,
            pressure, humidity, weather_main, weather_description,
            wind_speed, wind_deg, clouds, timestamp
        ))
        
        print(f"Successfully inserted weather data for {city}, {country}")
        cursor.close()
    except Exception as e:
        print(f"Error inserting data: {e}")

def main():
    """Main function to orchestrate weather data ingestion"""
    # List of cities to fetch weather data for
    cities = ['London', 'New York', 'Tokyo', 'Lagos', 'Abuja']
    
    # Connect to Snowflake
    conn = connect_to_snowflake()
    if not conn:
        return
    
    # Create table
    create_weather_table(conn)
    
    # Fetch and insert weather data for each city
    for city in cities:
        print(f"\nFetching weather data for {city}...")
        weather_data = get_weather_data(city)
        
        if weather_data:
            insert_weather_data(conn, weather_data)
    
    # Close connection
    conn.close()
    print("\nData ingestion completed!")

if __name__ == "__main__":
    main()