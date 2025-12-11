import snowflake.connector
import requests
from datetime import datetime
import os
from dotenv import load_dotenv
from typing import Dict, List, Optional, Tuple

load_dotenv()

# Weather API Configuration
WEATHER_API_KEY = os.getenv('WEATHER_API_KEY')
WEATHER_API_URL = "https://api.openweathermap.org/data/2.5/weather"

# Snowflake Configuration
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

class DataQualityError(Exception):
    """Custom exception for data quality failures"""
    pass

class WeatherDataValidator:
    """Validate weather data before ingestion"""
    
    def __init__(self):
        self.validation_errors = []
        self.validation_warnings = []
    
    def validate(self, weather_data: Dict) -> Tuple[bool, List[str], List[str]]:
        """
        Validate weather data against quality rules
        Returns: (is_valid, errors, warnings)
        """
        self.validation_errors = []
        self.validation_warnings = []
        
        # Check 1: Required fields exist
        self._check_required_fields(weather_data)
        
        # Check 2: Temperature is realistic
        self._check_temperature(weather_data)
        
        # Check 3: Humidity is valid
        self._check_humidity(weather_data)
        
        # Check 4: Pressure is realistic
        self._check_pressure(weather_data)
        
        # Check 5: City name is valid
        self._check_city_name(weather_data)
        
        # Check 6: Country code is valid
        self._check_country_code(weather_data)
        
        # Check 7: Wind speed is reasonable
        self._check_wind_speed(weather_data)
        
        is_valid = len(self.validation_errors) == 0
        return is_valid, self.validation_errors, self.validation_warnings
    
    def _check_required_fields(self, data: Dict):
        """Ensure all required fields are present"""
        required_paths = [
            ('name', 'City name'),
            ('sys.country', 'Country code'),
            ('main.temp', 'Temperature'),
            ('main.humidity', 'Humidity'),
            ('main.pressure', 'Pressure'),
            ('weather', 'Weather conditions'),
        ]
        
        for path, field_name in required_paths:
            if not self._get_nested_value(data, path):
                self.validation_errors.append(f"Missing required field: {field_name}")
    
    def _check_temperature(self, data: Dict):
        """Validate temperature is within realistic range"""
        temp = self._get_nested_value(data, 'main.temp')
        if temp is None:
            return
        
        # Extreme temperature limits (Celsius)
        MIN_TEMP = -90  # Coldest recorded on Earth: -89.2°C
        MAX_TEMP = 60   # Hottest recorded on Earth: 56.7°C
        
        if temp < MIN_TEMP or temp > MAX_TEMP:
            self.validation_errors.append(
                f"Temperature {temp}°C is outside valid range ({MIN_TEMP}°C to {MAX_TEMP}°C)"
            )
        
        # Warning for unusual but possible temperatures
        if temp < -40 or temp > 50:
            self.validation_warnings.append(
                f"Temperature {temp}°C is unusual but possible"
            )
    
    def _check_humidity(self, data: Dict):
        """Validate humidity is between 0-100%"""
        humidity = self._get_nested_value(data, 'main.humidity')
        if humidity is None:
            return
        
        if humidity < 0 or humidity > 100:
            self.validation_errors.append(
                f"Humidity {humidity}% is invalid (must be 0-100%)"
            )
    
    def _check_pressure(self, data: Dict):
        """Validate atmospheric pressure is realistic"""
        pressure = self._get_nested_value(data, 'main.pressure')
        if pressure is None:
            return
        
        # Atmospheric pressure limits (millibars)
        MIN_PRESSURE = 870   # Lowest recorded: 870 mb (Typhoon Tip)
        MAX_PRESSURE = 1085  # Highest recorded: 1085 mb (Siberia)
        
        if pressure < MIN_PRESSURE or pressure > MAX_PRESSURE:
            self.validation_errors.append(
                f"Pressure {pressure}mb is outside valid range ({MIN_PRESSURE}-{MAX_PRESSURE}mb)"
            )
        
        # Warning for unusual pressure
        if pressure < 950 or pressure > 1050:
            self.validation_warnings.append(
                f"Pressure {pressure}mb is unusual (possible extreme weather)"
            )
    
    def _check_city_name(self, data: Dict):
        """Validate city name format"""
        city = self._get_nested_value(data, 'name')
        if city is None:
            return
        
        if len(city) < 2:
            self.validation_errors.append(f"City name '{city}' is too short")
        
        if len(city) > 50:
            self.validation_errors.append(f"City name '{city}' is too long")
        
        # Check for suspicious characters
        if not city.replace(' ', '').replace('-', '').replace("'", '').isalnum():
            self.validation_warnings.append(f"City name '{city}' contains unusual characters")
    
    def _check_country_code(self, data: Dict):
        """Validate country code is 2 characters (ISO 3166-1 alpha-2)"""
        country = self._get_nested_value(data, 'sys.country')
        if country is None:
            return
        
        if len(country) != 2:
            self.validation_errors.append(
                f"Country code '{country}' is invalid (must be 2 characters)"
            )
        
        if not country.isalpha():
            self.validation_errors.append(
                f"Country code '{country}' must contain only letters"
            )
    
    def _check_wind_speed(self, data: Dict):
        """Validate wind speed is reasonable"""
        wind_speed = self._get_nested_value(data, 'wind.speed')
        if wind_speed is None:
            return
        
        # Wind speed in m/s
        MAX_WIND_SPEED = 113  # ~408 km/h (highest gust recorded)
        
        if wind_speed < 0:
            self.validation_errors.append(f"Wind speed cannot be negative: {wind_speed}m/s")
        
        if wind_speed > MAX_WIND_SPEED:
            self.validation_errors.append(
                f"Wind speed {wind_speed}m/s exceeds maximum recorded wind speed"
            )
        
        # Hurricane force winds (33 m/s = ~120 km/h)
        if wind_speed > 33:
            self.validation_warnings.append(
                f"Wind speed {wind_speed}m/s indicates hurricane-force winds"
            )
    
    def _get_nested_value(self, data: Dict, path: str):
        """Safely get nested dictionary value using dot notation"""
        keys = path.split('.')
        value = data
        
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
                if value is None:
                    return None
            else:
                return None
        
        return value


def get_weather_data(city: str) -> Optional[Dict]:
    """Fetch weather data from OpenWeatherMap API"""
    params = {
        'q': city,
        'appid': WEATHER_API_KEY,
        'units': 'metric'
    }
    
    try:
        response = requests.get(WEATHER_API_URL, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching weather data: {e}")
        return None


def connect_to_snowflake():
    """Create Snowflake connection"""
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE
        )
        
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE}")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")
        cursor.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
        cursor.close()
        
        return conn
    except Exception as e:
        print(f"❌ Error connecting to Snowflake: {e}")
        return None


def create_weather_table(conn):
    """Create weather table if it doesn't exist"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS weather_data (
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
        cursor.close()
    except Exception as e:
        print(f"❌ Error creating table: {e}")


def insert_weather_data(conn, weather_data: Dict):
    """Insert validated weather data into Snowflake"""
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
        
        cursor.close()
        return True
    except Exception as e:
        print(f"❌ Error inserting data: {e}")
        return False


def main():
    """Main function with pre-ingestion validation"""
    cities = ['London', 'New York', 'Tokyo', 'Lagos', 'Abuja']
    
    print("="*70)
    print("WEATHER DATA INGESTION WITH PRE-VALIDATION")
    print("="*70)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Initialize validator
    validator = WeatherDataValidator()
    
    # Track statistics
    total_cities = len(cities)
    validated_count = 0
    ingested_count = 0
    failed_validation = []
    warnings_list = []
    
    # Connect to Snowflake
    conn = connect_to_snowflake()
    if not conn:
        print("❌ Cannot proceed without Snowflake connection")
        return
    
    create_weather_table(conn)
    
    print("PHASE 1: FETCH & VALIDATE")
    print("-" * 70)
    
    valid_data = []
    
    for city in cities:
        print(f"\n📍 Processing {city}...")
        
        # Step 1: Fetch data
        weather_data = get_weather_data(city)
        if not weather_data:
            print(f"   ⚠️  Skipped: Could not fetch data")
            failed_validation.append(city)
            continue
        
        # Step 2: Validate BEFORE ingestion
        is_valid, errors, warnings = validator.validate(weather_data)
        
        if warnings:
            print(f"   ⚠️  Warnings:")
            for warning in warnings:
                print(f"      - {warning}")
            warnings_list.append((city, warnings))
        
        if not is_valid:
            print(f"   ❌ VALIDATION FAILED:")
            for error in errors:
                print(f"      - {error}")
            failed_validation.append(city)
            print(f"   🚫 Data will NOT be ingested")
        else:
            print(f"   ✅ Validation passed")
            valid_data.append((city, weather_data))
            validated_count += 1
    
    # Phase 2: Ingest only validated data
    print("\n" + "="*70)
    print("PHASE 2: INGEST VALIDATED DATA")
    print("-" * 70 + "\n")
    
    for city, weather_data in valid_data:
        print(f"💾 Ingesting {city}... ", end="")
        if insert_weather_data(conn, weather_data):
            print("✅ Success")
            ingested_count += 1
        else:
            print("❌ Failed")
    
    conn.close()
    
    # Summary
    print("\n" + "="*70)
    print("INGESTION SUMMARY")
    print("="*70)
    
    print(f"\nTotal Cities Attempted: {total_cities}")
    print(f"✅ Validated: {validated_count}")
    print(f"💾 Ingested: {ingested_count}")
    print(f"❌ Failed Validation: {len(failed_validation)}")
    print(f"⚠️  Warnings: {len(warnings_list)}")
    
    if failed_validation:
        print(f"\n🚫 Cities that failed validation (NOT ingested):")
        for city in failed_validation:
            print(f"   - {city}")
    
    if warnings_list:
        print(f"\n⚠️  Cities with warnings (but ingested):")
        for city, warnings in warnings_list:
            print(f"   - {city}: {len(warnings)} warning(s)")
    
    success_rate = (ingested_count / total_cities * 100) if total_cities > 0 else 0
    
    print(f"\n{'✅ SUCCESS' if success_rate == 100 else '⚠️  PARTIAL SUCCESS'}")
    print(f"Success Rate: {success_rate:.1f}%")
    
    if ingested_count == 0:
        print("\n❌ NO DATA WAS INGESTED - All records failed validation")
        exit(1)
    elif ingested_count < total_cities:
        print(f"\n⚠️  Only {ingested_count}/{total_cities} records ingested due to validation failures")
        exit(1)
    else:
        print(f"\n🎉 All {ingested_count} records successfully validated and ingested!")


if __name__ == "__main__":
    main()