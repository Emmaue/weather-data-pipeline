"""
Simple data quality validation without Great Expectations
This demonstrates the concepts of data quality checking
"""
import snowflake.connector
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA')
)

cursor = conn.cursor()

print("="*70)
print("WEATHER DATA QUALITY VALIDATION")
print("="*70)
print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# Track results
checks_passed = 0
checks_failed = 0
issues = []

# Check 1: Table has data
print("1. Checking if table has data...")
cursor.execute("SELECT COUNT(*) FROM weather_data")
row_count = cursor.fetchone()[0]
if row_count > 0:
    print(f"   ✓ PASS: Table has {row_count} rows")
    checks_passed += 1
else:
    print(f"   ✗ FAIL: Table is empty")
    checks_failed += 1
    issues.append("Table has no data")

# Check 2: No NULL values in critical columns
print("\n2. Checking for NULL values in critical columns...")
cursor.execute("""
    SELECT 
        COUNT(*) - COUNT(CITY) as null_cities,
        COUNT(*) - COUNT(TEMPERATURE) as null_temps,
        COUNT(*) - COUNT(HUMIDITY) as null_humidity
    FROM weather_data
""")
result = cursor.fetchone()
if sum(result) == 0:
    print(f"   ✓ PASS: No NULL values in critical columns")
    checks_passed += 1
else:
    print(f"   ✗ FAIL: Found NULL values - Cities: {result[0]}, Temps: {result[1]}, Humidity: {result[2]}")
    checks_failed += 1
    issues.append(f"NULL values detected")

# Check 3: Temperature range is realistic
print("\n3. Checking temperature range (-50°C to 60°C)...")
cursor.execute("""
    SELECT COUNT(*) 
    FROM weather_data 
    WHERE TEMPERATURE < -50 OR TEMPERATURE > 60
""")
invalid_temps = cursor.fetchone()[0]
if invalid_temps == 0:
    print(f"   ✓ PASS: All temperatures are within valid range")
    checks_passed += 1
else:
    print(f"   ✗ FAIL: {invalid_temps} rows have invalid temperatures")
    checks_failed += 1
    issues.append(f"{invalid_temps} invalid temperature values")

# Check 4: Humidity range is valid (0-100%)
print("\n4. Checking humidity range (0% to 100%)...")
cursor.execute("""
    SELECT COUNT(*) 
    FROM weather_data 
    WHERE HUMIDITY < 0 OR HUMIDITY > 100
""")
invalid_humidity = cursor.fetchone()[0]
if invalid_humidity == 0:
    print(f"   ✓ PASS: All humidity values are within valid range")
    checks_passed += 1
else:
    print(f"   ✗ FAIL: {invalid_humidity} rows have invalid humidity")
    checks_failed += 1
    issues.append(f"{invalid_humidity} invalid humidity values")

# Check 5: Pressure range is realistic (800-1100 millibars)
print("\n5. Checking pressure range (800-1100 millibars)...")
cursor.execute("""
    SELECT COUNT(*) 
    FROM weather_data 
    WHERE PRESSURE < 800 OR PRESSURE > 1100
""")
invalid_pressure = cursor.fetchone()[0]
if invalid_pressure == 0:
    print(f"   ✓ PASS: All pressure values are within valid range")
    checks_passed += 1
else:
    print(f"   ✗ FAIL: {invalid_pressure} rows have invalid pressure")
    checks_failed += 1
    issues.append(f"{invalid_pressure} invalid pressure values")

# Check 6: City names are reasonable length (2-50 chars)
print("\n6. Checking city name lengths...")
cursor.execute("""
    SELECT COUNT(*) 
    FROM weather_data 
    WHERE LENGTH(CITY) < 2 OR LENGTH(CITY) > 50
""")
invalid_cities = cursor.fetchone()[0]
if invalid_cities == 0:
    print(f"   ✓ PASS: All city names have valid length")
    checks_passed += 1
else:
    print(f"   ✗ FAIL: {invalid_cities} rows have invalid city names")
    checks_failed += 1
    issues.append(f"{invalid_cities} invalid city name lengths")

# Check 7: Country codes are 2 characters
print("\n7. Checking country code format...")
cursor.execute("""
    SELECT COUNT(*) 
    FROM weather_data 
    WHERE LENGTH(COUNTRY) != 2
""")
invalid_countries = cursor.fetchone()[0]
if invalid_countries == 0:
    print(f"   ✓ PASS: All country codes are 2 characters")
    checks_passed += 1
else:
    print(f"   ✗ FAIL: {invalid_countries} rows have invalid country codes")
    checks_failed += 1
    issues.append(f"{invalid_countries} invalid country codes")

# Check 8: Recent data exists (data from today)
print("\n8. Checking for recent data...")
cursor.execute("""
    SELECT COUNT(*) 
    FROM weather_data 
    WHERE DATE(INGESTION_TIMESTAMP) = CURRENT_DATE()
""")
recent_data = cursor.fetchone()[0]
if recent_data > 0:
    print(f"   ✓ PASS: Found {recent_data} records from today")
    checks_passed += 1
else:
    print(f"   ⚠  WARNING: No data from today (might need to run ingestion)")
    checks_failed += 1
    issues.append("No recent data from today")

# Summary
print("\n" + "="*70)
print("VALIDATION SUMMARY")
print("="*70)

total_checks = checks_passed + checks_failed
success_rate = (checks_passed / total_checks * 100) if total_checks > 0 else 0

print(f"\nTotal Checks: {total_checks}")
print(f"✓ Passed: {checks_passed}")
print(f"✗ Failed: {checks_failed}")
print(f"Success Rate: {success_rate:.1f}%")

if checks_failed == 0:
    print("\n" + "="*70)
    print("✅ ALL VALIDATIONS PASSED!")
    print("="*70)
    print("Your weather data meets all quality standards.")
else:
    print("\n" + "="*70)
    print("❌ VALIDATION FAILED")
    print("="*70)
    print("\nIssues found:")
    for i, issue in enumerate(issues, 1):
        print(f"  {i}. {issue}")

# Show sample data
print("\n" + "="*70)
print("SAMPLE DATA (Last 5 records)")
print("="*70)
cursor.execute("""
    SELECT CITY, COUNTRY, TEMPERATURE, HUMIDITY, PRESSURE, 
           TO_CHAR(INGESTION_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') as INGESTED
    FROM weather_data 
    ORDER BY INGESTION_TIMESTAMP DESC 
    LIMIT 5
""")

print(f"\n{'CITY':<15} {'COUNTRY':<8} {'TEMP(°C)':<10} {'HUMIDITY':<10} {'PRESSURE':<10} {'INGESTED':<20}")
print("-" * 70)
for row in cursor.fetchall():
    print(f"{row[0]:<15} {row[1]:<8} {row[2]:<10.1f} {row[3]:<10} {row[4]:<10} {row[5]:<20}")

cursor.close()
conn.close()

print(f"\n✓ Validation completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")