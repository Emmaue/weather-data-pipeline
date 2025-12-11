"""
Test the data quality validation with both good and bad data
This demonstrates how the validation prevents bad data from being ingested
"""
from weather_with_pre_validation import WeatherDataValidator

print("="*70)
print("TESTING DATA QUALITY GATES")
print("="*70)

validator = WeatherDataValidator()

# Test Case 1: Good data (should pass)
print("\n1. Testing VALID weather data...")
good_data = {
    'name': 'London',
    'sys': {'country': 'GB'},
    'main': {
        'temp': 15.5,
        'feels_like': 14.0,
        'temp_min': 13.0,
        'temp_max': 17.0,
        'pressure': 1013,
        'humidity': 65
    },
    'weather': [{'main': 'Clear', 'description': 'clear sky'}],
    'wind': {'speed': 5.5, 'deg': 180},
    'clouds': {'all': 20},
    'dt': 1638360000
}

is_valid, errors, warnings = validator.validate(good_data)
print(f"   Result: {'✅ PASS' if is_valid else '❌ FAIL'}")
if errors:
    print(f"   Errors: {errors}")
if warnings:
    print(f"   Warnings: {warnings}")

# Test Case 2: Extreme temperature (should fail)
print("\n2. Testing INVALID temperature (-150°C)...")
bad_temp_data = {
    'name': 'Arctic City',
    'sys': {'country': 'XX'},
    'main': {
        'temp': -150.0,  # Impossible temperature
        'feels_like': -155.0,
        'temp_min': -160.0,
        'temp_max': -145.0,
        'pressure': 1013,
        'humidity': 65
    },
    'weather': [{'main': 'Clear', 'description': 'clear sky'}],
    'wind': {'speed': 5.5, 'deg': 180},
    'clouds': {'all': 20},
    'dt': 1638360000
}

is_valid, errors, warnings = validator.validate(bad_temp_data)
print(f"   Result: {'✅ PASS' if is_valid else '❌ FAIL'}")
if errors:
    for error in errors:
        print(f"   ❌ {error}")
if warnings:
    for warning in warnings:
        print(f"   ⚠️  {warning}")

# Test Case 3: Invalid humidity (should fail)
print("\n3. Testing INVALID humidity (150%)...")
bad_humidity_data = {
    'name': 'Humid City',
    'sys': {'country': 'US'},
    'main': {
        'temp': 25.0,
        'feels_like': 26.0,
        'temp_min': 24.0,
        'temp_max': 27.0,
        'pressure': 1013,
        'humidity': 150  # Invalid: >100%
    },
    'weather': [{'main': 'Rain', 'description': 'heavy rain'}],
    'wind': {'speed': 10.0, 'deg': 90},
    'clouds': {'all': 100},
    'dt': 1638360000
}

is_valid, errors, warnings = validator.validate(bad_humidity_data)
print(f"   Result: {'✅ PASS' if is_valid else '❌ FAIL'}")
if errors:
    for error in errors:
        print(f"   ❌ {error}")

# Test Case 4: Missing required fields (should fail)
print("\n4. Testing MISSING required fields...")
incomplete_data = {
    'name': 'Mystery City',
    'sys': {},  # Missing country
    'main': {
        'temp': 20.0
        # Missing humidity, pressure
    },
    'weather': [{'main': 'Clear'}],
    'dt': 1638360000
}

is_valid, errors, warnings = validator.validate(incomplete_data)
print(f"   Result: {'✅ PASS' if is_valid else '❌ FAIL'}")
if errors:
    for error in errors:
        print(f"   ❌ {error}")

# Test Case 5: Unusual but valid data (should pass with warnings)
print("\n5. Testing UNUSUAL but valid data (very cold -45°C)...")
unusual_data = {
    'name': 'Siberia',
    'sys': {'country': 'RU'},
    'main': {
        'temp': -45.0,  # Extreme but possible
        'feels_like': -50.0,
        'temp_min': -48.0,
        'temp_max': -42.0,
        'pressure': 1040,
        'humidity': 75
    },
    'weather': [{'main': 'Clear', 'description': 'clear sky'}],
    'wind': {'speed': 2.0, 'deg': 0},
    'clouds': {'all': 0},
    'dt': 1638360000
}

is_valid, errors, warnings = validator.validate(unusual_data)
print(f"   Result: {'✅ PASS' if is_valid else '❌ FAIL'}")
if errors:
    for error in errors:
        print(f"   ❌ {error}")
if warnings:
    for warning in warnings:
        print(f"   ⚠️  {warning}")

# Test Case 6: Hurricane conditions (should pass with warnings)
print("\n6. Testing HURRICANE conditions...")
hurricane_data = {
    'name': 'Miami',
    'sys': {'country': 'US'},
    'main': {
        'temp': 28.0,
        'feels_like': 32.0,
        'temp_min': 27.0,
        'temp_max': 29.0,
        'pressure': 960,  # Very low pressure
        'humidity': 95
    },
    'weather': [{'main': 'Storm', 'description': 'hurricane'}],
    'wind': {'speed': 40.0, 'deg': 90},  # Hurricane force
    'clouds': {'all': 100},
    'dt': 1638360000
}

is_valid, errors, warnings = validator.validate(hurricane_data)
print(f"   Result: {'✅ PASS' if is_valid else '❌ FAIL'}")
if errors:
    for error in errors:
        print(f"   ❌ {error}")
if warnings:
    for warning in warnings:
        print(f"   ⚠️  {warning}")

print("\n" + "="*70)
print("KEY CONCEPT: Data Quality Gates")
print("="*70)
print("""
✅ Good data → Validated → Ingested
❌ Bad data → Validation fails → NOT ingested → No bad data in warehouse
⚠️  Unusual data → Validated with warnings → Ingested with flag

This prevents data quality issues BEFORE they reach your warehouse!
""")