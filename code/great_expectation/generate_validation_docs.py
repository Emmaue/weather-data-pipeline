"""
Generate HTML documentation for data quality validation
Similar to dbt docs but for data quality checks
"""
import snowflake.connector
from dotenv import load_dotenv
import os
from datetime import datetime
import json

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

print("Generating Data Quality Documentation...")

# Run all validations and collect results
validation_results = []

# Check 1: Row count
cursor.execute("SELECT COUNT(*) FROM weather_data")
row_count = cursor.fetchone()[0]
validation_results.append({
    "check": "Table Row Count",
    "description": "Verify that the table contains data",
    "expectation": "Row count > 0",
    "result": row_count,
    "status": "PASS" if row_count > 0 else "FAIL"
})

# Check 2: NULL values
cursor.execute("""
    SELECT 
        COUNT(*) - COUNT(CITY) as null_cities,
        COUNT(*) - COUNT(TEMPERATURE) as null_temps,
        COUNT(*) - COUNT(HUMIDITY) as null_humidity
    FROM weather_data
""")
nulls = cursor.fetchone()
total_nulls = sum(nulls)
validation_results.append({
    "check": "NULL Value Check",
    "description": "Ensure critical columns have no NULL values",
    "expectation": "0 NULL values in CITY, TEMPERATURE, HUMIDITY",
    "result": f"Cities: {nulls[0]}, Temps: {nulls[1]}, Humidity: {nulls[2]}",
    "status": "PASS" if total_nulls == 0 else "FAIL"
})

# Check 3: Temperature range
cursor.execute("SELECT COUNT(*) FROM weather_data WHERE TEMPERATURE < -50 OR TEMPERATURE > 60")
invalid_temps = cursor.fetchone()[0]
validation_results.append({
    "check": "Temperature Range",
    "description": "Validate temperature values are realistic",
    "expectation": "Temperature between -50°C and 60°C",
    "result": f"{invalid_temps} invalid values",
    "status": "PASS" if invalid_temps == 0 else "FAIL"
})

# Check 4: Humidity range
cursor.execute("SELECT COUNT(*) FROM weather_data WHERE HUMIDITY < 0 OR HUMIDITY > 100")
invalid_humidity = cursor.fetchone()[0]
validation_results.append({
    "check": "Humidity Range",
    "description": "Validate humidity percentage is valid",
    "expectation": "Humidity between 0% and 100%",
    "result": f"{invalid_humidity} invalid values",
    "status": "PASS" if invalid_humidity == 0 else "FAIL"
})

# Check 5: Pressure range
cursor.execute("SELECT COUNT(*) FROM weather_data WHERE PRESSURE < 800 OR PRESSURE > 1100")
invalid_pressure = cursor.fetchone()[0]
validation_results.append({
    "check": "Pressure Range",
    "description": "Validate atmospheric pressure is realistic",
    "expectation": "Pressure between 800-1100 millibars",
    "result": f"{invalid_pressure} invalid values",
    "status": "PASS" if invalid_pressure == 0 else "FAIL"
})

# Check 6: City name length
cursor.execute("SELECT COUNT(*) FROM weather_data WHERE LENGTH(CITY) < 2 OR LENGTH(CITY) > 50")
invalid_cities = cursor.fetchone()[0]
validation_results.append({
    "check": "City Name Length",
    "description": "Ensure city names are reasonable length",
    "expectation": "City name length between 2 and 50 characters",
    "result": f"{invalid_cities} invalid values",
    "status": "PASS" if invalid_cities == 0 else "FAIL"
})

# Check 7: Country code format
cursor.execute("SELECT COUNT(*) FROM weather_data WHERE LENGTH(COUNTRY) != 2")
invalid_countries = cursor.fetchone()[0]
validation_results.append({
    "check": "Country Code Format",
    "description": "Validate ISO 2-letter country codes",
    "expectation": "Country code is exactly 2 characters",
    "result": f"{invalid_countries} invalid values",
    "status": "PASS" if invalid_countries == 0 else "FAIL"
})

# Get data statistics
cursor.execute("""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT CITY) as unique_cities,
        COUNT(DISTINCT COUNTRY) as unique_countries,
        ROUND(AVG(TEMPERATURE), 2) as avg_temp,
        ROUND(MIN(TEMPERATURE), 2) as min_temp,
        ROUND(MAX(TEMPERATURE), 2) as max_temp,
        ROUND(AVG(HUMIDITY), 2) as avg_humidity,
        MIN(INGESTION_TIMESTAMP) as first_record,
        MAX(INGESTION_TIMESTAMP) as last_record
    FROM weather_data
""")
stats = cursor.fetchone()

# Calculate summary
total_checks = len(validation_results)
passed_checks = sum(1 for v in validation_results if v["status"] == "PASS")
failed_checks = total_checks - passed_checks
success_rate = (passed_checks / total_checks * 100) if total_checks > 0 else 0

# Generate HTML
html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Weather Data Quality Report</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }}
        
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 40px;
            text-align: center;
        }}
        
        .header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
        }}
        
        .header .timestamp {{
            opacity: 0.9;
            font-size: 0.9em;
        }}
        
        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            padding: 40px;
            background: #f8f9fa;
        }}
        
        .summary-card {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            text-align: center;
        }}
        
        .summary-card .value {{
            font-size: 2.5em;
            font-weight: bold;
            margin: 10px 0;
        }}
        
        .summary-card .label {{
            color: #666;
            font-size: 0.9em;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}
        
        .pass {{ color: #10b981; }}
        .fail {{ color: #ef4444; }}
        
        .section {{
            padding: 40px;
        }}
        
        .section h2 {{
            font-size: 1.8em;
            margin-bottom: 20px;
            color: #1f2937;
            border-bottom: 3px solid #667eea;
            padding-bottom: 10px;
        }}
        
        .validation-table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }}
        
        .validation-table th {{
            background: #f3f4f6;
            padding: 15px;
            text-align: left;
            font-weight: 600;
            color: #374151;
            border-bottom: 2px solid #e5e7eb;
        }}
        
        .validation-table td {{
            padding: 15px;
            border-bottom: 1px solid #e5e7eb;
        }}
        
        .validation-table tr:hover {{
            background: #f9fafb;
        }}
        
        .status-badge {{
            padding: 5px 12px;
            border-radius: 20px;
            font-weight: 600;
            font-size: 0.85em;
            text-transform: uppercase;
        }}
        
        .status-badge.pass {{
            background: #d1fae5;
            color: #065f46;
        }}
        
        .status-badge.fail {{
            background: #fee2e2;
            color: #991b1b;
        }}
        
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }}
        
        .stat-item {{
            background: #f9fafb;
            padding: 20px;
            border-radius: 8px;
            border-left: 4px solid #667eea;
        }}
        
        .stat-item .stat-label {{
            color: #6b7280;
            font-size: 0.9em;
            margin-bottom: 5px;
        }}
        
        .stat-item .stat-value {{
            font-size: 1.5em;
            font-weight: bold;
            color: #1f2937;
        }}
        
        .footer {{
            background: #f3f4f6;
            padding: 20px 40px;
            text-align: center;
            color: #6b7280;
            font-size: 0.9em;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🌤️ Weather Data Quality Report</h1>
            <p class="timestamp">Generated on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}</p>
        </div>
        
        <div class="summary">
            <div class="summary-card">
                <div class="label">Total Checks</div>
                <div class="value">{total_checks}</div>
            </div>
            <div class="summary-card">
                <div class="label">Passed</div>
                <div class="value pass">{passed_checks}</div>
            </div>
            <div class="summary-card">
                <div class="label">Failed</div>
                <div class="value fail">{failed_checks}</div>
            </div>
            <div class="summary-card">
                <div class="label">Success Rate</div>
                <div class="value {'pass' if success_rate == 100 else 'fail'}">{success_rate:.1f}%</div>
            </div>
        </div>
        
        <div class="section">
            <h2>📊 Validation Results</h2>
            <table class="validation-table">
                <thead>
                    <tr>
                        <th>Check</th>
                        <th>Description</th>
                        <th>Expectation</th>
                        <th>Result</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
"""

for v in validation_results:
    html_content += f"""
                    <tr>
                        <td><strong>{v['check']}</strong></td>
                        <td>{v['description']}</td>
                        <td>{v['expectation']}</td>
                        <td>{v['result']}</td>
                        <td><span class="status-badge {v['status'].lower()}">{v['status']}</span></td>
                    </tr>
"""

html_content += f"""
                </tbody>
            </table>
        </div>
        
        <div class="section">
            <h2>📈 Data Statistics</h2>
            <div class="stats-grid">
                <div class="stat-item">
                    <div class="stat-label">Total Records</div>
                    <div class="stat-value">{stats[0]:,}</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">Unique Cities</div>
                    <div class="stat-value">{stats[1]}</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">Unique Countries</div>
                    <div class="stat-value">{stats[2]}</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">Average Temperature</div>
                    <div class="stat-value">{stats[3]}°C</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">Min Temperature</div>
                    <div class="stat-value">{stats[4]}°C</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">Max Temperature</div>
                    <div class="stat-value">{stats[5]}°C</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">Average Humidity</div>
                    <div class="stat-value">{stats[6]}%</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">First Record</div>
                    <div class="stat-value" style="font-size: 1em;">{stats[7].strftime('%Y-%m-%d %H:%M')}</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">Last Record</div>
                    <div class="stat-value" style="font-size: 1em;">{stats[8].strftime('%Y-%m-%d %H:%M')}</div>
                </div>
            </div>
        </div>
        
        <div class="footer">
            <p>Weather Data Quality Pipeline | Database: {os.getenv('SNOWFLAKE_DATABASE')} | Schema: {os.getenv('SNOWFLAKE_SCHEMA')}</p>
        </div>
    </div>
</body>
</html>
"""

# Save HTML file
output_file = "data_quality_report.html"
with open(output_file, "w", encoding="utf-8") as f:
    f.write(html_content)

cursor.close()
conn.close()

print(f"✅ Documentation generated successfully!")
print(f"📄 File: {output_file}")
print(f"🌐 Open it in your browser to view the report")

# Try to open in browser
import webbrowser
import os
file_path = os.path.abspath(output_file)
webbrowser.open('file://' + file_path)
print(f"🚀 Opening in browser...")