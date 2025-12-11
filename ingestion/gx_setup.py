import great_expectations as gx
from great_expectations.core.yaml_handler import YAMLHandler
from dotenv import load_dotenv
import os
from pathlib import Path

# Load environment variables
load_dotenv()

print("Setting up Great Expectations with Snowflake...")

# Create gx directory if it doesn't exist
gx_dir = Path("gx")
gx_dir.mkdir(exist_ok=True)

# Initialize GX with a file-based context
context_root_dir = str(gx_dir.absolute())

try:
    # Create a new file-based context
    from great_expectations.data_context import FileDataContext
    
    context = FileDataContext.create(project_root_dir=context_root_dir)
    print(f"✓ Created new FileDataContext at: {context_root_dir}")
except Exception as e:
    print(f"Context might already exist: {e}")
    # Try to load existing context
    try:
        context = gx.get_context(project_root_dir=context_root_dir)
        print(f"✓ Loaded existing context from: {context_root_dir}")
    except Exception as e2:
        print(f"❌ Could not create or load context: {e2}")
        print("\nManual setup required. Creating configuration files...")
        
        # Create minimal config structure manually
        (gx_dir / "expectations").mkdir(exist_ok=True)
        (gx_dir / "checkpoints").mkdir(exist_ok=True)
        (gx_dir / "plugins").mkdir(exist_ok=True)
        (gx_dir / "uncommitted").mkdir(exist_ok=True)
        
        yaml = YAMLHandler()
        
        # Create great_expectations.yml
        config = {
            "config_version": 3.0,
            "datasources": {},
            "stores": {
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": "expectations/"
                    }
                },
                "validations_store": {
                    "class_name": "ValidationsStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": "uncommitted/validations/"
                    }
                },
                "checkpoint_store": {
                    "class_name": "CheckpointStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": "checkpoints/"
                    }
                }
            },
            "expectations_store_name": "expectations_store",
            "validations_store_name": "validations_store",
            "checkpoint_store_name": "checkpoint_store",
            "data_docs_sites": {}
        }
        
        with open(gx_dir / "great_expectations.yml", "w") as f:
            yaml.dump(config, f)
        
        print("✓ Created configuration structure")
        
        # Now try to load it
        context = gx.get_context(project_root_dir=context_root_dir)
        print(f"✓ Successfully loaded context")

# Snowflake connection
snowflake_config = {
    "account": os.getenv('SNOWFLAKE_ACCOUNT'),
    "user": os.getenv('SNOWFLAKE_USER'),
    "password": os.getenv('SNOWFLAKE_PASSWORD'),
    "database": os.getenv('SNOWFLAKE_DATABASE'),
    "schema": os.getenv('SNOWFLAKE_SCHEMA'),
    "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
}

connection_string = (
    f"snowflake://{snowflake_config['user']}:{snowflake_config['password']}"
    f"@{snowflake_config['account']}/{snowflake_config['database']}"
    f"/{snowflake_config['schema']}?warehouse={snowflake_config['warehouse']}"
)

datasource_name = "snowflake_weather"

print(f"\nConfiguring datasource: {datasource_name}")
print(f"Database: {snowflake_config['database']}")
print(f"Schema: {snowflake_config['schema']}")

try:
    # Try the fluent API first
    if hasattr(context, 'sources'):
        print("Using Fluent Datasources API...")
        datasource = context.sources.add_or_update_snowflake(
            name=datasource_name,
            connection_string=connection_string,
        )
        
        table_asset = datasource.add_table_asset(
            name="weather_data",
            table_name="weather_data",
        )
        
        print(f"\n✅ Successfully configured!")
        print(f"   - Datasource: {datasource_name}")
        print(f"   - Data Asset: weather_data")
    else:
        print("Fluent API not available, using direct SQL approach")
        print("You'll need to specify the table in your validation scripts")
        
        # Store connection info for later use
        conn_info_file = gx_dir / "snowflake_connection.txt"
        with open(conn_info_file, "w") as f:
            f.write(connection_string)
        
        print(f"\n✅ Connection info saved")
        print(f"   You can use SQL queries directly in validation")
        
except Exception as e:
    print(f"\n⚠️  Error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*60)
print("Next steps:")
print("1. Run: python create_expectations_simple.py")
print("2. Run: python validate_weather_simple.py")
print("="*60)