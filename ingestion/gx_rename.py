import great_expectations as gx
import os

print("Initializing Great Expectations...")

# Create a Data Context (this initializes GX in your project)
try:
    context = gx.get_context()
    print("✅ Great Expectations initialized successfully!")
    print(f"   Context root: {context.root_directory}")
    print(f"   GX version: {gx.__version__}")
except Exception as e:
    print(f"⚠️  Error: {e}")
    print("\nTrying to create new context...")
    
    try:
        # Create context in current directory
        context = gx.data_context.FileDataContext.create(project_root_dir=".")
        print("✅ Created new GX context!")
    except Exception as e2:
        print(f"❌ Failed: {e2}")