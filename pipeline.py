import subprocess

print("Starting data pipeline...\n")

# Step 1: Load raw data from MySQL
subprocess.run(["python", "scripts/load_data.py"], check=True)
print("Data loaded from database.")

# Step 2: Preprocess the data
subprocess.run(["python", "scripts/preprocess.py"], check=True)
print("Data preprocessing complete.")

# Step 3: Feature engineering
subprocess.run(["python", "scripts/feature_engineering.py"], check=True)
print("Feature engineering complete.")

print("\nPipeline completed successfully. Final dataset saved to /output folder.")
