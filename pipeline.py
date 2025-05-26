import dlt

# Sample data to be loaded
data = [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"}]

# Create a dlt pipeline
table_pipeline = dlt.pipeline(
    pipeline_name="static2duck", destination="duckdb", dataset_name="alice_and_bob"
)

# Load the data to the "users" table
load_info = table_pipeline.run(data, table_name="users")
print(load_info)

# Print the row counts for each table that was loaded in the last run of the pipeline
print("\nNumber of new rows loaded into each table: ", table_pipeline.last_trace.last_normalize_info.row_counts)
