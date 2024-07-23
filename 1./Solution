from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, broadcast
from pyspark.sql.types import StructType, StructField, StringType

def compute(spark, source_df):
    # Define schema for column_change DataFrame
    column_change_schema = StructType([
        StructField("new_column", StringType(), True),
        StructField("old_column", StringType(), True),
        StructField("dataset_name", StringType(), True)  # Field to have null values
    ])

    # Create an empty DataFrame with the defined schema
    column_change = spark.createDataFrame([], column_change_schema)

    # Filter and select relevant columns from source_df
    column_change = source_df.filter((col("status") == 'CHANGE-IN-PROGRESS') & (col("new_column_name").isNotNull())) \
                             .select("new_column_name", "request_column")

    # Rename columns
    column_change = column_change.withColumnRenamed("request_column", "column_name") \
                                 .withColumnRenamed("new_column_name", "rename_value")

    # Add dataset_name column with null values
    column_change = column_change.withColumn("dataset_name", lit(None).cast(StringType()))

    # Create a lookup DataFrame for existing records with status 'COLUMN_ADDED'
    existing_records = source_df.filter(col("status") == 'COLUMN_ADDED') \
                                .select(col("request_column").alias("column_name"), "new_column_name")

    # Broadcast the existing_records DataFrame for efficient joins
    broadcast_existing_records = broadcast(existing_records)

    # Collect data for processing
    source_rows = source_df.collect()
    column_change_rows = column_change.collect()

    # Process each row
    for row in source_rows:
        column_name = row['request_column']
        rename_value = row['new_column_name']
        status = row['status']

        if status == 'COLUMN_ADDED':
            # Join column_change with existing_records to find matches
            existing_record = broadcast_existing_records.filter(col("new_column_name") == rename_value)

            if existing_record.count() > 0:
                # Get the original column name
                original_column_name = existing_record.select("column_name").collect()[0][0]
                # Add a new record with the original column name
                new_row = spark.createDataFrame([(original_column_name, rename_value, None)], column_change_schema)
                column_change = column_change.union(new_row)

    return column_change

# Example usage
if __name__ == "__main__":
    spark = SparkSession.builder.appName("ColumnChangeProcessing").getOrCreate()
    
    # Sample source DataFrame creation (replace this with your actual data)
    data = [
        ("CHANGE-IN-PROGRESS", "a", "b"),
        ("COLUMN_ADDED", "a", "b"),
        ("COLUMN_ADDED", "b", "c"),
        ("CHANGE-IN-PROGRESS", "c", "d")
    ]
    schema = StructType([
        StructField("status", StringType(), True),
        StructField("request_column", StringType(), True),
        StructField("new_column_name", StringType(), True)
    ])
    source_df = spark.createDataFrame(data, schema)

    # Call the compute function
    result_df = compute(spark, source_df)
    
    # Show the result
    result_df.show()
