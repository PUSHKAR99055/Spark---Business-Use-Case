from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Broadcast Variable Update Example") \
    .getOrCreate()

# Sample VIP Customer DataFrame
def load_vip_customers():
    # This would typically be loaded from a database or external file
    data = [("customer_1", 10), ("customer_2", 15), ("customer_3", 20)]
    columns = ["customer_id", "discount_percentage"]
    return spark.createDataFrame(data, columns)

# Broadcast the VIP customer list
vip_customers_df = load_vip_customers()
vip_customers_broadcast = spark.sparkContext.broadcast(vip_customers_df.collectAsMap())

# Function to process transactions with the latest VIP customer list
def process_transactions(transactions_df):
    vip_customers_map = vip_customers_broadcast.value
    broadcast_df = spark.createDataFrame(vip_customers_map.items(), ["customer_id", "discount_percentage"])

    result_df = transactions_df.join(broadcast_df, on="customer_id", how="left")
    result_df = result_df.withColumn("final_price", 
                                      col("amount") * (1 - col("discount_percentage") / 100))
    return result_df

# Sample Transactions DataFrame
transactions_data = [("customer_1", 100), ("customer_4", 200)]
transactions_columns = ["customer_id", "amount"]
transactions_df = spark.createDataFrame(transactions_data, transactions_columns)

# Periodically update the broadcast variable
def update_broadcast_variable():
    global vip_customers_broadcast
    new_vip_customers_df = load_vip_customers()
    new_vip_customers_map = new_vip_customers_df.collectAsMap()
    
    # Unpersist the old broadcast variable
    vip_customers_broadcast.unpersist()
    
    # Update with the new broadcast variable
    vip_customers_broadcast = spark.sparkContext.broadcast(new_vip_customers_map)
    print("VIP Customer Broadcast Variable Updated")

# Main processing loop
while True:
    # Process transactions
    processed_df = process_transactions(transactions_df)
    processed_df.show()

    # Update the broadcast variable periodically (e.g., every hour)
    time.sleep(3600)  # Sleep for 1 hour
    update_broadcast_variable()
