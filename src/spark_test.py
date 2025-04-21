# %%
# Imports #

import os
import socket

from dotenv import load_dotenv
from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Doesn't need to be reachable; just forces IP resolution
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()


driver_ip = get_local_ip()
print(f"Detected driver IP: {driver_ip}")

# %%
# Variables #

grandparent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(grandparent_dir, ".env")
if os.path.exists(dotenv_path):
    print(f"Loading environment variables from {dotenv_path}")
    load_dotenv(dotenv_path)

# Set the URL of the Spark master (use the actual service or node IP)
spark_master_url = f"spark://{os.getenv("SPARK_MASTER_IP")}:{os.getenv("SPARK_MASTER_API_PORT")}"  # Replace with your Spark master's address
print(f"Spark master URL: {spark_master_url}")

# Set JAVA_HOME and update PATH inside the script
os.environ["JAVA_HOME"] = r"C:\Program Files\OpenJDK\jdk-22.0.2"
os.environ["PATH"] = rf"{os.environ['JAVA_HOME']}\bin;{os.environ['PATH']}"


# %%

# Create a Spark session that connects to the Spark cluster
spark = (
    SparkSession.builder.master(spark_master_url)
    .appName("Spark Scalability Test")
    .config("spark.ui.showConsoleProgress", "true")
    .config("spark.driver.host", driver_ip)
    .config("spark.driver.bindAddress", "0.0.0.0")
    .getOrCreate()
)

# %%

# Create an RDD with some sample data
data = range(100000)  # Large data set for testing scalability
rdd = spark.sparkContext.parallelize(
    data, numSlices=10
)  # Parallelize data with 10 partitions

# Perform a map operation (example: square each number) to test the distribution
result = rdd.map(lambda x: x * x).collect()

# Print the first 10 results to verify
print(result[:10])
print(f"Total results: {len(result)}")


# %%
# Stop the Spark session after the job is complete
spark.stop()

print("Job completed successfully.")


# %%
