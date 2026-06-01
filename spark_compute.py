import sys
import os

print("⚡ ==========================================================")
print("🌐 INITIALIZING INLINE-CONFIGURED DISTRIBUTED ENGINE...")
print("==============================================================")

# 🚨 THE DIRECT INJECTION: Explicitly point to the container's pre-installed Spark paths
sys.path.insert(0, '/usr/local/spark/python')
sys.path.insert(0, '/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip') # Maps the core communication engine

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when
except ModuleNotFoundError:
    # If the exact py4j zip version number differs slightly on the newest image, fall back to searching the folder dynamically
    import glob
    sys.path.insert(0, glob.glob('/usr/local/spark/python/lib/py4j-*.zip')[0])
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when

# Boot up the isolated containerized distributed Spark cluster instance
spark = SparkSession.builder \
    .appName("HighVolumeDataProcessor") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

file_name = "massive_raw_transactions.csv"

try:
    print("🚀 Distributed cluster processing nodes successfully initialized.")
    print("📝 Generating high-volume transaction stream dataset...")
    with open(file_name, "w") as f:
        f.write("transaction_id,amount,status\n")
        for i in range(1, 25001):
            status_pick = "SUCCESS" if i % 2 == 0 else "FAILED" if i % 3 == 0 else "ERROR"
            f.write(f"TXN_{100000+i},{round(i * 1.37, 2)},{status_pick}\n")

    print(f"✅ Mock dataset finalized: '{file_name}' written to storage.")

    # Load dataset into Spark Distributed memory matrix nodes
    print("📥 Ingesting dataset into distributed processing shards...")
    df = spark.read.csv(file_name, header=True, inferSchema=True)
    
    print("\n📊 Distributed Cluster Partition Metrics:")
    print(f"  🔹 Total Rows Discovered in Partition: {df.count()}")
    print("  🔹 Schema Matrix Inferred by Spark:")
    df.printSchema()

    # Execute parallel transformations across partitions simultaneously
    print("⚡ Executing parallel metrics processing transformations...")
    processed_df = df.withColumn("processing_tax", col("amount") * 0.05) \
                     .withColumn("risk_profile", when(col("status") == "SUCCESS", "LOW")
                                                .when(col("status") == "FAILED", "MEDIUM")
                                                .otherwise("HIGH"))

    # Execute a distributed GroupBy aggregation computation
    print("\n📊 === SPARK DISTRIBUTED COMPUTATION SUMMARY REPORT ===")
    summary_df = processed_df.groupBy("risk_profile").count()
    summary_df.show()
    print("\n🖥️ === SPARK VISUAL WEB UI SERVER ACTIVE ===")
    print("  👉 Open your web browser and go to: http://localhost:8888/proxy/4040/jobs/")
    print("-----------------------------------------------------------------")
    input("⏸️ Cluster frozen for visual inspection. Press ENTER in this terminal to shut it down...")


    print("✅ Parallel distributed computation lifecycle finished successfully!")

except Exception as e:
    print(f"❌ Spark cluster operation failed: {e}")

finally:
    if os.path.exists(file_name):
        os.remove(file_name)
    spark.stop()
