# import dlt
# from pyspark.sql.functions import *
# #creating an end to end basic pipeline

# #Staging area

# @dlt.table(
#     name = "staging_orders"
# )

# def staging_orders():
#     df = spark.readStream.table("dltyash.source.orders")
#     return df

# #transformed area

# @dlt.table(
#     name = "transformed_orders"
# )
# def transformed_orders():
#     df = spark.readStream.table("staging_orders")
#     df = df.withColumn("order_status", lower(col("order_status")))
#     return df



# # Creating Aggregated area
# @dlt.table(
#     name = "aggregated_table"
# )
# def aggregated_table():
#     df = spark.readStream.table("transformed_orders")
#     df = df.groupBy("order_status").count()
#     return df