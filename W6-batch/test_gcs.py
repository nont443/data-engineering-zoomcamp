#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("gcs-test").getOrCreate()
print(spark.sparkContext.uiWebUrl)

df = spark.read.parquet("gs://zoomcamp-486023-homework6/yellow_tripdata_2025-11.parquet")
df.limit(5).show()


# In[ ]:




