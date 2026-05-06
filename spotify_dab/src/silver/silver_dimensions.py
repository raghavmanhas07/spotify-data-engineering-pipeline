# Databricks notebook source
# MAGIC %md
# MAGIC ###DimUser

# COMMAND ----------

df = spark.read.format("parquet")\
.load('abfss://bronze@raghavazureproject.dfs.core.windows.net/DimUser')


# COMMAND ----------

display(df) 

# COMMAND ----------

# MAGIC %md
# MAGIC ####AutoLoader

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys
project_pth = os.path.join(os.getcwd(), '..', '..')
sys.path.append(project_pth)
from utils.transformations import reusable

# COMMAND ----------

# DBTITLE 1,Cell 5
df_user = spark.readStream.format('cloudFiles') \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "abfss://silver@raghavazureproject.dfs.core.windows.net/DimUser/checkpoint") \
    .option('cloudFiles.schemaEvolutionMode', 'addNewColumns') \
    .load('abfss://bronze@raghavazureproject.dfs.core.windows.net/DimUser')

# COMMAND ----------

display(df_user, checkpointLocation = "abfss://silver@raghavazureproject.dfs.core.windows.net/DimUser/checkpoint_display1")

# COMMAND ----------

df_user = df_user.withColumn("user_name", upper(col("user_name")))

# COMMAND ----------

df_user_obj = reusable()

# COMMAND ----------

df_user.writeStream.format('delta')\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@raghavazureproject.dfs.core.windows.net/DimUser/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@raghavazureproject.dfs.core.windows.net/DimUser/data")\
    .toTable("spotify_cata.silver.DimUser")

# COMMAND ----------

# MAGIC %md
# MAGIC ###DimArtist

# COMMAND ----------

df_art = spark.readStream.format('cloudFiles') \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "abfss://silver@raghavazureproject.dfs.core.windows.net/DimArtist/checkpoint") \
    .option('cloudFiles.schemaEvolutionMode', 'addNewColumns') \
    .load('abfss://bronze@raghavazureproject.dfs.core.windows.net/DimArtist')

# COMMAND ----------

display(df_art, checkpointLocation = "abfss://silver@raghavazureproject.dfs.core.windows.net/DimArtist/checkpoint_display2")

# COMMAND ----------

df_art.writeStream.format('delta')\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@raghavazureproject.dfs.core.windows.net/DimArtist/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@raghavazureproject.dfs.core.windows.net/DimArtist/data")\
    .toTable("spotify_cata.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC ###DimTrack

# COMMAND ----------

df_track = spark.readStream.format('cloudFiles') \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "abfss://silver@raghavazureproject.dfs.core.windows.net/DimTrack/checkpoint") \
    .option('cloudFiles.schemaEvolutionMode', 'addNewColumns') \
    .load('abfss://bronze@raghavazureproject.dfs.core.windows.net/DimTrack')

# COMMAND ----------

display(df_track, checkpointLocation = "abfss://silver@raghavazureproject.dfs.core.windows.net/DimTrack/checkpoint_display1")

# COMMAND ----------

df_track = df_track.withColumn("durationFlag", when(col('duration_sec') < 150, "low")\
                                              .when(col('duration_sec') < 300, "medium")\
                                              .otherwise("high"))

df_track = df_track.withColumn("track_name", regexp_replace(col("track_name"), '-', ' '))
df_track = reusable().dropColumns(df_track, ['_rescued_data'])
display(df_track, checkpointLocation = "abfss://silver@raghavazureproject.dfs.core.windows.net/DimTrack/checkpoint_display5")

# COMMAND ----------

df_track.writeStream.format('delta')\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@raghavazureproject.dfs.core.windows.net/DimTrack/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@raghavazureproject.dfs.core.windows.net/DimTrack/data")\
    .toTable("spotify_cata.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC ###DimDate

# COMMAND ----------

df_date = spark.readStream.format('cloudFiles') \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "abfss://silver@raghavazureproject.dfs.core.windows.net/DimDate/checkpoint") \
    .option('cloudFiles.schemaEvolutionMode', 'addNewColumns') \
    .load('abfss://bronze@raghavazureproject.dfs.core.windows.net/DimDate')

# COMMAND ----------

display(df_date, checkpointLocation = "abfss://silver@raghavazureproject.dfs.core.windows.net/DimDate/checkpoint_display1")

# COMMAND ----------

df_date = reusable().dropColumns(df_date, ['_rescued_data'])

# COMMAND ----------

df_date.writeStream.format('delta')\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@raghavazureproject.dfs.core.windows.net/DimDate/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@raghavazureproject.dfs.core.windows.net/DimDate/data")\
    .toTable("spotify_cata.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC ###FactStream

# COMMAND ----------

df_fact = spark.readStream.format('cloudFiles') \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "abfss://silver@raghavazureproject.dfs.core.windows.net/FactStream/checkpoint") \
    .option('cloudFiles.schemaEvolutionMode', 'addNewColumns') \
    .load('abfss://bronze@raghavazureproject.dfs.core.windows.net/FactStream')

# COMMAND ----------

display(df_fact, checkpointLocation = "abfss://silver@raghavazureproject.dfs.core.windows.net/FactStream/checkpoint_display1")

# COMMAND ----------

df_fact = reusable().dropColumns(df_fact, ['_rescued_data'])

# COMMAND ----------

df_fact.writeStream.format('delta')\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@raghavazureproject.dfs.core.windows.net/FactStream/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@raghavazureproject.dfs.core.windows.net/FactStream/data")\
    .toTable("spotify_cata.silver.FactStream")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spotify_cata.gold.dimtrack
# MAGIC where '__END_AT_' is not null;