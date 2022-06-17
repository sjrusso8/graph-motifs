# Databricks notebook source
# MAGIC %md
# MAGIC ## Load Airport Data and Create a Graph
# MAGIC The nodes are the individual airports with the code being the 3 digit code. The edges the flights between the two airport codes. 
# MAGIC 
# MAGIC Keep in mind that GraphFrames needs to have a column named `id` on the 'nodes' dataframe, and `src` & `dst` on the 'edges' dataframe. The `id` values need to be the same values as in `src` & `dst`. 

# COMMAND ----------

import pyspark.sql.functions as F
import pandas as pd

from graphframes import *

spark.conf.set('spark.sql.shuffle.partitions','4')
spark.conf.set("spark.databricks.io.cache.enabled", "true")

# COMMAND ----------

# Load the airport detials as nodes
airports_url = "./data/airports.csv"

pd_airports = pd.read_csv(airports_url,header=None)

# clean up column names and filter out bad ids
df_nodes = (
    spark
      .createDataFrame(pd_airports)
      .select(
          F.col('4').alias('id'),
          F.col('1').alias('long_name'),
          F.col('2').alias('short_name'),
          F.col('3').alias('country'),
          F.col('4').alias('iata_code'),
          F.col('5').alias('icao_code'),
          F.col('6').alias('latitude'),
          F.col('7').alias('longitude'),
          F.col('11').alias('region')
      )
      .filter((F.col('id') != '\\N'))
)

df_nodes.cache().count()

# COMMAND ----------

# Load the flight path details
flight_paths_url = "./data/openflights_188591317_T_ONTIME.csv.gz"

pd_paths = pd.read_csv(flight_paths_url,low_memory=False)

# clean up column names 
df_edges = (
    spark
      .createDataFrame(pd_paths)
      .select(
          F.col('ORIGIN').alias('src'),
          F.col('DEST').alias('dst'),
          F.col('ORIGIN').alias('origin'),
          F.col('DEST').alias('dest'),
          F.col('FL_DATE').alias('date'),
          F.col('UNIQUE_CARRIER').alias('airline'),
          F.col('TAIL_NUM').alias('tail_number'),
          F.col('FL_NUM').alias('flight_number'),
          F.col('CRS_DEP_TIME').alias('time'),
          F.col('CRS_ARR_TIME').alias('arrival_time'),
          F.col('DISTANCE').alias('distance'),
          F.col('DEP_DELAY').alias('dept_deplay'),
          F.col('ARR_DELAY').alias('arr_deplay')
      )
)

df_edges.cache().count()

# COMMAND ----------

df_edges.createOrReplaceTempView('df_edges')

# COMMAND ----------

# Create the graph
airlines_graph = GraphFrame(df_nodes, df_edges)

# COMMAND ----------

# MAGIC %md
# MAGIC Count of vertices and edges

# COMMAND ----------

# Count of nodes (veritices)
airlines_graph.vertices.count()

# COMMAND ----------

# Count of edges 
airlines_graph.edges.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 1 : Flights coming into and out of an airport
# MAGIC 
# MAGIC You can replace the id filter to whatever airport code you perfer.

# COMMAND ----------

motifs = (
  airlines_graph
    .find("(incoming)-[flight_to]->(destination); (destination)-[flight_from]->(outgoing)")
)

display(motifs)

# COMMAND ----------

motifs.createOrReplaceTempView('motif')

display(
  spark.sql("""
      SELECT DISTINCT
        destination.id as airport,
        incoming.id as related_airport,
        flight_to.airline as related_airline,
        'Incoming Flights' as relationship
      FROM  motif
      WHERE destination.id == 'JFK'

      UNION

      SELECT DISTINCT
        destination.id as airport,
        outgoing.id as related_airport,
        flight_from.airline as related_airline,
        'Outgoing Flights' as relationship
      FROM  motif
      WHERE destination.id == 'JFK'
  """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2: Anonymous Edges and Nodes

# COMMAND ----------

motifs_anon_edge = (
  airlines_graph
    .find("(src_airport)-[]->(dst_airport)")
)

display(motifs_anon_edge)

# COMMAND ----------

motifs_anon_node = (
  airlines_graph
    .find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[cd]->(d)")
)

display(motifs_anon_node)

# COMMAND ----------

from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import IntegerType
from graphframes.examples import Graphs

from functools import reduce

g = Graphs(sqlContext).friends()  # Get example graph

chain4 = g.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[cd]->(d)")

# Query on sequence, with state (cnt)
#  (a) Define method for updating state given the next element of the motif.
sumFriends =  lambda cnt,relationship: when(relationship == "friend", cnt+1).otherwise(cnt)

#  (b) Use sequence operation to apply method to sequence of elements in motif.
#      In this case, the elements are the 3 edges.

condition =  reduce(lambda cnt,e: sumFriends(cnt, col(e).relationship), ["ab", "bc", "cd"], lit(0))

#  (c) Apply filter to DataFrame.
chainWith2Friends2 = chain4.where(condition >= 2)
chainWith2Friends2.show()

# COMMAND ----------

Graphs(sqlContext).gridIsingModel(n=10)

# COMMAND ----------

condition

# COMMAND ----------

Graphs(sqlContext).friends()
