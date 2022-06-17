# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Load Movie Data
# MAGIC 
# MAGIC This data is a copy of the Neo4j dataset used in their demo.
# MAGIC 
# MAGIC Source: https://github.com/neo4j-graph-examples/recommendations
# MAGIC 
# MAGIC ### Import process
# MAGIC I have my data saved to an Azure storage account that is mounted to a Databricks instance.
# MAGIC 
# MAGIC 1. Read in data as text
# MAGIC 2. Remove the forward slashed
# MAGIC 3. Seperate the data into nodes and relations by the `type` column
# MAGIC 4. Apply a schema to both dataframes by reading the rows of the dataframe as JSON
# MAGIC 5. Update the column names to have `id`, `src`, `dst` as required by GraphFrames
# MAGIC 6. Save the data as a Delta table (a fancy parquet file) then register them as a Hive table

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

movie_data = (
    spark
        .read
        .format('text')
        .load('/mnt/datalake/data/movie-graph/raw/movie-data.json')
        .withColumn('json_clean', F.regexp_replace(F.col('value'), "\\\\", ''))
        .drop('value')
)

movie_nodes = (
    movie_data
        .filter("json_clean:type == 'node'")
)

movie_relations = (
    movie_data
        .filter("json_clean:type == 'relationship'")
)

# COMMAND ----------

movie_nodes_clean = (
        spark
            .read.json(movie_nodes.rdd.map(lambda row: row.json_clean))
            .withColumnRenamed('labels', 'node_type')
            .drop('type')
    )

movie_relations_clean = (
        spark
            .read.json(movie_relations.rdd.map(lambda row: row.json_clean))
            .select(
                F.col('id').alias('relation_id'),
                F.col('start.id').alias('src'),
                F.col('end.id').alias('dst'),
                F.col('label').alias('relation_type'),
                'properties',
                'start',
                'end'
            )
    )

# COMMAND ----------

(
    movie_nodes_clean
        .write
        .format('delta')
        .mode('overwrite')
        .save('/mnt/datalake/data/movie-graph/tables/nodes')
)

# COMMAND ----------

(
    movie_relations_clean
        .write
        .format('delta')
        .mode('overwrite')
        .save('/mnt/datalake/data/movie-graph/tables/edges')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS movies;
# MAGIC 
# MAGIC CREATE TABLE movies.nodes USING DELTA LOCATION '/mnt/datalake/data/movie-graph/tables/nodes';
# MAGIC 
# MAGIC CREATE TABLE movies.edges USING DELTA LOCATION '/mnt/datalake/data/movie-graph/tables/edges';
