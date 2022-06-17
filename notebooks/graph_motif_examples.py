# Databricks notebook source
# MAGIC %md
# MAGIC ## Load movie data as graph
# MAGIC The nodes are the individual airports with the code being the 3 digit code. The edges the flights between the two airport codes. 
# MAGIC 
# MAGIC Keep in mind that GraphFrames needs to have a column named `id` on the 'nodes' dataframe, and `src` & `dst` on the 'edges' dataframe. The `id` values need to be the same values as in `src` & `dst`. 

# COMMAND ----------

import pyspark.sql.functions as F

from graphframes import *

spark.conf.set('spark.sql.shuffle.partitions','4')

# COMMAND ----------

# load the nodes with a column named as 'id'
df_nodes = spark.table('movies.nodes')

# load the edges with columns named as 'src' and 'dst'
df_edges = spark.table('movies.edges')

# Create the graph of nodes and edges
movie_graph = GraphFrame(df_nodes, df_edges)

# COMMAND ----------

# MAGIC %md
# MAGIC Display the nodes (vertices) and edges

# COMMAND ----------

display(movie_graph.vertices)

# COMMAND ----------

display(movie_graph.edges)

# COMMAND ----------

display(df_edges)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 1 : Simple Motif
# MAGIC 
# MAGIC What are all the movies Tom Hanks played in and what role did he play?

# COMMAND ----------

motifs = (
  movie_graph
    .find("(actor)-[relation]->(movie)")
    .filter("actor.properties.name = 'Tom Hanks' AND relation.relation_type = 'ACTED_IN'")
    .select(
          'actor.properties.name',
          'relation.relation_type',
          'movie.properties.title',
          'relation.properties.roles',
          'movie.properties.released'
    )
    .orderBy(F.col('movie.properties.released'))
)

display(motifs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2: Anonymous Edges and Nodes
# MAGIC 
# MAGIC What individuals have more than one outgoing relation type?

# COMMAND ----------

motifs_anon_node = (
  movie_graph
    .find("(node)-[relation]->()")
    .groupBy("node.properties.name")
    .agg(F.collect_set("relation.relation_type").alias('relation_types'))
    .filter((F.size('relation_types') > 1))
    .orderBy(F.size('relation_types'), ascending=False)
)

display(motifs_anon_node)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 3: Variable Length Motifs
# MAGIC 
# MAGIC How many directed connections exist within the dataset?

# COMMAND ----------

# motifs are a special way of querying a graph with a DSL (domain specific language)

def create_motif(length):
    """Create a motif of variable length"""
    motif_path = "(a)-[e0]->"
    
    for i in range(1, length):
        motif_path += "(n%s);(n%s)-[e%s]->" % (i - 1, i - 1, i)
        
    motif_path += f"(n{length-1})"
    
    return motif_path

def variable_length_motif(depth:int, graph):
    """Append variable length motifs to one DataFrame"""
    for length in range(1, depth + 1):
        motif_path = create_motif(length)
    
        if length == 1:
            base_motif = graph.find(motif_path)

        else:
            current_motif = graph.find(motif_path)

            base_motif = base_motif.unionByName(current_motif,allowMissingColumns=True)
    
    return base_motif

# COMMAND ----------

variable_motif = variable_length_motif(3, movie_graph)

# COMMAND ----------

coalesce_name  = F.coalesce(*(F.col( i + '.properties.name') for i in variable_motif.columns[::-1] if i.startswith('n')))
coalesce_movie = F.coalesce(*(F.col( i + '.properties.title') for i in variable_motif.columns[::-1] if i.startswith('n')))

network_path   = F.array_except(F.array(*(F.col( i + '.relation_type' ) for i in variable_motif.columns if i.startswith('e'))), F.array(F.lit(None)))

display(
    variable_motif
        .select(
            "a.properties.name",
            network_path.alias('network_path'),
            coalesce_name.alias('person_name'),
            coalesce_movie.alias('movie_title'),
            F.size(network_path).alias('degrees')
        )
        .where(F.size('network_path') > 1)
)

# COMMAND ----------

motifs = (
  movie_graph
    .find("(actor)-[relation]->(movie)")
)

motifs.createOrReplaceTempView('motifs')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM motifs
# MAGIC WHERE movie.properties.title = 'The Matrix'
