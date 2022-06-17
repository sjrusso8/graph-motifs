# Working with Motifs Using GraphFrames

This repo contains some examples of using the 'motif finding' DSL language that is a part of the GraphFrame packages for Apache Spark. The documentation for GraphFrames Motif finding can be found here: [GraphFrames Motif Finding](graphframes.github.io/graphframes/docs/_site/user-guide.html#motif-finding)

The accompanying Medium article with a detailed walkthrough of the examples in this repo can be found here: [3 Examples of Motifs with Spark GraphFrames]()

## Data Used
The data used for this demo was sourced from the Neo4j example movie dataset. 

![](https://github.com/neo4j-graph-examples/recommendations/blob/main/documentation/img/model.png?raw=true)

## Setup

I used an Azure Databricks workspace with a small cluster running Spark 10.4 LTS ML. The Spark ML versions have GraphFrames pre-installed on the cluster. I loaded the data into a storage account and saved the data as a hive table with the code in the notebook [load_movie_data](notebooks/load_movie_data). 