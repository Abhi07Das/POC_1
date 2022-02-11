from pyspark.sql import SparkSession
import networkx as nx
import pandas
import graphframes

spark = SparkSession.builder.appName("PySpark Demo").getOrCreate()

act_columns = ["ID", "Orig", "Benef"]
act_data = [("1", "Jack", "Harry"), ("2", "John", "Jack"),
            ("3", "Harry", "Tom"), ("4", "Tom", "Jack"), ("4", "Bruce", "Jackie")]
act_df = spark.createDataFrame(act_data).toDF(*act_columns)

'''act_df = act_df.toPandas()

G = nx.from_pandas_edgelist(act_df, "Orig", "Benef")
l = list(nx.connected_components(G))
L = [dict.fromkeys(y, x) for x, y in enumerate(l)]
d = {k: v for d in L for k, v in d.items()}

act_df['group'] = act_df.Orig.map(d)
df = spark.createDataFrame(act_df)
df.show()'''

v_df = act_df.select("Orig").union(act_df.select("Benef"))

graph = GraphFrame(v_df, act_df)