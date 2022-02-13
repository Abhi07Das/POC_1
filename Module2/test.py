from pyspark.sql import SparkSession, Window
import networkx as nx
import pandas
from pyspark.sql import functions as F
from pyspark.sql.functions import sort_array, array, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("PySpark Demo").getOrCreate()

df = spark.read.load("data.csv", format="csv", header=True, inferSchema=True, sep=";")

df = df.filter(df["TOTAL_Score"] >= 15)

df = df.toPandas()
G = nx.from_pandas_edgelist(df, "ORIG", "BENEF")
l = list(nx.connected_components(G))
L = [dict.fromkeys(y, x) for x, y in enumerate(l)]
d = {k: v for d in L for k, v in d.items()}

df['group'] = df.ORIG.map(d)
df = spark.createDataFrame(df)

'''
df2 = df.select("REF_ID", "FEATURE1_Score", "FEATURE2_Score", "FEATURE3_Score", "FEATURE4_Score", "FEATURE5_Score")
df2_col = df2.columns
# df_col = ["FEATURE1_Score", "FEATURE2_Score", "FEATURE3_Score", "FEATURE4_Score", "FEATURE5_Score"]
df2 = df.withColumn("top1_val", sort_array(array([F.col(x) for x in df2_col[1:]]), asc=False)[0]) \
    .withColumn("top2_val", sort_array(array([F.col(x) for x in df2_col[1:]]), asc=False)[1]) \
    .withColumn("top3_val", sort_array(array([F.col(x) for x in df2_col[1:]]), asc=False)[2])


def tolist(df, colName):
    l = df.select(colName).collect()
    arr = [(row[colName]) for row in l]
    return arr


df_col = ["FEATURE1", "FEATURE1_Score", "FEATURE2", "FEATURE2_Score", "FEATURE3", "FEATURE3_Score",
          "FEATURE4", "FEATURE4_Score", "FEATURE5", "FEATURE5_Score"]
colval=[]
for col in df_col:
    l=tolist(df, col)
    colval.append(l)

arr=[]
for i in range(0,len(colval),2):
    listoftup = list(zip(colval[i], colval[i+1]))
    arr.append(listoftup)
print(arr[0][0][0])


#df_dict = [row['FEATURE1']: row['FEATURE1_Score'] for row in df.collect()]
#print(df_dict)
'''
'''
def modify_values(r, max_col):
    l = []
    for i in range(len(df_col[1:])):
        if r[i] == max_col:
            l.append(df_col[i + 1])
    return l


modify_values_udf = udf(modify_values, StringType())

df3 = df2. \
    withColumn("top1_feature", modify_values_udf(array(df2.columns[1:-3]), "top1_val")). \
    withColumn("top2_feature", modify_values_udf(array(df2.columns[1:-3]), "top2_val")). \
    withColumn("top3_feature", modify_values_udf(array(df2.columns[1:-3]), "top3_val"))

df3.show()
'''

df = df.withColumn("Alert_Key",
                   F.max(F.struct("ORIG","TOTAL_Score")).over(Window.partitionBy("group"))["ORIG"])

df=df.orderBy("REF_ID")
df.show()
