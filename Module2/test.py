import operator

from pyspark.sql import SparkSession, Window
import networkx as nx
import pandas
from pyspark.sql import functions as F
from pyspark.sql.functions import sort_array, array, udf, when, expr, explode, split
from pyspark.sql.types import StringType, StructType, IntegerType, DoubleType, TimestampType, DataType, NullType, \
    StructField, ArrayType

spark = SparkSession.builder.appName("PySpark Demo").getOrCreate()
schema = StructType() \
    .add('REF_ID', IntegerType(), True) \
    .add('ORIG', StringType(), True) \
    .add('BENEF', StringType(), True) \
    .add('FEATURE1', StringType(), True) \
    .add('FEATURE1_Score', DoubleType(), True) \
    .add('FEATURE2', StringType(), True) \
    .add('FEATURE2_Score', DoubleType(), True) \
    .add('FEATURE3', StringType(), True) \
    .add('FEATURE3_Score', DoubleType(), True) \
    .add('FEATURE4', StringType(), True) \
    .add('FEATURE4_Score', DoubleType(), True) \
    .add('FEATURE5', StringType(), True) \
    .add('FEATURE5_Score', DoubleType(), True) \
    .add('TOTAL_Score', DoubleType(), True) \
    .add('PAYMENT_DATE', TimestampType(), True) \
    .add('MONTH', IntegerType(), True)

df = spark.read.schema(schema).load("data.csv", format="csv", header=True, sep=";")
# df = spark.read.load("data.csv", format="csv",inferSchema=True, header=True, sep=";")
df = df.filter(df["TOTAL_Score"] >= 15)

df = df.toPandas()

G = nx.from_pandas_edgelist(df, "ORIG", "BENEF")
l = list(nx.connected_components(G))
# L = [dict.fromkeys(y, x) for x, y in enumerate(l)]
# to_map = {k: v for d in L for k, v in d.items()}
to_map = {x: f'G{k}' for k, v in enumerate(l, 1) for x in v}

df['group'] = df['ORIG'].map(to_map)
df = spark.createDataFrame(df)

df2 = df.select("REF_ID", "FEATURE1_Score", "FEATURE2_Score", "FEATURE3_Score", "FEATURE4_Score", "FEATURE5_Score")
df2_col = df2.columns
df_col = ["FEATURE1_Score", "FEATURE2_Score", "FEATURE3_Score", "FEATURE4_Score", "FEATURE5_Score"]
df2 = df.withColumn("top1_val", sort_array(array([F.col(x) for x in df_col]), asc=False)[0]) \
    .withColumn("top2_val", sort_array(array([F.col(x) for x in df_col]), asc=False)[1]) \
    .withColumn("top3_val", sort_array(array([F.col(x) for x in df_col]), asc=False)[2])

'''
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


#df_dict = [row['FEATURE1']: row['FEATURE1_Score'] for row in df.collect()]
#print(df_dict)

def func(df, top1_val, col):
    for i in range(1,len(col),2):
        if top1_val == df[col[i]]:
            print(df[col[i-1]])
            return df[col[i-1]]
    return None

func_udf = udf(func, DataType())
df3_col = ["FEATURE1", "FEATURE1_Score", "FEATURE2", "FEATURE2_Score", "FEATURE3", "FEATURE3_Score",
          "FEATURE4", "FEATURE4_Score", "FEATURE5", "FEATURE5_Score"]

df2 = df2.withColumn("top1_feature", F.lit(None).cast(NullType()))
df2.show()
for i in range(1,len(df3_col),2):
    df2 = df2.withColumn("top1_feature", when((df2["top1_val"] == df[df3_col[i]] & df2["top1_feature"].isNull()), df[df3_col[i-1]]))
df2.show()
#df2 = df2.withColumn("top1_feature", func_udf(df, df2["top1_val"], df3_col))
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


'''
w2 = Window.partitionBy("group")
w1 = Window.partitionBy("group", "Alert_Key")
#df = df.withColumn("Alert_Key", F.max(F.struct("ORIG","TOTAL_Score")).over(w2)["ORIG"])
df = df.withColumn("Alert_Key", F.when(F.col("TOTAL_Score") == F.max("Total_Score").over(w2),"alert").otherwise(None))
df = df.withColumn("Alert_Key", F.when((F.col("PAYMENT_DATE") == F.min("PAYMENT_DATE").over(w1)) &
                                       (F.col("Alert_Key") == "alert"),df["ORIG"]).otherwise(None))
df.show()
df = df.withColumn("Alert_Key", F.last("Alert_Key", True).over(w2))
df = df.orderBy("REF_ID")


'''
df_dict = [row.asDict() for row in df.collect()]
new_dict = []
for items in df_dict:
    values_list = list(items.values())
    values = values_list[3:-3]
    dicts = dict(zip(values[::2], values[1::2]))
    sorted_dicts = sorted(dicts.items(), key=operator.itemgetter(1), reverse=True)
    new_dict.append(sorted_dicts)

# [[' Credit', 5.0], [' Transfer', 4.0], ['HighRisk', 3.0], ['Lease', 3.0], [' Deposit', 2.0]]

mySchema = StructType([StructField("FEATURE1", ArrayType(StringType(),True)),
                       StructField("FEATURE2", ArrayType(StringType(),True)),
                       StructField("FEATURE3", ArrayType(StringType(),True)),
                       StructField("FEATURE4", ArrayType(StringType(),True)),
                       StructField("FEATURE5", ArrayType(StringType(),True))
                       ])

columns = ["FEATURE1", "FEATURE2", "FEATURE3",
           "FEATURE4", "FEATURE5", ]
#df_new = spark.createDataFrame(new_dict).toDF(*columns)
df_new = spark.createDataFrame(new_dict, schema=mySchema)

#df_new = df_new.select(explode(df["FEATURE1"]))
#df_new.printSchema()
#df_new.show()

#df_new = df_new.select(F.struct(df_new.FEATURE1[0].alias("TopFeature1"),
#                               df_new.FEATURE1[1].alias("Top1_val")).alias("FEATURE1"))

#df_new = df_new.withColumn("Top_Feature1", split(df_new["FEATURE1"], ",").getItem(0)).\
#    withColumn("Top1_val", split(df_new["FEATURE1"], ",").getItem(1))
df_new = df_new.select(df_new.FEATURE1[0].alias("Top1_Feature"),
                       df_new.FEATURE1[1].alias("top1_val"),
                       df_new.FEATURE2[0].alias("Top2_Feature"),
                       df_new.FEATURE2[1].alias("top2_val"),
                       df_new.FEATURE3[0].alias("Top3_Feature"),
                       df_new.FEATURE3[1].alias("top3_val")
                       )


w = Window().orderBy(F.lit('A'))
df_new = df_new.withColumn("row_no", F.row_number().over(w))
df = df.withColumn("row_no", F.row_number().over(w))

df_new.printSchema()
df_new.show()

df_new = df_new.withColumn("top1_val", df_new["top1_val"].astype(IntegerType)).\
    withColumn("top2_val", df_new["top2_val"].astype(IntegerType)).\
    withColumn("top3_val", df_new["top3_val"].astype(IntegerType))
df_new.printSchema()
df_new.show()

df_final = df.join(df_new, ["row_no"])
df_final.show()
#df.show()
#df_new.show()
'''