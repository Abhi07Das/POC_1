from Module2.Utilities import *
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, DoubleType
import networkx as nx


class CustomerFeature(Utility):
    def __init__(self, fmt, path):
        """Constructor of Class CustomerFeature"""

        myLogger.info("Calling CustomerFeature Class constructor")
        super.__init__()

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
            .add('MONTH', DoubleType(), True)

        self.df = self.readFileWithSchema(fmt,schema, path, ";")

    def filter_total_score(self, df):
        res_df = df.filter(df["TOTAL_Score"] >= 15)
        return res_df

    def create_connected_components(self, df):
        pd_df = df.toPandas()

        Graph = nx.from_pandas_edgelist(pd_df, "ORIG", "BENEF")
        list_connected_components = list(nx.connected_components(Graph))
        to_map = {x: f'G{k}' for k,v in enumerate(list_connected_components, 1) for x in v}

        pd_df['Group'] = pd_df["ORIG"].map(to_map)

        res_df = self.spark.createDataFrame(pd_df)

        return res_df

    def generate_alert_key(self, df):
        window1 = Window.partitionBy("Group")
        window2 = Window.partitionBy("Group", "Alert_Key")

        df = df.withColumn("Alert_Key", F.when(F.col("TOTAL_Score") == F.max("Total_Score").over(window1), "alert")
                           .otherwise(None))

        df = df.withColumn("Alert_Key", F.when((F.col("PAYMENT_DATE") == F.min("PAYMENT_DATE").over(window2)) &
                                               (F.col("Alert_Key") == "alert"), "alert").otherwise(None))
        res_df = df.orderBy("REF_ID")

        return res_df

    def customer_transform(self, df):
        filer_df = self.filter_total_score(df)
        cc_df = self.create_connected_components(filer_df)
