import configparser
import operator

from Module2.Utilities import *
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, DoubleType, StructField, ArrayType
import networkx as nx


config = configparser.ConfigParser()
config.read(r'../Module2/Config_Files/config.ini')

class CustomerFeature(Utility):
    def __init__(self, fmt, path):
        """Constructor of Class CustomerFeature"""

        super().__init__()
        myLogger.info("Calling CustomerFeature Class constructor")

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

        self.df = self.readFileWithSchema(fmt, schema, path, ";")

    def filter_total_score(self, df, colName, score_val):
        """A function to filter the dataframe based on a column and its value"""

        res_df = df.filter(df[colName] >= score_val)
        return res_df

    def create_connected_components(self, df):
        """A function to create connected components between 2 columns of dataframe and assign a group to them"""

        pd_df = df.toPandas()

        Graph = nx.from_pandas_edgelist(pd_df, "ORIG", "BENEF")
        list_connected_components = list(nx.connected_components(Graph))
        to_map = {x: f'G{k}' for k, v in enumerate(list_connected_components, 1) for x in v}

        pd_df['Group'] = pd_df["ORIG"].map(to_map)

        res_df = self.spark.createDataFrame(pd_df)

        return res_df

    def generate_alert_key(self, df):
        """A function to generate alert key for highest total_score and minimum payment date among the group"""

        window1 = Window.partitionBy("Group")
        window2 = Window.partitionBy("Group", "Alert_Key")

        df = df.withColumn("Alert_Key", F.when(F.col("TOTAL_Score") == F.max("Total_Score").over(window1), "alert")
                           .otherwise(None))

        df = df.withColumn("Alert_Key", F.when((F.col("PAYMENT_DATE") == F.min("PAYMENT_DATE").over(window2)) &
                                           (F.col("Alert_Key") == "alert"), df["ORIG"]).otherwise(None))

        res_df = df.orderBy("REF_ID")

        return res_df

    def generate_alert_scores(self, df, new_col, prev_col):
        """A function to generate scores for the row having alert key"""

        res_df = df.withColumn(new_col, F.when(df["Alert_key"].isNotNull(), df[prev_col]))

        return res_df

    def fill_alert_columns(self, df, colName):
        """A function to fill alert values for the whole group"""

        window1 = Window.partitionBy("Group")

        res_df = df.withColumn(colName, F.last(colName, True).over(window1))
        return res_df

    def generate_top3_score(self, df):
        """A function to to find out top scores among the row"""

        df_dict = [row.asDict() for row in df.collect()]
        list_of_dict = []
        for items in df_dict:
            values_list = list(items.values())
            values = values_list[3:-3]
            dicts = dict(zip(values[::2], values[1::2]))
            sorted_dicts = sorted(dicts.items(), key=operator.itemgetter(1), reverse=True)
            list_of_dict.append(sorted_dicts)

        mySchema = StructType([StructField("FEATURE1", ArrayType(StringType(), True)),
                               StructField("FEATURE2", ArrayType(StringType(), True)),
                               StructField("FEATURE3", ArrayType(StringType(), True)),
                               StructField("FEATURE4", ArrayType(StringType(), True)),
                               StructField("FEATURE5", ArrayType(StringType(), True))
                               ])
        df_features = self.createDataFramefromList(list_of_dict, mySchema)

        df_features = df_features.select(df_features.FEATURE1[0].alias("Top1_Feature"),
                                         df_features.FEATURE1[1].alias("top1_val"),
                                         df_features.FEATURE2[0].alias("Top2_Feature"),
                                         df_features.FEATURE2[1].alias("top2_val"),
                                         df_features.FEATURE3[0].alias("Top3_Feature"),
                                         df_features.FEATURE3[1].alias("top3_val")
                                         )

        w = Window().orderBy(F.lit('A'))
        df_features = df_features.withColumn("row_no", F.row_number().over(w))
        df = df.withColumn("row_no", F.row_number().over(w))

        res_df = self.joinDataFrame(df, df_features, "row_no", "inner")

        return res_df

    def customer_transform(self, df):
        """A function "which handles all other class functions"""

        alert_score_val = config.get('alert', 'score_val')
        score_column = config.get('alert', 'alert_column')

        filter_df = self.filter_total_score(df, score_column, alert_score_val)

        cc_df = self.create_connected_components(filter_df)

        df_top_scores = self.generate_top3_score(cc_df)

        df_alert_key = self.generate_alert_key(df_top_scores)

        columns = ["alert_Top1_Feature", "Top1_Feature", "alert_top1_val", "top1_val",
                   "alert_Top2_Feature", "Top2_Feature", "alert_top2_val", "top2_val",
                   "alert_Top3_Feature", "Top3_Feature", "alert_top3_val", "top3_val"
                   ]

        for i in range(0, len(columns), 2):
            df_alert_key = self.generate_alert_scores(df_alert_key, columns[i], columns[i+1])

        for i in range(0, len(columns), 2):
            df_alert_key = self.fill_alert_columns(df_alert_key, columns[i])

        columns_to_display = ["REF_ID", "ORIG", "BENEF",
                             "Top1_Feature", "top1_val",
                             "Top2_Feature", "top2_val",
                             "Top3_Feature", "top3_val",
                             "TOTAL_Score", "PAYMENT_DATE",
                             "MONTH", "Group", "Alert_Key",
                             "alert_Top1_Feature", "alert_top1_val",
                             "alert_Top2_Feature", "alert_top2_val",
                             "alert_Top3_Feature", "alert_top3_val"
                             ]
        df_final = self.display_only_required(df_alert_key, columns_to_display)
        df_final.show()
