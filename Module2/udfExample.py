from pyspark.sql import SparkSession
import configparser
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType

config = configparser.ConfigParser()
config.read(r'../Module2/Config_Files/config.ini')


class UDFExample():
    def rank_given(self, score_given):

        scores = [int(i) for i in config['score_range']['range'].split(',')]

        if score_given <= scores[0]:
            return "1"
        elif scores[0] < score_given <= scores[1]:
            return "2"
        elif scores[1] < score_given <= scores[2]:
            return "2"
        elif scores[2] < score_given <= scores[3]:
            return "3"
        elif scores[3] < score_given <= scores[4]:
            return "4"
        else:
            return None

    def sample_udf(self, df):
        rank_UDF = udf(lambda x: self.rank_given(x), StringType())

        res_df = df.withColumn("rank", rank_UDF(df["Scores"]))

        return res_df


spark = SparkSession \
    .builder \
    .appName("PySpark Demo") \
    .getOrCreate()

act_columns = ["customerId", "Scores"]
act_data = [("IND0325", 42), ("IND0520", 75), ("IND0436", 57),
            ("IND0425", 24), ("IND0284", 109), ("IND0724", 63)]
act_df = spark.createDataFrame(act_data).toDF(*act_columns)

obj = UDFExample()
act_df = obj.sample_udf(act_df)
act_df.show()
