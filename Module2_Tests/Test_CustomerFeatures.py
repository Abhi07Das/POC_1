import logging
from pandas.util.testing import assert_frame_equal
from Module2.CustomerFeatures import CustomerFeature
from pyspark.sql import SparkSession
import unittest
import configparser

config = configparser.ConfigParser()
config.read(r'../Module2_Tests/Config_Files/test_customer_features_config.ini')

fmt = config.get('paths', 'sample_fmt')
data = config.get('paths', 'sample_data')
c = CustomerFeature(fmt, data)

logFormat = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename="test_sales.log",
                    filemode="w",
                    format=logFormat,
                    level=logging.INFO)
myLogger = logging.getLogger()


class TestCustomerFeatures(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession \
            .builder \
            .appName("PySpark Demo") \
            .getOrCreate()

    def test_filter_total_score(self):
        spark = TestCustomerFeatures.spark
        act_columns = ["ORIG", "TOTAL_Score"]
        act_data = [("Jack", 10), ("John", 12), ("Harry", 16),
                    ("Tom", 19), ("Bruce", 15)
                    ]
        act_df = spark.createDataFrame(act_data).toDF(*act_columns)
        act_df = c.filter_total_score(act_df, "TOTAL_Score", 15)
        act_df = act_df.orderBy("TOTAL_Score")

        exp_columns = ["ORIG", "TOTAL_Score"]
        exp_data = [("Bruce", 15), ("Harry", 16),("Tom", 19)]
        exp_df = spark.createDataFrame(exp_data).toDF(*exp_columns)

        pd_act = act_df.toPandas()
        pd_exp = exp_df.toPandas()
        assert_frame_equal(pd_act, pd_exp)

    def test_create_connected_components(self):
        spark = TestCustomerFeatures.spark

        sample_Data = config.get('paths', 'sample_data')
        act_df = spark.read.load(sample_Data, format="csv", header=True, inferSchema=True, sep=",")
        act_df = c.create_connected_components(act_df)

        exp_columns = ["REF_ID", "ORIG", "BENEF", "Group"]
        exp_data = [(1, "Jack", "Harry", "G1"), (2, "John", "Jack", "G1"),
                    (3, "Harry", "Tom", "G1"), (4, "Tom", "Jack", "G1"), (5, "Bruce", "Jackie", "G2")]
        exp_df = spark.createDataFrame(exp_data).toDF(*exp_columns)

        pd_act = act_df.toPandas()
        pd_exp = exp_df.toPandas()
        assert_frame_equal(pd_act, pd_exp)

    def test_generate_alert_key(self):
        spark = TestCustomerFeatures.spark
        act_columns = ["REF_ID", "ORIG", "BENEF", "Group", "TOTAL_Score", "PAYMENT_DATE"]
        act_data = [(1, "Jack", "Harry", "G1", 15, "2017-01-08"),
                    (2, "John", "Jack", "G1", 16, "2017-03-05"),
                    (3, "Harry", "Tom", "G1", 19, "2018-01-09"),
                    (4, "Tom", "Jack", "G1", 19, "2019-01-09"),
                    (5, "Bruce", "Jackie", "G2", 16, "2015-05-02")
                    ]
        act_df = spark.createDataFrame(act_data).toDF(*act_columns)
        act_df = c.generate_alert_key(act_df)

        exp_columns = ["REF_ID", "ORIG", "BENEF", "Group", "TOTAL_Score", "PAYMENT_DATE", "Alert_Key"]
        exp_data = [(1, "Jack", "Harry", "G1", 15, "2017-01-08", "Harry"),
                    (2, "John", "Jack", "G1", 16, "2017-03-05", "Harry"),
                    (3, "Harry", "Tom", "G1", 19, "2018-01-09", "Harry"),
                    (4, "Tom", "Jack", "G1", 19, "2019-01-09", "Harry"),
                    (5, "Bruce", "Jackie", "G2", 16, "2015-05-02", "Bruce")
                    ]

        exp_df = spark.createDataFrame(exp_data).toDF(*exp_columns)

        pd_act = act_df.toPandas()
        pd_exp = exp_df.toPandas()
        assert_frame_equal(pd_act, pd_exp)
