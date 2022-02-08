import logging
from pandas.util.testing import assert_frame_equal
from Module1.Utilities import Utility
from pyspark.sql import SparkSession
import unittest
import configparser
from pyspark.sql.functions import to_date
from pyspark.sql import functions as f
from pyspark.sql.types import StringType, StructType, DoubleType

u = Utility()
config = configparser.ConfigParser()
config.read(r'../Module1_Tests/Config_Files/test_utilities_config.ini')

logFormat = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename="test_utilities.log",
                    filemode="w",
                    format=logFormat,
                    level=logging.INFO)
myLogger = logging.getLogger()


class TestUtility(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession \
            .builder \
            .appName("PySpark Demo") \
            .getOrCreate()

    def test_readFile(self):
        myLogger.info("testing readFile method")

        sample_Data = config.get('paths', 'sample_data')
        df = u.readFile("csv", sample_Data)
        col_names = df.columns

        col_expected = ["customerId", "accountId", "balance"]

        self.assertEqual(col_names, col_expected)

    def test_readFileWithSchema(self):
        myLogger.info("testing readFileWithSchema method")

        spark = TestUtility.spark
        sample_Data = config.get('paths', 'sample_data1')
        schema = StructType() \
            .add('customerId', StringType(), True) \
            .add('accountId', StringType(), True) \
            .add('balance', DoubleType(), True)

        act_df = u.readFileWithSchema("csv", schema, sample_Data, ",")
        act_df = act_df.sort("accountId")

        exp_columns = ["customerId", "accountId", "balance"]
        exp_data = [("IND0325", "ACC0001", 687.0), ("IND0221", "ACC0002", 589.0),
                    ("IND0147", "ACC0003", 194.0), ("IND0284", "ACC0004", 809.0)]
        exp_df = spark.createDataFrame(exp_data).toDF(*exp_columns)

        pd_act = act_df.toPandas()
        pd_exp = exp_df.toPandas()
        assert_frame_equal(pd_act, pd_exp)

    def test_joinDataFrame(self):
        myLogger.info("testing joinDataFrame method")

        spark = TestUtility.spark

        act1_columns = ["customerId", "accountId", "balance"]
        act1_data = [("IND0325", "ACC0001", 687.0), ("IND0221", "ACC0002", 589.0),
                    ("IND0147", "ACC0003", 194.0), ("IND0284", "ACC0004", 809.0)]
        act1_df = spark.createDataFrame(act1_data).toDF(*act1_columns)

        act2_columns = ["customerId", "forename", "surname"]
        act2_data = [("IND0325", "Christopher", "Black"), ("IND0221", "Madeleine", "Kerr"),
                     ("IND0147", "Rachel", "Parsons"), ("IND0284", "Oliver", "Johnston"),
                     ("IND0285", "Boris", "Graham")]
        act2_df = spark.createDataFrame(act2_data).toDF(*act2_columns)

        joined_df = u.joinDataFrame(act1_df, act2_df, "customerId", "inner")

        exp_columns = ["customerId", "accountId", "balance", "forename", "surname"]
        exp_data = [("IND0221", "ACC0002", 589.0, "Madeleine", "Kerr"),
                    ("IND0284", "ACC0004", 809.0, "Oliver", "Johnston"),
                    ("IND0325", "ACC0001", 687.0, "Christopher", "Black"),
                    ("IND0147", "ACC0003", 194.0, "Rachel", "Parsons")]
        exp_df = spark.createDataFrame(exp_data).toDF(*exp_columns)

        pd_act = joined_df.toPandas()
        pd_exp = exp_df.toPandas()
        assert_frame_equal(pd_act, pd_exp)

    def test_changeColumnName(self):
        myLogger.info("testing changeColumnName method")

        spark = TestUtility.spark
        sample_Data = config.get('paths', 'sample_data2')

        df = spark.read.load(sample_Data, format="csv", header=True, inferSchema=True, sep=";")
        act_df = u.changeColumnName(df)
        col_names = act_df.columns

        col_expected = ["Region", "Country", "Item_Type", "Sales_Channel",
                        "Order_Priority", "Order_Date", "Order_ID", "Units_Sold",
                        "Unit_Price", "Total_Revenue", "Total_Profit"]

        self.assertEqual(col_names, col_expected)

    def test_convertToDate(self):
        myLogger.info("testing convertToDate method")

        spark = TestUtility.spark

        act_columns = ["Sl No", "Date"]
        act_data = [("1", "5/28/2010"), ("2", "2-4-2015")]
        act_df = spark.createDataFrame(act_data).toDF(*act_columns)
        act_df = u.convertToDate(act_df, "Date", "Date")

        exp_columns = ["Sl No", "Date"]
        exp_data = [("1", "28-05-2010"), ("2", "02-04-2015")]
        exp_df = spark.createDataFrame(exp_data).toDF(*exp_columns)

        exp_df = exp_df.withColumn("Date", to_date(f.col("Date"), "dd-MM-yyyy").alias("Date"))

        pd_act = act_df.toPandas()
        pd_exp = exp_df.toPandas()
        assert_frame_equal(pd_act, pd_exp)



