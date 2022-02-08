import logging
from pandas.util.testing import assert_frame_equal
from Module1.Accounts import AccountData
from pyspark.sql import SparkSession
import unittest
import configparser

config = configparser.ConfigParser()
config.read(r'../Module1_Tests/Config_Files/test_accounts_config.ini')

sample_fmt = config.get('paths', 'sample_fmt')
sample_data = config.get('paths', 'sample_data')
a = AccountData(sample_fmt, sample_data, sample_fmt, sample_data)

logFormat = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename="test_accounts.log",
                    filemode="w",
                    format=logFormat,
                    level=logging.INFO)
myLogger = logging.getLogger()


class TestAccounts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession \
            .builder \
            .appName("PySpark Demo") \
            .getOrCreate()

    def test_totalAccounts_Customer(self):
        myLogger.info("testing totalAccounts_Customer method")

        spark = TestAccounts.spark

        act_columns = ["customerId", "accountId", "balance"]
        act_data = [("IND0325", "ACC0001", 687.0), ("IND0325", "ACC0002", 589.0),
                     ("IND0325", "ACC0003", 194.0), ("IND0284", "ACC0004", 809.0)]
        act_df = spark.createDataFrame(act_data).toDF(*act_columns)
        act_df = a.totalAccounts_Customer(act_df)
        act_df.show()

        exp_columns = ["customerId", "Account_Count"]
        exp_data = [("IND0284", 1),
                    ("IND0325", 3)
                    ]
        exp_df = spark.createDataFrame(exp_data).toDF(*exp_columns)

        pd_act = act_df.toPandas()
        pd_exp = exp_df.toPandas()
        assert_frame_equal(pd_act, pd_exp)

    def test_customerWithMultipleAccounts(self):
        myLogger.info("testing customerWithMultipleAccounts method")

        spark = TestAccounts.spark
        act1_columns = ["customerId", "accountId", "balance"]
        act1_data = [("IND0325", "ACC0001", 687.0), ("IND0325", "ACC0002", 589.0),
                     ("IND0325", "ACC0003", 194.0), ("IND0284", "ACC0004", 809.0)]
        act1_df = spark.createDataFrame(act1_data).toDF(*act1_columns)

        act2_columns = ["customerId", "forename", "surname"]
        act2_data = [("IND0325", "Christopher", "Black"), ("IND0221", "Madeleine", "Kerr"),
                     ("IND0147", "Rachel", "Parsons"), ("IND0284", "Oliver", "Johnston"),
                     ("IND0285", "Boris", "Graham")]
        act2_df = spark.createDataFrame(act2_data).toDF(*act2_columns)

        res_df = a.customerWithMultipleAccounts(act1_df, act2_df)

        exp_columns = ["customerId", "forename", "surname", "NoOfAccounts"]
        exp_data = [("IND0325", "Christopher", "Black", 3)]
        exp_df = spark.createDataFrame(exp_data).toDF(*exp_columns)

        pd_act = res_df.toPandas()
        pd_exp = exp_df.toPandas()
        assert_frame_equal(pd_act, pd_exp)

    def test_customerWithHighestBal(self):
        myLogger.info("testing customerWithHighestBal method")

        spark = TestAccounts.spark
        act_columns = ["customerId", "accountId", "balance"]
        act_data = [("IND0325", "ACC0001", 687.0), ("IND0325", "ACC0002", 589.0),
                     ("IND0325", "ACC0003", 194.0), ("IND0284", "ACC0004", 809.0),
                     ("IND0284", "ACC0005", 445.0), ("IND0284", "ACC0006", 995.0)]
        act_df = spark.createDataFrame(act_data).toDF(*act_columns)
        act_df = a.customerWithHighestBal(act_df)
        act_df.show()

        exp_columns = ["customerId", "accountId", "balance"]
        exp_data = [("IND0284", "ACC0006", 995.0), ("IND0284", "ACC0004", 809.0),
                    ("IND0325", "ACC0001", 687.0), ("IND0325", "ACC0002", 589.0),
                    ("IND0284", "ACC0005", 445.0)]
        exp_df = spark.createDataFrame(exp_data).toDF(*exp_columns)

        pd_act = act_df.toPandas()
        pd_exp = exp_df.toPandas()
        assert_frame_equal(pd_act, pd_exp)
