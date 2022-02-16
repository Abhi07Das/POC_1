import logging
from pandas.util.testing import assert_frame_equal
from Module1.Sales import SalesData
from pyspark.sql import SparkSession
import unittest
import configparser

config = configparser.ConfigParser()
config.read(r'../Module1_Tests/Config_Files/test_sales_config.ini')

fmt = config.get('paths', 'sample_fmt')
data = config.get('paths', 'sample_data')
s = SalesData(fmt, data)

logFormat = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename="test_sales.log",
                    filemode="w",
                    format=logFormat,
                    level=logging.INFO)
myLogger = logging.getLogger()


class TestSales(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession \
            .builder \
            .appName("PySpark Demo") \
            .getOrCreate()

    def test_totalRevenue_Region(self):
        myLogger.info("testing totalRevenue_Region method")

        spark = TestSales.spark

        sample_Data = config.get('paths', 'sample_data')
        act_df = spark.read.load(sample_Data, format="csv", header=True, inferSchema=True, sep=";")
        act_df = act_df.select(act_df["Region"].alias("Region"),
                               act_df["Country"].alias("Country"),
                               act_df["Item Type"].alias("Item_Type"),
                               act_df["Sales Channel"].alias("Sales_Channel"),
                               act_df["Order Priority"].alias("Order_Priority"),
                               act_df["Order Date"].alias("Order_Date"),
                               act_df["Order ID"].alias("Order_ID"),
                               act_df["Units Sold"].alias("Units_Sold"),
                               act_df["Unit Price"].alias("Unit_Price"),
                               act_df["Total Revenue"].alias("Total_Revenue"),
                               act_df["Total Profit"].alias("Total_Profit"))
        act_df = s.totalRevenue_Region(act_df)

        exp_columns = ["Region", "Total_Revenue"]
        exp_data = [("Middle East and North Africa", 1.405270658E7),
                    ("Australia and Oceania", 1.4094265130000003E7),
                    ("Europe", 3.336893211E7),
                    ("Sub-Saharan Africa", 3.967203143000001E7),
                    ("Central America and the Caribbean", 9170385.49),
                    ("North America", 5643356.550000001),
                    ("Asia", 2.1347091020000003E7)
                    ]
        exp_df = spark.createDataFrame(exp_data).toDF(*exp_columns)

        pd_act = act_df.toPandas()
        pd_exp = exp_df.toPandas()
        assert_frame_equal(pd_act, pd_exp)

    def test_highestUnitsSold(self):
        myLogger.info("testing highestUnitsSold method")

        spark = TestSales.spark

        #act_df = spark.read.load(sample_Data, format="csv", header=True, inferSchema=True, sep=";")
        act_columns = ["Region", "Item_type", "Units_Sold"]
        act_data = [("Europe", "Household", 282), ("Sub-Saharan Africa", "Household", 2370),
                    ("Asia", "Household", 4187), ("Europe", "Household", 3830),
                    ("Europe", "Household", 5210), ("Asia", "Household", 6210),
                    ("Central America and the Caribbean", "Household", 200)]
        act_df = spark.createDataFrame(act_data).toDF(*act_columns)
        act_df = s.highestUnitsSold(act_df, "Household")

        exp_columns = ["Region", "Item_type", "Units_Sold"]
        exp_data = [ ("Asia", "Household", 6210), ("Europe", "Household", 5210),
                     ("Asia", "Household", 4187), ("Europe", "Household", 3830),
                     ("Sub-Saharan Africa", "Household", 2370)]
        exp_df = spark.createDataFrame(exp_data).toDF(*exp_columns)

        pd_act = act_df.toPandas()
        pd_exp = exp_df.toPandas()
        assert_frame_equal(pd_act, pd_exp)

    def test_totalProfit_region(self):
        myLogger.info("testing totalProfit_region method")

        spark = TestSales.spark

        sample_Data = config.get('paths', 'sample_data')
        #act_df = spark.read.load(sample_Data, format="csv", header=True, inferSchema=True, sep=";")
        act_columns = ["Region", "Order_Date", "Total_Profit"]
        act_data = [("Europe", "2011-06-05", 282), ("Sub-Saharan Africa", "2011-07-05", 2370),
                    ("Asia", "2011-09-05", 4187), ("Europe", "2011-11-05", 3830),
                    ("Asia", "2011-02-05", 5210), ("Asia", "2017-06-05", 6210),
                    ("Central America and the Caribbean", "2018-06-05", 200)]
        act_df = spark.createDataFrame(act_data).toDF(*act_columns)
        act_df = s.totalProfit_Region(act_df, "Asia", "2011-01-01", "2015-12-31")
        act_df.show()

        exp_columns = ["Region", "Order_Date", "Total_Profit"]
        exp_data = [ ("Asia", "2011-02-05", 5210), ("Asia", "2011-09-05", 4187)]
        exp_df = spark.createDataFrame(exp_data).toDF(*exp_columns)

        pd_act = act_df.toPandas()
        pd_exp = exp_df.toPandas()
        assert_frame_equal(pd_act, pd_exp)
