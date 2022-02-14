from Module1.Utilities import *
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, LongType, DoubleType


class SalesData(Utility):
    def __init__(self, fmt, path):
        """Constructor of Class Sales"""

        myLogger.info("Calling SalesData Class constructor")
        super().__init__()
        schema = StructType()\
            .add('Region', StringType(), True) \
            .add('Country', StringType(), True) \
            .add('Item Type', StringType(), True) \
            .add('Sales Channel', StringType(), True) \
            .add('Order Priority', StringType(), True) \
            .add('Order Date', StringType(), True) \
            .add('Order ID', LongType(), True) \
            .add('Units Sold', DoubleType(), True) \
            .add('Unit Price', DoubleType(), True) \
            .add('Total Revenue', DoubleType(), True) \
            .add('Total Profit', DoubleType(), True)

        self.df = self.readFileWithSchema(fmt, schema, path, ";")
        self.df = self.changeColumnName(self.df)
        self.df.printSchema()
        self.df = self.convertToDate(self.df, "Order_Date", "Order_Date")
        self.df.show()

    def totalRevenue_Region(self, df):
        """A function to calculate total revenue per region"""

        myLogger.info("function totalRevenue_Region called")

        res_df = df.groupBy("Region").agg(F.sum("Total_Revenue").alias("Total_Revenue"))
        return res_df

    def highestUnitsSold(self, df, itmType):
        """A function to display top 5 countries where highest given items are sold"""

        myLogger.info("function highestUnitsSold called")

        res_df = df.filter(df["Item_Type"] == itmType)
        res_df = res_df.orderBy(df["Units_Sold"], ascending=False).limit(5)

        return res_df

    def totalProfit_Region(self, df, reg, sdate, edate):
        """A function to calculate profit by region with start date and end date given"""

        myLogger.info("function totalProfit_Region called")

        res_df = df.filter(df["Region"] == reg)
        res_df = res_df.filter(res_df["Order_Date"].between(sdate, edate))
        res_df = res_df.orderBy(df["Order_Date"])

        return res_df

    def sls_transform(self, df):
        totRev_df = self.totalRevenue_Region(df)
        totRev_df.show()
        self.writeFile(totRev_df, "csv", "overwrite", "TotalRevenuePerRegion")

        itmType = "Household"
        highSold_df = self.highestUnitsSold(df, itmType)
        highSold_df.show()
        self.writeFile(highSold_df, "csv", "overwrite", "HighestUnitsSold")

        reg = "Asia"
        sdate = "2011-01-01"
        edate = "2015-12-31"
        totProfit_df = self.totalProfit_Region(df, reg, sdate, edate)
        totProfit_df.show()
        self.writeFile(totProfit_df, "csv", "overwrite", "TotalProfitByRegion")
