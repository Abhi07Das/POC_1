from Module1.Utilities import *
from pyspark.sql import functions as F, Window


class AccountData(Utility):
    def __init__(self, acc_fmt, acc_path, cst_fmt, cst_path):
        """Constructor of Class AccountData"""

        myLogger.info("Calling CustomerData Class constructor")
        super().__init__()

        self.acc_df = self.readFile(acc_fmt, acc_path)
        self.cst_df = self.readFile(cst_fmt, cst_path)

    def totalAccounts_Customer(self, df):
        """A function to find total number of accounts associated For each customer"""

        myLogger.info("function totalAccounts_Customer called")

        acc_dfCount = df.groupBy("customerId") \
            .agg(F.countDistinct("accountId")
                 .alias("Account_Count"))

        return acc_dfCount

    def customerWithMultipleAccounts(self, acc_df, cst_df):
        """A function to find customers who has more than 2 accounts """

        myLogger.info("function customerWithMultipleAccounts called")

        df_join = self.joinDataFrame(cst_df, acc_df, "customerId", "inner")
        df_res = df_join.groupBy("customerId", "forename", "surname")\
            .agg(F.countDistinct("accountId").alias("NoOfAccounts"))

        df_res = df_res.filter(df_res["NoOfAccounts"] > 2)

        return df_res

    def customerWithHighestBal(self, df):
        """A function to display top 5 customer with highest account balance"""

        myLogger.info("function customerWithHighestBal called")

        df_sorted = df.sort(df.balance.desc()).limit(5)

        return df_sorted

        #window = Window.orderBy(F.col("balance").desc())
        #df_top5 = df.select("*", F.row_number().over(window).alias("rank")).filter(F.col('rank') <= 5)
        #df_top5.show()

    def acc_transform(self, acc_df, cst_df):
        acc_dfCount = self.totalAccounts_Customer(acc_df)
        acc_dfCount.show()

        mulAcc_df = self.customerWithMultipleAccounts(acc_df, cst_df)
        mulAcc_df.show()

        highBal_df = self.customerWithHighestBal(acc_df)
        highBal_df.show()
