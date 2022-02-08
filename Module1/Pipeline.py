from Utilities import *
from Accounts import AccountData
from Sales import SalesData


class Pipeline(AccountData, SalesData):
    def __init__(self):
        myLogger.info("Calling Pipeline Constructor")

        self.accountObject = None
        self.salesObject = None

    def createAccountsObject(self, acc_fmt, acc_path, cst_fmt, cst_path):
        self.accountObject = AccountData(acc_fmt, acc_path, cst_fmt, cst_path)

    def createSalesObject(self, fmt, path):
        self.salesObject = SalesData(fmt, path)

    def acc_sales_transform(self):
        acc_df = self.accountObject.acc_df
        cst_df = self.accountObject.cst_df
        self.accountObject.acc_transform(acc_df, cst_df)

        sls_df = self.salesObject.df
        self.salesObject.sls_transform(sls_df)

pipeline = Pipeline()
pipeline.createAccountsObject("csv", "account_data.csv", "csv", "customer_data.csv")
pipeline.createSalesObject("csv", "salesData.csv")
pipeline.acc_sales_transform()