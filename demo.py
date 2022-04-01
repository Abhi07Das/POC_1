from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import configparser


class Bank_Transform:
    def __init__(self):

        self.df = None
        self.df_trim = None
        self.df_amt = None
        self.df_filter_trn = None
        self.df_chq = None
        self.df_date_range = None
        self.df_count = None
        self.df_unique = None
        self.df_total_withdraw_deposit = None

        self.spark = SparkSession \
            .builder \
            .appName("PySpark Project") \
            .getOrCreate()

    def readFile(self, path, fmt="csv", schema=None, ):
        try:
            if schema is None:
                self.df = self.spark.read.format(fmt) \
                    .option("header", True) \
                    .load(path, inferSchema=True)
            else:
                self.df = self.spark.read.format(fmt) \
                    .option("header", True) \
                    .schema(schema) \
                    .load(path)
        except Exception:
            raise

    def writeFile(self, df, name, fmt="csv", mode="overwrite"):
        try:
            df.write.format(fmt).mode(mode).save(name, header='true')
        except Exception:
            raise

    def changeColumnName(self, df):
        try:
            self.df = df.select(col("Account No").alias("Account_No"),
                                col("DATE"),
                                col("TRANSACTION DETAILS").alias("TRANSACTION_DETAILS"),
                                col("CHQ NO").alias("CHQ_NO"),
                                col("VALUE DATE").alias("VALUE_DATE"),
                                col("WITHDRAWAL AMT").alias("WITHDRAWAL_AMT"),
                                col("DEPOSIT AMT").alias("DEPOSIT_AMT"),
                                col("BALANCE AMT").alias("BALANCE_AMT"))
        except Exception:
            raise

    def convertToDate(self, df, colName, newColName):
        try:
            self.df = df.withColumn(colName, to_date(col(colName), "dd-MM-yyyy").alias(newColName))

        except Exception:
            raise
        self.df.createOrReplaceTempView("bank")

    def drop_column(self):
        """A Method to drop the required column"""

        try:
            query = """SELECT Account_No, DATE, TRANSACTION_DETAILS, CHQ_NO, 
                       VALUE_DATE, WITHDRAWAL_AMT, DEPOSIT_AMT, BALANCE_AMT FROM bank 
                    """

            self.df_trim = self.spark.sql(query)
            self.df_trim.createOrReplaceTempView("bank_trim")
        except Exception:
            raise

    def add_transaction_type_amt(self):
        """A Method to add transaction type and transaction amount"""

        try:
            query1 = """
                        SELECT *,
                        CASE 
                            WHEN WITHDRAWAL_AMT IS NULL THEN DEPOSIT_AMT
                            ELSE WITHDRAWAL_AMT
                        END
                        AS TransactionAmount FROM bank_trim
                    """
            self.df_amt = self.spark.sql(query1)
            self.df_amt.createOrReplaceTempView("bank_amt")

            query2 = """
                        SELECT *,
                        CASE 
                            WHEN WITHDRAWAL_AMT IS NULL THEN 'CR'
                            ELSE 'DR'
                        END
                        AS TransactionType FROM bank_amt
                    """
            self.df_amt_type = self.spark.sql(query2)
            self.df_amt_type.createOrReplaceTempView("bank_amt_type")

        except Exception:
            raise

    def filter_trn(self, trn_type, filename):
        """A Method to filter dataframe based on transaction type"""

        try:
            query = "SELECT * FROM bank_amt_type WHERE TransactionType == '{}'".format(trn_type)

            self.df_filter_trn = self.spark.sql(query)
            self.df_filter_trn.createOrReplaceTempView("bank_filter_trn")

        except Exception:
            raise

    def cheque_trn(self):
        """A Method to determine all transaction done by cheque"""

        try:
            query = "SELECT * FROM bank_amt_type WHERE CHQ_NO IS NOT NULL"

            self.df_chq = self.spark.sql(query)
            self.df_chq.createOrReplaceTempView("bank_chq_trn")
        except Exception:
            raise

    def trn_date_range(self, sdate, edate):
        """A Method to filter dataframe within a date range"""

        try:
            query = """SELECT * FROM bank_amt_type 
                       WHERE DATE BETWEEN {0} AND {1}
                    """.format(sdate, edate)
            self.df_date_range = self.spark.sql(query)
            self.df_date_range.createOrReplaceTempView("bank_date_range")
        except Exception:
            raise

    def determine_duplicates(self):
        """A Method to determine duplicates in the dataframe"""

        try:
            query = """SELECT Account_No, DATE, TransactionType, TransactionAmount, COUNT(*) AS CNT FROM bank_amt_type 
                       GROUP BY Account_No, DATE, TransactionType, TransactionAmount
                       HAVING COUNT(*)>1
                    """
            self.df_count = self.spark.sql(query)
            self.df_count.createOrReplaceTempView("bank_duplicate_count")
        except Exception:
            raise

    def sum_withdraw_deposit(self, sdate, edate):
        """A Method to calculate total withdraw and total deposit for each account within a date range"""

        try:
            self.trn_date_range(sdate, edate)

            query = """SELECT Account_No, DATE, WITHDRAWAL_AMT, DEPOSIT_AMT, COUNT(*) AS CNT FROM bank_date_range 
                       GROUP BY Account_No, DATE, WITHDRAWAL_AMT, DEPOSIT_AMT
                       HAVING COUNT(*)=1
                    """
            self.df_unique = self.spark.sql(query)
            self.df_unique.createOrReplaceTempView("bank_unique_trn")

            query = """SELECT Account_No, sum(try_cast(WITHDRAWAL_AMT AS double)) AS Total_Withdraw, sum(try_cast(DEPOSIT_AMT AS double)) AS Total_Deposit FROM bank_unique_trn 
                       GROUP BY Account_No
                    """
            self.df_total_withdraw_deposit = self.spark.sql(query)
        except Exception:
            raise


config = configparser.ConfigParser()
config.read(r'../POC1_SparkSQL/Config_Files/transformations_config.ini')

if __name__ == "__main__":
    path = "bank.csv"

    bt = Bank_Transform()

    bt.readFile(path)
    bt.changeColumnName(bt.df)
    bt.convertToDate(bt.df, "DATE", "DATE")

    bt.drop_column()
    bt.add_transaction_type_amt()

    bt.filter_trn("DR", "Debit_Transaction")
    bt.writeFile(bt.df_filter_trn, name="Debit_Transaction", fmt='parquet', mode='overwrite')

    bt.filter_trn("CR", "Credit_Transaction")
    bt.writeFile(bt.df_filter_trn, name="Debit_Transaction", fmt='parquet', mode='overwrite')

    bt.cheque_trn()
    bt.writeFile(bt.df_chq, 'Cheque_Transaction', 'parquet', 'overwrite')

    sdate = config.get('parameters', 'start_date')
    edate = config.get('parameters', 'end_date')
    bt.trn_date_range(sdate, edate)

    bt.determine_duplicates()

    sdate = config.get('total_withdrawal', 'start_date')
    edate = config.get('total_withdrawal', 'end_date')
    bt.sum_withdraw_deposit(sdate, edate)

    bt.df_total_withdraw_deposit.show()
