from pyspark.sql import SparkSession
import logging

from pyspark.sql.functions import expr, coalesce, to_date
from pyspark.sql import functions as F

logFormat = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename="app.log",
                    filemode="w",
                    format=logFormat,
                    level=logging.INFO)
myLogger = logging.getLogger()


class Utility:
    def __init__(self):
        """Constructor of Class Utility"""

        myLogger.info("Calling Utility Class Constructor")
        self.df = None

        self.spark = SparkSession \
            .builder \
            .appName("PySpark Demo") \
            .getOrCreate()

        myLogger.info("Spark Session Created")

    def readFile(self, fmt, path, sepby):
        """A Function to read any type of file without any schema given"""

        try:
            self.df = self.spark.read \
                .load(path, format=fmt, header=True, inferSchema=True, sep=sepby)
        except:
            myLogger.error("Failed to read File")
            print("Failed to read File")

        myLogger.info("Reading file " + path)
        return self.df

    def readFileWithSchema(self, fmt, schema, path, sepby):
        """A function to read any file with a schema given"""

        try:
            self.df = self.spark.read.schema(schema).format(fmt) \
                .load(path, header=True, sep=sepby)
        except:
            myLogger.error("Failed to read File")
            print("Failed to read File")

        myLogger.info("Reading file " + path)
        return self.df

    def writeFile(self, df, fmt, mode, name):
        """A function to write  the dataframe into any type of file"""
        try:
            df.write \
                .format(fmt) \
                .mode(mode) \
                .save(name, header=True)
        except:
            myLogger.error("Failed to write File")
            print("Failed to write File")

        myLogger.info("File written as " + name)

    def joinDataFrame(self, df1, df2, joinOn, joinType):
        """A function to join two dataframes into one"""

        df_res = df1.join(df2, on=joinOn, how=joinType)
        return df_res

    def createDataFramefromList(self, list_given, schema_given):
        """A function to create dataframe from a list given"""
        df = self.spark.createDataFrame(list_given, schema=schema_given)
        return df

    def display_only_required(self, df, colNames):
        """A function to keep only the required columns in the dataframe"""

        df_res = df.select(colNames)
        return df_res

    def convertToDate(self, df, newColName, colName, formats=("dd-MM-yyyy", "MM/dd/yyyy")):
        """A function to convert different date formats to one"""

        # res_df = df.withColumn(newColName, expr("to_date(colName, fmt)"))
        self.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        res_df = df.withColumn(newColName, coalesce(*[to_date(colName, fmt) for fmt in formats]))
        return res_df
