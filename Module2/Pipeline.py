from Utilities import *
from CustomerFeatures import CustomerFeature


class Pipeline(CustomerFeature):
    def __init__(self):
        myLogger.info("Calling Pipeline Constructor")

        self.customerFeatureObject = None

    def create_Customer_Feature_Object(self, fmt, path):
        self.customerFeatureObject = CustomerFeature(fmt, path)

    def customer_feature_transform(self):
        df = self.customerFeatureObject.cst_df
        self.customerFeatureObject.customer_transform(df)


pipeline = Pipeline()
pipeline.create_Customer_Feature_Object("csv", "data.csv")
pipeline.customer_feature_transform()
