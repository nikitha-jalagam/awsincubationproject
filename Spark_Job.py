# Importing packages
import sys
import json
from pyspark.sql import functions as f
from pyspark.sql.functions import concat_ws,lit
from pyspark.sql.types import DecimalType
from pyspark.sql import SparkSession
from datetime import date

# Initialising Spark
spark = SparkSession.builder.appName("Spark_Job").getOrCreate()

# Loading Delta from Jar in S3
delta_jar_path = sys.argv[2]
spark.sparkContext.addPyFile(delta_jar_path)

# Importing delta
from delta.tables import *


class DataTransformations:

    # Loading app-config file when class is called
    def __init__(self):
        self.app_config_path = spark.conf.get("spark.path")
        self.config_data = spark.sparkContext.textFile(self.app_config_path).collect()
        self.file_data = ''.join(self.config_data)
        self.json_data = json.loads(self.file_data)

    # Reading values from app-config file
    def read_configuration(self):
        self.ingest_actives_source = self.json_data['ingest-actives']['source']['data-location']
        self.ingest_actives_destination = self.json_data['ingest-actives']['destination']['data-location']
        self.transform_actives_source = self.json_data['transformed-actives']['source']['data-location']
        self.transform_actives_destination = self.json_data['transformed-actives']['destination']['data-location']
        self.ingest_viewership_source = self.json_data['ingest-viewership']['source']['data-location']
        self.ingest_viewership_destination = self.json_data['ingest-viewership']['destination']['data-location']
        self.transform_viewership_source = self.json_data['transformed-viewership']['source']['data-location']
        self.transform_viewership_destination = self.json_data['transformed-viewership']['destination']['data-location']
        self.actives_mask_cols = self.json_data['transformed-actives']['masking-cols']
        self.actives_transform_cols = self.json_data['transformed-actives']['transformation-cols']
        self.actives_partition_cols = self.json_data['transformed-actives']['partition-cols']
        self.actives_pii_cols = self.json_data['actives-pii-cols']
        self.viewership_mask_cols = self.json_data['transformed-viewership']['masking-cols']
        self.viewership_transform_cols = self.json_data['transformed-viewership']['transformation-cols']
        self.viewership_partition_cols = self.json_data['transformed-viewership']['partition-cols']
        self.viewership_pii_cols = self.json_data['viewership-pii-cols']
        self.delta_table_location = self.json_data['delta-table-location']

    # Reading parquet data from S3 Location
    def read_data(self,source_location):
        df = spark.read.parquet(source_location)
        return df

    # Writing parquet data to S3 Location
    def write_data(self,df,destination_location,partition_cols=[]):
        if partition_cols:
            df.write.mode("overwrite").partitionBy(partition_cols).parquet(destination_location)
        else:
            df.write.mode("overwrite").parquet(destination_location)

    # Transforming the required columns
    def transform_cols(self,df,transform_cols):
        for transform_col_name,transform_col_type in transform_cols.items():
            if transform_col_type == 'ArrayType-StringType':
                df = df.withColumn(transform_col_name,concat_ws(",",df[transform_col_name]))
            elif transform_col_type.split(',')[0] == 'DecimalType':
                precision = int(transform_col_type.split(',')[1])
                df = df.withColumn(transform_col_name,df[transform_col_name].cast(DecimalType(precision+3,precision)))
        return df

    # Masking the PII columns
    def mask_cols(self,df,mask_cols):
        for mask_col_name in mask_cols:
            df = df.withColumn(mask_col_name,f.sha2(df[mask_col_name], 256))
        return df

    # Implementing SCD2 Lookup Table
    def scd2_lookup(self,raw_df,pii_cols):
        # Getting Unmasked PII columns from Raw Zone DF
        tempdf = raw_df.select(pii_cols)
        new_pii_cols = []
        # Creating masked PII columns
        for pii_col_name in pii_cols:
            tempdf = tempdf.withColumn("masked_"+pii_col_name,f.sha2(tempdf[pii_col_name], 256))
            new_pii_cols += [pii_col_name,"masked_"+pii_col_name] # For re-ordering the DF
        # Reordering the DF
        tempdf = tempdf.select(new_pii_cols)
        # Adding missing(user_id) PII columns as null for viewership df
        if sys.argv[1]=='viewership':
            tempdf = tempdf.withColumn(self.actives_pii_cols[1],lit('null'))
            tempdf = tempdf.withColumn('masked_'+self.actives_pii_cols[1],lit('null'))
        # Adding start_date column to df
        tempdf = tempdf.withColumn("start_date",lit(date.today()))
        # Updated Column Names List
        new_pii_cols = tempdf.columns
        # Creating initdf with end_date and active columns for table creation
        initdf = tempdf.withColumn("end_date",lit('null'))
        initdf = initdf.withColumn("active",lit(True))
        # Checking if Delta Table exists and Updating the table
        if DeltaTable.isDeltaTable(spark, self.delta_table_location):
            # Retrieving Delta Table
            Delta_Lookup_Table = DeltaTable.forPath(spark,delta_table_path)
            # Converting Delta Table to DF
            deltadf = Delta_Lookup_Table.toDF()
            # Assigning the tempdf as updatesDF
            updatesDF = tempdf
            # Rows that will be inserted as part of the upsert, mergeKey column is null, joined with "advertising_id"
            # new_pii_cols[0] --> "advertising_id" ; new_pii_cols[2] --> "user_id"
            stagedPart1 = updatesDF.alias("updates").join(deltadf.alias("final"), new_pii_cols[0]).where("updates."+new_pii_cols[2]+" <> final."+new_pii_cols[2]).where("final.active = true").selectExpr("null as mergeKey", "updates.*")
            # All rows from the tempdf with mergeKey column taken from advertising_id
            stagedPart2 = updatesDF.selectExpr(pii_cols[0]+" as mergeKey", "*")
            # stagedUpdates is union of stagedPart1 and stagedPart2
            stagedUpdates = stagedPart1.union(stagedPart2)
            # Appling SCD Type 2 Operation using Merge
            Delta_Lookup_Table.alias("final").merge(
                source = stagedUpdates.alias("staged_updates"),
                condition = "final." + new_pii_cols[0] + " = mergeKey"
            ).whenMatchedUpdate(
                condition = "final.active = true AND (staged_updates.new_pii_cols[2] <> final.new_pii_cols[2])",
                set = {
                    # Set active as false and endDate as new record's start date.
                    "end_date": "staged_updates.start_date",
                    "active": "false"
                }
            ).whenNotMatchedInsert(
                values = {
                # Set active to true along with the new record details
                    new_pii_cols[0]: "staged_updates.advertising_id",
                    new_pii_cols[1]: "staged_updates.masked_advertising_id",
                    new_pii_cols[2]: "staged_updates.user_id",
                    new_pii_cols[3]: "staged_updates.masked_user_id",
                    new_pii_cols[4]: "staged_updates.start_date",
                    "end_date": "null",
                    "active": "true"
                }
            ).execute()
        else:
            # Creating new delta table
            initdf.write.format("delta").mode("overwrite").save(self.delta_table_location)


# Creating an object
s = DataTransformations()

# Reading the app-config file
s.read_configuration()

# Triggering functions if actives file is uploaded
if sys.argv[1]=='actives':
    # Reading data present in Landing Zone
    ingest_actives_df = s.read_data(s.ingest_actives_source)

    # Displaying landing data
    #ingest_actives_df.show(5,truncate=False)

    # Writing data to Raw Zone
    s.write_data(ingest_actives_df,s.ingest_actives_destination)

    # Reading data present in Raw Zone
    raw_actives_df = s.read_data(s.transform_actives_source)

    # Transforming required columns
    transformed_actives_df = s.transform_cols(raw_actives_df,s.actives_transform_cols)

    # Masking PII columns
    transformed_actives_df = s.mask_cols(transformed_actives_df,s.actives_mask_cols)

    # SCD2 Lookup Store
    s.scd2_lookup(raw_actives_df,s.actives_pii_cols)

    # Displaying transformed data
    #transformed_actives_df.show(5,truncate=False)

    # Writing transformed data to Staging Zone
    s.write_data(transformed_actives_df,s.transform_actives_destination,s.actives_partition_cols)

# Triggering functions if viewership file is uploaded
elif sys.argv[1]=='viewership':
    # Reading data present in Landing Zone
    ingest_viewership_df = s.read_data(s.ingest_viewership_source)

    # Displaying landing data
    #ingest_viewership_df.show(5,truncate=False)

    # Writing data to Raw Zone
    s.write_data(ingest_viewership_df,s.ingest_viewership_destination)

    # Reading data present in Raw Zone
    raw_viewership_df = s.read_data(s.transform_viewership_source)

    # Transforming required columns
    transformed_viewership_df = s.transform_cols(raw_viewership_df,s.viewership_transform_cols)

    # Masking PII columns
    transformed_viewership_df = s.mask_cols(transformed_viewership_df,s.viewership_mask_cols)

    # SCD2 Lookup Store
    s.scd2_lookup(raw_viewership_df,s.viewership_pii_cols)

    # Displaying transformed data
    #transformed_viewership_df.show(5,truncate=False)

    # Writing transformed data to Staging Zone
    s.write_data(transformed_viewership_df,s.transform_viewership_destination,s.viewership_partition_cols)
