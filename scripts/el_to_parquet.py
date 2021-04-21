# command-line argument parser
import argparse

# miscellaneous imports
import re

from datetime import datetime

# s3 file handling
import boto3

# pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, sha2, regexp_replace, lit, col, when, length, substring


def create_spark_session(aws_key, aws_secret):
    """Creates a PySpark Session with the specified AWS credentials

    Args:
        aws_key (str): the AWS access key
        aws_secret (str): the AWS access secret

    Returns:
        pyspark.sql.SparkSession: the created PySpark Session
    """

    spark = SparkSession.builder\
        .enableHiveSupport().getOrCreate()

    hadoop_config = spark._jsc.hadoopConfiguration()
    hadoop_config.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")
    hadoop_config.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_config.set("fs.s3n.awsAccessKeyId", aws_key)
    hadoop_config.set("fs.s3n.awsSecretAccessKey", aws_secret)
    hadoop_config.set("fs.s3a.endpoint", "s3.amazonaws.com")

    return spark


def clean_data(data):
    """Cleans the provided PySpark RDD by:
           * replacing empty content
           * removing HTML tags 
           * standardizing column names

    Args:
        data (pyspark.rdd.RDD): the PySpark RDD containing the data

    Returns:
        pyspark.rdd.RDD: the cleaned PySpark RDD
    """
    # numeric attributes should have missing values replaced with -1
    numeric_attributes = ['AreaNet', 'Bathrooms', 'Bedrooms',
                          'PriceCurrencyFormated', 'Latitude', 'Longitude']
    # textual attributes should have missing values replaced with "Unknown"
    textual_attributes = ['Broker', 'Country', 'County',
                          'Description', 'Parish', 'PropertyType', 'Title']

    # replace missing numeric attributes with default values
    data = data.fillna(-1, subset=numeric_attributes)

    # replace missing textual attributes with default values
    data = data.fillna("Unknown", subset=textual_attributes)

    # remove html tags from the Description and Title attributes
    data = data.withColumn('Description', regexp_replace("Description", """<(?!\/?a(?=>|\s.*>))\/?.*?>""", "")). \
        withColumn('Title', regexp_replace(
            "Title", """<(?!\/?a(?=>|\s.*>))\/?.*?>""", ""))

    # rename the price column to match the database's
    data = data.withColumnRenamed("PriceCurrencyFormated", "Price")

    return data


def limit_length(data, column_name, length_threshold):
    """Truncates a column to a maximum length, effectivelly removing any content above the specified threshold.

    Args:
        data (pyspark.rdd.RDD): the target Pyspark RDD
        column_name (str): the column to limit
        length_threshold (int): the maximum column length, above which, data is deleted

    Returns:
        pyspark.rdd.RDD: the modified RDD
    """
    column_obj = col(column_name)

    data = data.withColumn(column_name,
                           when(length(column_obj) > length_threshold, substring(
                               column_obj, 1, length_threshold)).otherwise(column_obj)
                           )

    return data


def to_snake_case(data):
    """Converts all column names in a given Spark RDD to snake case

    Args:
        data (pyspark.rdd.RDD): the target Pyspark RDD

    Returns:
        pyspark.rdd.RDD: the modified Pyspark RDD
    """
    # rename all columns to Snake Case using Uppercase as the delimiter
    for column in data.columns:
        column_tokens = re.sub(r"([A-Z])", r" \1", column).split()
        column_tokens = [token.lower() for token in column_tokens]
        new_column_name = '_'.join(column_tokens)

        # faster than using toDF(*columns)
        data = data.withColumnRenamed(column, new_column_name)

    return data


def remove_tmp_files(aws_key, aws_secret, bucket, prefix):
    """Removes metadata and temporary files from an S3 bucket

    Args:
        aws_key (str): the AWS access key
        aws_secret (str): the AWS secret key
        bucket (str): the AWS S3 bucket
        prefix (str): the AWS S3 bucket's prefix
    """    
    
    s3 = boto3.client('s3', aws_access_key_id=aws_key,
                      aws_secret_access_key=aws_secret)
    deletion_pattern = ['_SUCESS', '_committed', '_started']

    response = s3.list_objects(
        Bucket=bucket,
        Prefix=prefix
    )

    # loop through each file
    for file in response['Contents']:
        file_name = file['Key']

        # delete any file matching the provided pattern
        if any(domain in file_name for domain in deletion_pattern):
            s3.delete_object(Bucket=bucket, Key=file_name)


def to_parquet(data, destination):
    """Saves the provided PySpark RDD into the provided destination in a Parquet format

    Args:
        data (pyspark.rdd.RDD): [description]
        destination (str): [description]

    Raises:
        ValueError: invalid input arguments
    """    
    
    if data is None:
        raise ValueError('to_parquet:: Could not retrieve data')

    if destination is None or len(destination) == 0:
        raise ValueError('to_parquet:: Could not retrieve destination path')

    data.write.parquet(destination, mode='overwrite')


def create_dimensional_partitions(data, parquet_loc, execution_date):
    """Creates the dimensional objects (Dimensions, Facts) from a PySpark RDD and outputs them to the Parquet format
       to be processed by Redshift.

    Args:
        data (pyspark.rdd.RDD): the base PySpark RDD
        parquet_loc (str): the output path where the Parquet files are to be saved
        execution_date (str): the execution date
    """

    broker_staging = data.select(['broker']).distinct()

    # calculate hash using SHA2
    broker_staging = broker_staging.withColumn(
        "hash", sha2(concat_ws("||", *broker_staging.columns), 256))

    # geography dim
    geography_staging = data.select(['country', 'county', 'parish']).distinct()
    geography_staging = geography_staging.withColumn("hash", sha2(
        concat_ws("||", *geography_staging.columns), 256))
    
    asset_staging = data.select(['contract_number', 'country', 'county', 'parish', 'title', 'description',
                            'price', 'property_type', 'bathrooms', 'bedrooms', 'area_net', 'latitude', 'longitude']).distinct()

    # calculate hash using SHA2
    asset_staging = asset_staging.withColumn(
        "hash", sha2(concat_ws("||", *asset_staging.columns), 256))

    # weekly stock base
    asset_stock = data.select(['broker', 'contract_number', 'country', 'county', 'parish', 'price']).withColumn(
        "quantity", lit(1)).withColumn("stock_date", lit(execution_date))

    # save the data onto parquet to be consumed by Redshift
    broker_staging_loc = parquet_loc + "broker_staging.parquet"
    asset_staging_loc = parquet_loc + "asset_staging.parquet"
    geography_staging_loc = parquet_loc + "geography.parquet"
    stock_staging_loc = parquet_loc + "asset_stock.parquet"

    to_parquet(broker_staging, broker_staging_loc)
    to_parquet(asset_staging, asset_staging_loc)
    to_parquet(geography_staging, geography_staging_loc)
    to_parquet(asset_stock, stock_staging_loc)

def load_json_source(spark, source_path):
    """Loads the JSON source data in the source_path to a Pyspark RDD

    Args:
        spark (pyspark.sql.SparkSession): the PySpark Session to be used in the loading process
        source_path (str): the path containing the source JSON files

    Returns:
        pyspark.rdd.RDD: the PySpark RDD containing the loaded data
    """

    json_data = spark.read.format(
        'json').options(inferSchema='true').load(source_path)

    # the allowed attribute subset, define a common base for all sources
    allowed_attributes = ['Broker', 'ContractNumber', 'Country', 'County', 'Parish', 'Title', 'Description',
                          'PriceCurrencyFormated', 'PropertyType', 'Bathrooms', 'Bedrooms', 'AreaNet', 'Latitude', 'Longitude']

    # select only the common subset of attributes
    json_data = json_data.select(allowed_attributes)

    return json_data


def main():
    parser = argparse.ArgumentParser(prog='extract_to_parquet',
                                     description='Extract the data from JSON files and dump into a parquet staging layer'
                                     )

    # DAG execution date
    parser.add_argument('-ds',
                        '--execution_date',
                        # type=lambda ds: date.fromisoformat(ds), Python 3.7 only, EMR runs on Python 3.6
                        type=lambda ds: datetime.strptime(
                            ds, "%Y-%m-%d").date(),
                        required=True,
                        help='The execution date in ISO format'
                        )

    # AWS configuration
    parser.add_argument('-awsk',
                        '--aws_key',
                        type=str,
                        required=True
                        )

    parser.add_argument('-awss',
                        '--aws_secret',
                        type=str,
                        required=True
                        )

    # S3 bucket location without the protocol, e.g. 's3://my_bucket' should be 'my_bucket
    parser.add_argument('-s3b',
                        '--s3_bucket',
                        type=str,
                        required=True,
                        default='a',
                        help='The source (JSON) and destination (parquet) S3 bucket name'
                        )

    parser.add_argument('-s3p',
                        '--s3_path_template',
                        type=str,
                        required=False,
                        default='/{year}/{month}/{week}/',
                        help='The source (JSON) and destination (parquet) S3 bucket template')

    parser.add_argument('-s3f',
                        '--s3_path_subfolder',
                        type=str,
                        required=False,
                        default='tmp',
                        help='The subfolder within the s3_path_template to be used for the temporary (parquet) files for Redshift')

    args = parser.parse_args()

    # parse the configuration data
    date_formatted = args.execution_date
    aws_key = args.aws_key
    aws_secret = args.aws_secret
    s3_bucket = args.s3_bucket
    s3_path_template = args.s3_path_template
    # the subfolder within s3_path_template where the temporary parquet files are to be stored to be used by Redshift
    s3_path_subfolder = args.s3_path_subfolder

    # generate the time metadata to be injected to the s3 template path
    year = date_formatted.year
    month = date_formatted.month
    week = date_formatted.isocalendar()[1]
    day = date_formatted.day

    s3_path = s3_path_template.format(year=year,
                                      month=month,
                                      week=week,
                                      day=day
                                      )

    # the input data's (JSON) location
    data_loc = s3_bucket + s3_path

    # the s3 bucket location s3://bucket_name/template_path/*.json
    json_loc = 's3://' + data_loc + '*.json'

    # the parquet destination path
    parquet_loc = 's3://' + data_loc + s3_path_subfolder + "/"

    # create a spark session configured with the AWS credentials
    spark = create_spark_session(
        aws_key=aws_key, aws_secret=aws_secret)

    # fetch the base JSON data for the applicable time period via json_loc
    base_data = load_json_source(spark, json_loc)

    # cache the base_data given it will be reused further on
    base_data.cache()

    # perform data cleanup, normalization and limit
    base_data = clean_data(base_data)
    base_data = limit_length(base_data, 'Description', 250)
    base_data = limit_length(base_data, 'Title', 250)

    # convert all column names to snake case
    base_data = to_snake_case(base_data)

    # create the partitions and save the parquet files to be consumed by Redshift
    create_dimensional_partitions(
        data=base_data, parquet_loc=parquet_loc, execution_date=date_formatted)

    # remove temporary residual metadata files in S3

    # the prefix starts with a /, boto3 requires the prefix without the trailing slash
    prefix_trailed = s3_path[1:]
    bucket = s3_bucket

    remove_tmp_files(aws_key, aws_secret, bucket,  prefix_trailed)
    
    print('Extraction and Loading to parquet complete.')


if __name__ == "__main__":
    main()
