import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as fn
from pyspark.sql.functions import udf
import uuid
import boto3
import json
import os

from capstonellm.common.spark import ClosableSparkSession

logger = logging.getLogger(__name__)

def write_row(title, question_body, response_body, tag):
    row = {}
    row['title'] = title
    row['question_body'] = question_body
    row['response_body'] = response_body
    filename=f'{uuid.uuid4()}.json'

    with open(filename, "w") as outfile:
        json.dump(row, outfile)

    s3 = boto3.client('s3')
    bucket='dataminded-academy-capstone-llm-data-us'
    s3.put_object(Body= json.dump(row, outfile), Bucket=bucket, Key=f'/mattias/cleaned/udf/{tag}/{filename}')
    return 1

write_row_udf = udf(write_row)


def clean(spark: SparkSession, environment: str, tag: str):
    bucket='dataminded-academy-capstone-llm-data-us'
    q = spark.read.json(f's3a://{bucket}/input/{tag}/questions.json').select(fn.explode(fn.col('items'))).select('col.*').alias('question')
    q.show(1,False)
    a = spark.read.json(f's3a://{bucket}/input/{tag}/answers.json').select(fn.explode(fn.col('items'))).select('col.*').alias('answer')
    a.show(1,False)

    r = (q
        #.join(a, [fn.col('question.accepted_answer_id') == fn.col('answer.answer_id')], 'left')
        .join(a, [fn.col('question.question_id') == fn.col('answer.question_id')], 'inner')
        .select(
            fn.col('question.title').alias('title'), 
            fn.col('question.body').alias('question_body'), 
            fn.col('answer.body').alias('response_body'),
            fn.col('question.link').alias('link')
            )
        )

    r.show(1,False)
    #r.withColumn('write_col', write_row_udf(fn.col('title'), fn.col('question_body'), fn.col('response_body'), fn.lit(tag))).show(10)
    #write_location = f's3a://{bucket}/mattias/cleaned/{tag}'
    write_location = '../data_temp'
    r.repartition(r.count()).write.json(write_location, mode='overwrite')

    # Move files to S3
    s3 = boto3.client('s3')

    # Remove existing files
    objects_to_delete = s3.list_objects(Bucket=bucket, Prefix=f'/mattias/cleaned/udf/{tag}/')
    if len(objects_to_delete.get('Contents', [])) > 0: 
        delete_keys = {'Objects' : [{'Key' : k} for k in [obj['Key'] for obj in objects_to_delete.get('Contents', [])]]}
        s3.delete_objects(Bucket=bucket, Delete=delete_keys)
    
    # Copy new files
    for root,dirs,files in os.walk(write_location):
        for file in files:
            extension = file.split('.')[-1]
            if extension == 'json':
                print(f'{file}')
                s3.upload_file(f'{write_location}/{file}', bucket, f'/mattias/cleaned/udf/{tag}/{file}')
            os.remove(f'{write_location}/{file}')

def main():
    parser = argparse.ArgumentParser(description="capstone_llm")
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=True
    )
    parser.add_argument(
        "-t", "--tag", dest="tag", help="the tag to process",
        default="python-polars", required=False
    )
    logger.info("starting the cleaning job")

    args = parser.parse_args()
    common_spark_config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    }
    if args.env == "local":
        session = (
            SparkSession.builder.appName("Spark S3 Integration")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
            .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
            .config("fs.s3a.committer.name", "magic")
            .getOrCreate()
        )
        clean(session, args.env, args.tag)
    else:
        with ClosableSparkSession("capstone_llm", spark_config=common_spark_config) as session:
            clean(session, args.env, args.tag)


if __name__ == "__main__":
    main()
