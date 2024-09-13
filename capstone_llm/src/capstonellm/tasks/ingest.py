import argparse

import logging
logger = logging.getLogger(__name__)

def ingest(tag: str):
    # # Connect with S3
    # s3 = boto3.client('s3')
    # bucket='dataminded-academy-capstone-llm-data-us'
    
    # # Generate S3 object key and local filename
    # key_question = f'input/{tag}/questions.json'
    # file_name_question = key_question.replace('/', '_')

    # key_answer = f'input/{tag}/answers.json'
    # file_name_answer = key_answer.replace('/', '_')

    # # Download files 
    # s3.download_file(bucket, key_question, f'../../data/{file_name_question}')
    # s3.download_file(bucket, key_answer, f'../../data/{file_name_answer}')
    pass

def main():
    parser = argparse.ArgumentParser(description="stackoverflow ingest")
    parser.add_argument(
        "-t", "--tag", dest="tag", help="Tag of the question in stackoverflow to process",
        default="python-polars", required=False
    )
    args = parser.parse_args()
    logger.info("Starting the ingest job")

    ingest(args.tag)


if __name__ == "__main__":
    main()
