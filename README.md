# Taxi_Stream_Analytics
## Introduction

Welcome to the Real-Time Taxi Trip Data Processing project with AWS Kinesis and DynamoDB! This project is designed to ingest data from a Kinesis stream and store it in a DynamoDB table using AWS Lambda. The Kinesis stream is fed by a data generator that generates JSON records representing taxi trips.


## Architecture

![text alternatif](https://github.com/yassinetaiki/Taxi_Nyc_Stream_Analytics/blob/master/architecture.PNG)

## Repository Contents

- `generate_data.py` script generates simulated JSON records representing taxi trips in .The generated records are then placed into the 'input-stream' Kinesis stream using the Boto3 library's Kinesis client.
- `lambda_function.py` script defines the AWS Lambda function `lambda_handler`. The function receives records from the Kinesis stream and uses aggregation to calculate the total distance traveled and total trip duration for each taxi vendor (vendorId). The results are stored in a state structure that is passed with each function call.
When the state is complete for a processing window, the results are stored in the 'my-dynamodb-table' DynamoDB table using the Boto3 library's API.
- `architecture.png`: Architecture diagram of the project.

### 3. Aws service 
- `AWS Cloud9`: AWS Cloud9 is an integrated development environment (IDE) that provides a cloud-based code editor, terminal, and development environment. It was used to write, test, and debug the Python scripts for data generation and AWS Lambda function.

- `Amazon Kinesis`: Amazon Kinesis is a real-time data streaming service that allows you to collect, process, and analyze data in real-time. In this project, we used Kinesis as the data streaming platform, ingesting the generated taxi trip data into the 'input-stream' Kinesis stream.

- `AWS Lambda`: AWS Lambda is a serverless compute service that allows you to run code without managing servers. It was used to process the incoming data from the Kinesis stream in real-time and aggregate the results for each taxi vendor.

- `Amazon DynamoDB`: Amazon DynamoDB is a fully managed NoSQL database service that provides fast and predictable performance with seamless scalability. In this project, DynamoDB was used to store the aggregated results for each taxi vendor, using 'vendorId' as the primary key for efficient data retrieval.

- `Amazon Kinesis Data Firehose`: Amazon Kinesis Data Firehose is a fully managed service that allows you to capture, transform, and load streaming data into data stores and analytics tools. In this project, we could use Kinesis Data Firehose to load the data from the Kinesis stream directly into Amazon S3.

- `Amazon S3` (Simple Storage Service): Amazon S3 is a scalable object storage service that offers industry-leading durability, availability, and performance. In this project, we could use S3 to store the raw data streamed from Kinesis or the processed data for further analysis or long-term storage.

- `AWS Glue Catalog`: AWS Glue Catalog is a fully managed metadata catalog that allows you to discover, catalog, and share metadata for your data assets. In this project, we could use Glue Catalog to maintain metadata about the data stored in S3, making it easier to query and analyze the data with Amazon Athena.

- `Amazon Athena`: Amazon Athena is an interactive query service that enables you to analyze data directly from Amazon S3 using standard SQL. In this project, we could use Athena to run SQL queries on the data stored in S3 through the Glue Catalog without the need for complex data pipelines.

