# Historical Flight Analysis Project

**Spark version**: 3.2.1

**Scala version**: 2.12.18

**Java version**: Java 11.x

This is a scala application build for Harvard Extension class CSCI-E88c (Programming in Scala for Big Data Systems). This applications takes downloaded CSV files from the Bureau of Transportation Statistics (https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGJ) runs them through some basic aggregation with Spark SQL and outputs parquet files.

## Amazon S3
For this project I utilized Amazon S3 storage for both input and output data storage.

For more details see: https://aws.amazon.com/pm/serv-s3/

## Amazon EMR
To run and execute this application I utilized Amazon's EMR Serverless Studio service.

For more details see: https://aws.amazon.com/emr/ 

## Amazon Redshift
As my query engine and data warehouse storage to run further analytics I used Amazon's Redshift Serveless.

For more details see: https://aws.amazon.com/pm/redshift/

## Command Line Arguments

 Example of command-line arguments (inputPath, outputPath, [optional] "full", referenceTablePath, carrierSummaryPath)

 ```
 ./bin/spark-submit \
  --class "Main" \
  s3://csci-e-88/final_project/script/SparkHistoricalFlightApp.jar  \
  s3a://csci-e-88/final_project/input_data/flight_data/2023/ \
  s3a://csci-e-88/final_project/output_data/ \
  full \
  s3://csci-e-88/final_project/input_data/reference_tables/ \
  s3://csci-e-88/final_project/input_data/carrier_summary/
 ```

## Generating the JAR File
The JAR file will be located in the target/scala-2.12 directory.
```
sbt comile assembly
```

## Dependencies
Listed in the Dependencies.scala file