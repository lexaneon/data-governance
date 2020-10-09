package atlas.example

import org.apache.atlas.AtlasClientV2
import org.apache.spark.sql.SparkSession

object SampleRun extends App{

  val spark = SparkSession
    .builder()
    .appName("Spark test application2")
    .master("local[*]")
    .getOrCreate()

  val s3ObjectGuidList = MyAtlasClientV2.registerAWSS3Objects("s3://some_bucket_name/example_key/example_key_continue/example_file.json", "s3://some_bucket_name/example_key/example_key_continue/example_file2.json")
  val sparkObjectGuid =  MyAtlasClientV2.registerSparkProcess(spark, List(s3ObjectGuidList(0)-> Constants.AWS_S3_OBJECT), List(s3ObjectGuidList(1) -> Constants.AWS_S3_OBJECT))


}
