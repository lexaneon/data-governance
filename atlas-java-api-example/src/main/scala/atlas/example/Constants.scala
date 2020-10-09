package atlas.example

case object Constants{
  val BASE_URL: String = "http://localhost:21000/"
  val USERNAME: String = "admin"
  val PASSWORD: String = "admin"
  val userPass = Array(USERNAME, PASSWORD)

  val AWS_S3_BUCKET = "aws_s3_bucket"
  val AWS_S3_PSEUDO_DIR = "aws_s3_pseudo_dir"
  val AWS_S3_OBJECT = "aws_s3_object"
  val HDFS_PATH = "hdfs_path"
  val SPARK_PROCESS = "spark_process"

}



