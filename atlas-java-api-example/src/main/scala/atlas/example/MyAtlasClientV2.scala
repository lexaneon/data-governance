package atlas.example

import org.apache.atlas.AtlasClientV2
import org.apache.atlas.model.instance.{AtlasEntity, AtlasEntityHeader}
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import collection.JavaConverters._

object MyAtlasClientV2 extends AtlasClientV2 (Array(Constants.BASE_URL), Array(Constants.USERNAME, Constants.PASSWORD)){

  val logger = Logger.getLogger(this.getClass.getName)
  logger.info(s"BASE_URL = ${Constants.BASE_URL}")
  logger.info(s"USERNAME = ${Constants.USERNAME}")
  logger.info(s"PASSWORD = ${Constants.PASSWORD}")
  /**
   *
   * @param keyFullName - list of s3 key for registration
   * @return Seq of guid
   */
  def registerAWSS3Objects(keyFullName: String*): Seq[String] = {
    keyFullName.map(element => registerAWSS3Object(element))
  }

  /**
   *
   * @param awsS3ObjectFullName - object full name with s3 prefix & bucket name
   * @return entity guid
   */
  def registerAWSS3Object(awsS3ObjectFullName: String): String = {
    if (!awsS3ObjectFullName.toLowerCase.startsWith("s3://")) throw new IllegalArgumentException("s3 prefix doesn't exist")

    val objectComponents = awsS3ObjectFullName.toLowerCase.split("s3://")(1).split("/")

    val awsS3BucketGuid = registerAWSS3Bucket("s3://"+ objectComponents(0))
    val awsS3PseudoDirGuid = registerAWSS3PseudoDir(objectComponents.slice(1, objectComponents.length-2).mkString("/"), awsS3BucketGuid)

    val entityType = Constants.AWS_S3_OBJECT
    val entity = new AtlasEntity(entityType)
    entity.setAttribute("qualifiedName", awsS3ObjectFullName)
    entity.setAttribute("name", objectComponents.last)
    entity.setAttribute("pseudoDirectory", Map(("guid" -> awsS3PseudoDirGuid), ("typeName" -> Constants.AWS_S3_PSEUDO_DIR)).asJava)
    val awsS3Entity = createEntity(new AtlasEntityWithExtInfo(entity))
    val guid = awsS3Entity.getGuidAssignments.asScala.head._2
    logger.info(s"s3 object was registered with guid: $guid")
    guid

  }

  /**
   * Create/update aws s3 pseudo dir
   * @param pseudoDirName
   * @param bucketGuid
   * @return entity guid
   */
  def registerAWSS3PseudoDir(pseudoDirName: String, bucketGuid: String): String = {
    val entityType = Constants.AWS_S3_PSEUDO_DIR
    val entity = new AtlasEntity(entityType)
    entity.setAttribute("qualifiedName", pseudoDirName)
    entity.setAttribute("name", pseudoDirName)
    entity.setAttribute("objectPrefix", pseudoDirName)
    entity.setAttribute("bucket", Map(("guid" -> bucketGuid), ("typeName" -> entityType)).asJava)
    val guid = this.createEntity(new AtlasEntityWithExtInfo(entity)).getGuidAssignments.asScala.head._2
    logger.info(s"s3 pseudo dir was registered with guid: $guid")
    guid
  }
  /**
   * Create/update bucket and return it guid
   * @param bucketName - full bucket name, like: s3://bucket_name
   * @return entity guid
   */
  def registerAWSS3Bucket(bucketName: String): String = {
    val entityType = Constants.AWS_S3_BUCKET
    val entity = new AtlasEntity(entityType)
    entity.setAttribute("qualifiedName", bucketName)
    entity.setAttribute("name", bucketName)
    val guid = createEntity(new AtlasEntityWithExtInfo(entity)).getGuidAssignments.asScala.head._2
    logger.info(s"s3 bucket was registered with guid: $guid")
    guid
  }

  /**
   *
   * @param spark
   * @param inputDataSet format: DataSet guid -> object type, for example: 0cb39950-c48c-42cd-b2ca-a54f89dd5d13 -> aws_s3_object
   * @param outputDataSet format: DataSet guid -> object type, for example: 0cb39950-c48c-42cd-b2ca-a54f89dd5d13 -> aws_s3_object
   * @return entity guid
   */
  def registerSparkProcess(spark: SparkSession, inputDataSet: Seq[(String, String)], outputDataSet: Seq[(String, String)]): String = {

    val input = inputDataSet.map(x => Map("guid"-> x._1, "typeName" -> x._2).asJava).asJava
    val output = outputDataSet.map(x => Map("guid"-> x._1, "typeName" -> x._2).asJava).asJava

    val atlasEntity = new AtlasEntity()
    atlasEntity.setTypeName(Constants.SPARK_PROCESS)
    atlasEntity.setAttribute("name", spark.sparkContext.appName)
    atlasEntity.setAttribute("qualifiedName", spark.sparkContext.appName.toLowerCase)
    atlasEntity.setAttribute("inputs", input)
    atlasEntity.setAttribute("outputs", output)

    val guid = createEntity(new AtlasEntityWithExtInfo(atlasEntity)).getGuidAssignments.asScala.head._2
    logger.info(s"spark process was registered with guid: $guid")
    guid
  }

}
