package org.ekstep.analytics.dashboard.report.assess

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.{DummyInput, DummyOutput}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}

import java.io.Serializable

object UserModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.assess.UserModel"
  override def name() = "UserModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(data: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = data.first().timestamp  // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processCompetencyMetricsData(timestamp, config)
    sc.parallelize(Seq())  // return empty rdd
  }

  override def postProcess(data: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq())  // return empty rdd
  }


//  def processData(config: String)(implicit fc: Option[FrameworkContext]): DataFrame = {
//    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](config)
//    implicit val spark: SparkSession = null // openSparkSession(jobConfig)
//    //  assessmentReportDf()
//  }

  //  def openSparkSession(config: JobConfig): SparkSession = {
  //
  //    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
  //    val sparkCassandraConnectionHost = modelParams.get("sparkCassandraConnectionHost")
  //    //    val sparkElasticsearchConnectionHost = ""
  //    JobContext.parallelization = CommonUtil.getParallelization(config)
  //    val readConsistencyLevel = modelParams.getOrElse("cassandraReadConsistency", "LOCAL_QUORUM").asInstanceOf[String];
  //    val writeConsistencyLevel = modelParams.getOrElse("cassandraWriteConsistency", "LOCAL_QUORUM").asInstanceOf[String]
  //    val sparkSession = CommonUtil.getSparkSession(JobContext.parallelization, config.appName.getOrElse(config.model), sparkCassandraConnectionHost, Option(readConsistencyLevel))
  //    println("before setReportsStorageConfiguration")
  //    setReportsStorageConfiguration(config)(sparkSession)
  //    sparkSession;
  //
  //  }
  //
  //  def setReportsStorageConfiguration(config: JobConfig)(implicit spark: SparkSession) {
  //
  //    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
  //    val store = modelParams.getOrElse("store", "s3").asInstanceOf[String];
  //    val storageKey = modelParams.getOrElse("storageKeyConfig", "reports_storage_key").asInstanceOf[String];
  //    val storageSecret = modelParams.getOrElse("storageSecretConfig", "reports_storage_secret").asInstanceOf[String];
  //    println(store)
  //    store.toLowerCase() match {
  //      case "s3" =>
  //        spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", AppConf.getConfig(storageKey));
  //        spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", AppConf.getConfig(storageSecret));
  //      case "ceph" =>
  //        spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", AppConf.getConfig(storageKey));
  //        spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", AppConf.getConfig(storageSecret));
  //      case "azure" =>
  //        val storageKeyValue = AppConf.getConfig(storageKey);
  //        spark.sparkContext.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
  //        spark.sparkContext.hadoopConfiguration.set(s"fs.azure.account.key.$storageKeyValue.blob.core.windows.net", AppConf.getConfig(storageSecret))
  //        spark.sparkContext.hadoopConfiguration.set(s"fs.azure.account.keyprovider.$storageKeyValue.blob.core.windows.net", "org.apache.hadoop.fs.azure.SimpleKeyProvider")
  //      case _ =>
  //
  //    }
  //
  //  }
  //
  //  def getStorageConfig(config: JobConfig, key: String): StorageConfig = {
  //
  //    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
  //    val container = "igot"
  //    val storageKey = modelParams.getOrElse("storageKeyConfig", "reports_storage_key").asInstanceOf[String];
  //    val storageSecret = modelParams.getOrElse("storageSecretConfig", "reports_storage_secret").asInstanceOf[String];
  //    val store = modelParams.getOrElse("store", "local").asInstanceOf[String]
  //    StorageConfig(store, container, key, Option(storageKey), Option(storageSecret));
  //  }


  def assessmentReportDf()(implicit spark: SparkSession, config: JobConfig): DataFrame = {
    val assessmentData = userAssessmentDataDf()
    val assessmentIds = userAssessmentDataDf().select("course_id").distinct()
    var df = assessmentData.join(courseDataDbDf(assessmentIds), Seq("course_id"))
      .join(userDataDbDf(), Seq("user_id"))
    //      .join(contentDataDbDf(), Seq("content_id"))
    //    df = df.join(assessmentStatusDf(), Seq("user_id"))
    df = df.distinct()
    show(df, "Assessment Report")
    df.show()
    df
  }

//  val submitassessmentrequestSchema: StructType = StructType(Seq(
//    StructField("primaryCategory", StringType, nullable = false),
//    StructField("courseId", StringType, nullable = false)
//  ))
//
//  val submitassessmentresponseSchema: StructType = StructType(Seq(
//    StructField("pass", BooleanType, nullable = false),
//    StructField("result", StringType, nullable = false) // check the datatype
//  ))
//
//  def userAssessmentDataDf()(implicit spark: SparkSession, config: JobConfig): DataFrame = {
//    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]())
//    var df = cassandraTableAsDataFrame("sunbird", "user_assessment_data")
//      .select(col("userid").alias("user_id"), col("assessmentid"),
//        col("submitassessmentresponse"), col("submitassessmentrequest"))
//    df = df.withColumn("requestData", from_json(col("submitassessmentrequest"), submitassessmentrequestSchema))
//    var df1 = df.select(
//      col("requestData.primaryCategory").alias("type"),
//      col("requestData.courseId").alias("course_id"),
//      col("user_id")
//    )
//
//
//    df = df.withColumn("responseData", from_json(col("submitassessmentresponse"), submitassessmentresponseSchema))
//    var df2 = df.select(
//      col("responseData.pass").alias("pass"),
//      col("responseData.result").alias("percentage_score"),
//      col("user_id")
//    )
//
//
//    df = df.join(df1, Seq("user_id")).join(df2, Seq("user_id"))
//
//
//    df = df.select(
//      col("user_id"), col("assessmentid"), col("pass"), col("type"), col("course_id"),
//      col("percentage_score")
//    )
//    df.show()
//    df
//  }

  /**
   * Schema for course data
   */
  val courseHierarchySchema: StructType = StructType(Seq(
    StructField("identifier", StringType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("channel", StringType, nullable = true)
  ))

  /**
   * Fetch course-content data from cassandra hierarchy table
   *
   * @param spark
   * @param config
   * @return Dataframe - course_id, hierarchy, user_id, content_id, attempt_id, total_max_score, total_score
   */
//  def hierarchyDataDf(assessmentData: DataFrame)(implicit spark: SparkSession, config: JobConfig): DataFrame = {
//    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]())
//    var df = cassandraTableAsDataFrame("dev_hierarchy_store", "content_hierarchy")
//      .select(col("identifier").alias("course_id"), col("hierarchy"))
//    df = df.filter(col("hierarchy").isNotNull)
//    println("before join - dev-hierarchy")
//    df = df.join(assessmentData, Seq("course_id"), "inner")
//    show(df, "Course - content data from Cassandra")
//    df.show()
//    df
//  }

  /**
   * Fetch course data from cassandra hierarchy table and join with organisation related data
   *
   * @param spark
   * @param config
   * @return Dataframe - course_id, course_name, course_org_id, course_org_name, course_org_status
   */

//  def courseDataDbDf(assessmentData: DataFrame)(implicit spark: SparkSession, config: JobConfig): DataFrame = {
//    var df = hierarchyDataDf(assessmentData)
//    df = df.withColumn("data", from_json(col("hierarchy"), courseHierarchySchema))
//    df = df.select(
//      col("course_id"),
//      col("data.name").alias("course_name"),
//      col("data.channel").alias("rootorgid")
//    )
//    val orgData = organisationDataDbDf()
//    df = df.join(orgData, Seq("rootorgid"), "inner")
//    df = df.select(
//      col("course_id"), col("course_name"), col("rootorgid").alias("course_org_id"),
//      col("org_name").alias("course_org_name"), col("org_status")
//    )
//    show(df, "Course related data from Cassandra")
//    df.show()
//    df
//  }

  /**
   * Schema for content data
   */
  val contentHierarchySchema = new StructType()
    .add("identifier", StringType)
    .add("children", ArrayType(new StructType()
      .add("identifier", StringType)
      .add("name", StringType)
    ))

  /**
   * Fetch content related data
   *
   * @param spark
   * @param config
   * @return Dataframe - content_id, content_name
   */

  def contentDataDbDf(assessmentData: DataFrame)(implicit spark: SparkSession, config: JobConfig): DataFrame = {
    var df = hierarchyDataDf(assessmentData)
    df = df.withColumn("content", from_json(col("hierarchy"), contentHierarchySchema))

    df = df.select("content.*", "course_id")
    df = df.select(explode(df.col("children")))
    df = df.select(df("col.identifier").alias("content_id"), df("col.name").alias("content_name"))
    df = df.distinct()
    show(df, "Content related data from cassandra")
    df.show()
    df
  }

  /**
   * Fetch user related data from Cassandra User table and joining with organisation related dataframe
   *
   * @param spark
   * @param config
   * @return Dataframe - user_id, firstname, lastname, user_status, user_org_id, user_org_name, user_org_status
   */
  def userDataDbDf()(implicit spark: SparkSession, config: JobConfig): DataFrame = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]())
    var df = cassandraTableAsDataFrame("sunbird", "user")
      .select(col("userid").alias("user_id"), col("firstname"), col("lastname"),
        col("status").alias("user_status"), col("rootorgid"), col("maskedemail"), col("maskedphone"))
    val orgData = organisationDataDbDf()
    df = df.join(orgData, Seq("rootorgid"), "inner")
    df = df.withColumn("full_name", functions.concat(col("firstname"), lit(' '), col("lastname")))
    df = df.drop("firstname", "lastname")
    df = df.select(
      col("user_id"), col("full_name"),
      col("rootorgid").alias("user_org_id"), col("org_name").alias("user_org_name"), col("org_status").alias("user_org_status"), col("maskedphone").alias("mobile_no"),
      col("maskedemail").alias("email"), col("user_status")
    )
    show(df, "User related data from Cassandra")
    df.show()
    df
  }

  /**
   * Fetch organisation related data from cassandra organisation table
   *
   * @param spark
   * @param config
   * @return Dataframe - rootorgid, org_name, org_status
   */

  def organisationDataDbDf()(implicit spark: SparkSession, config: JobConfig): DataFrame = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]())
    var df = cassandraTableAsDataFrame("sunbird", "organisation")
      .select(col("rootorgid"), col("orgname").alias("org_name"), col("status").alias("org_status"))
    df
  }


}
