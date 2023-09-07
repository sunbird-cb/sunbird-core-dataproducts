package org.ekstep.analytics.dashboard.report.enrollment

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.StorageUtil._
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate, StorageConfig}

import java.io.Serializable

object UserEnrollmentModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable{

  implicit val className: String = "org.ekstep.analytics.dashboard.report.enrollment.UserEnrollmentModel"

  override def name() = "UserEnrollmentModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(data: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = data.first().timestamp // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processUserEnrollmentData(timestamp, config)
    sc.parallelize(Seq()) // return empty rdd
  }

  override def postProcess(data: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq()) // return empty rdd
  }


  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   * @param config    model config, should be defined at sunbird-data-pipeline:ansible/roles/data-products-deploy/templates/model-config.j2
   */
  def processUserEnrollmentData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config

    val today = getDate()
    val reportPath = s"${conf.userEnrolmentReportTempPath}/${today}/"

    val userDataDF = userProfileDetailsDF().withColumn("Full Name", functions.concat(coalesce(col("firstName"), lit("")), lit(' '),
      coalesce(col("lastName"), lit(""))))
    val userEnrolmentDF = userCourseProgramCompletionDataFrame()
    val org = orgDataFrame()
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF, allCourseProgramDetailsWithRatingDF)=
      contentDataFrames(org, false, false)

    val allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userEnrolmentDF, allCourseProgramDetailsDF, userOrgDF)
      .select(col("courseID"), col("userID"), col("completionPercentage"))

    // get the mdoids for which the report are requesting
    val mdoID = conf.mdoIDs
    val mdoIDDF = mdoIDsDF(mdoID)
    val mdoData = mdoIDDF.join(orgDF, Seq("orgID"), "inner").select(col("orgID").alias("userOrgID"), col("orgName"))

    val userRating = userCourseRatingDataframe()
    val allCourseData = allCourseProgramDetailsWithRatingDF.join(userEnrolmentDF, Seq("courseID"), "inner")

    val orgHierarchyData = orgHierarchyDataframe()
    var df = allCourseData.join(userDataDF, Seq("userID"), "inner").join(mdoData, Seq("userOrgID"), "inner")
      .join(allCourseProgramCompletionWithDetailsDF, Seq("courseID", "userID"), "inner")
      .join(userRating, Seq("courseID", "userID"), "left").join(orgHierarchyData, Seq("userOrgName"),"left")
//      .join(userCourseRatingDataframe, )

    df = df.withColumn("courseCompletionPercentage", round(col("completionPercentage"), 2))

    df = userCourseCompletionStatus(df)

    df = df.withColumn("CBP_Duration", format_string("%02d:%02d:%02d", expr("courseDuration / 3600").cast("int"),
      expr("courseDuration % 3600 / 60").cast("int"),
      expr("courseDuration % 60").cast("int")
    ))

    val caseExpression = "CASE WHEN userCourseCompletionStatus == 'completed' THEN 100 " +
      "WHEN userCourseCompletionStatus == 'not-started' THEN 0 ELSE courseCompletionPercentage END"
    df = df.withColumn("Completion Percentage", expr(caseExpression))

    df.show()
    df = df.distinct().dropDuplicates("userID", "courseID").select(
      col("Full Name").alias("Full_Name"),
      col("professionalDetails.designation").alias("Designation"),
      col("maskedEmail").alias("Email"),
      col("maskedPhone").alias("Phone_Number"),
      col("professionalDetails.group").alias("Group"),
      col("additionalProperties.tag").alias("Tags"),
      col("ministry_name").alias("Ministry"),
      col("dept_name").alias("Department"),
      col("userOrgName").alias("Organization"),
      col("courseOrgName").alias("CBP Provider"),
      col("courseName").alias("CBP Name"),
      col("category").alias("CBP Type"),
      col("CBP_Duration"),
      from_unixtime(col("courseLastPublishedOn").cast("long"),"dd/MM/yyyy").alias("Last_Published_On"),
      col("userCourseCompletionStatus").alias("Status"),
      col("courseCompletionPercentage").alias("Completion_Percentage"),
      from_unixtime(col("courseCompletedTimestamp"),"dd/MM/yyyy").alias("Completed_On"),
      col("userRating").alias("Rating"),
      col("personalDetails.gender").alias("Gender"),
      col("personalDetails.category").alias("Category"),
      col("additionalProperties.externalSystem").alias("External System"),
      col("additionalProperties.externalSystemId").alias("External System Id"),
      col("userOrgID").alias("mdoid")
    )

    df.repartition(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").partitionBy("mdoid")
      .save(reportPath)

    import spark.implicits._
    val ids = df.select("mdoid").map(row => row.getString(0)).collect().toArray

    // remove _SUCCESS file
    removeFile(reportPath + "_SUCCESS")

    // rename csv
    renameCSV(ids, reportPath)

    //upload files - s3://{container}/standalone-reports/user-enrollment-report/{date}/mdoid={mdoid}/{mdoid}.csv
    val storageConfig = new StorageConfig(conf.store, conf.container, reportPath)
    val storageService = getStorageService(conf)

    storageService.upload(storageConfig.container, reportPath,
      s"${conf.userEnrolmentReportPath}/${today}/", Some(true), Some(0), Some(3), None)

    closeRedisConnect()
  }
}
