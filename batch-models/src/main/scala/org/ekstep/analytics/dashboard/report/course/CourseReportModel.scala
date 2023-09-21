package org.ekstep.analytics.dashboard.report.course

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._


object CourseReportModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {
  implicit val className: String = "org.ekstep.analytics.dashboard.report.course.CourseReportModel"
  override def name() = "CourseReportModel"
  /**
   * Pre processing steps before running the algorithm. Few pre-process steps are
   * 1. Transforming input - Filter/Map etc.
   * 2. Join/fetch data from LP
   * 3. Join/Fetch data from Cassandra
   */
  override def preProcess(events: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  /**
   * Method which runs the actual algorithm
   */
  override def algorithm(events: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = events.first().timestamp // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processCourseReport(timestamp, config)
    sc.parallelize(Seq()) // return empty rdd
  }

  /**
   * Post processing on the algorithm output. Some of the post processing steps are
   * 1. Saving data to Cassandra
   * 2. Converting to "MeasuredEvent" to be able to dispatch to Kafka or any output dispatcher
   * 3. Transform into a structure that can be input to another data product
   */
  override def postProcess(events: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq())
  }

  def processCourseReport(timestamp: Long, config: Map[String, AnyRef]) (implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config

    val today = getDate()

    val userEnrolmentDF = userCourseProgramCompletionDataFrame()

    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    val userDataDF = userProfileDetailsDF(orgDF)

    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF, allCourseProgramDetailsWithRatingDF) =
      contentDataFrames(orgDF, false, false, true)
    //val courseBatchData = courseBatchDataFrame()

    val courseStatusUpdateData = courseStatusUpdateDataFrame(hierarchyDF)

    //    val userCourseProgramCompletionDF = userCourseProgramCompletionDataFrame()
    val allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userEnrolmentDF, allCourseProgramDetailsDF, userOrgDF)
      .select(col("courseID"), col("userID"), col("completionPercentage"), col("userOrgID"))
    val courseDetailsWithCompletionStatus = withCompletionStatusColumn(allCourseProgramCompletionWithDetailsDF)

    // get the mdoids for which the report are requesting
    val mdoID = conf.mdoIDs
    val mdoIDDF = mdoIDsDF(mdoID)
    val mdoData = mdoIDDF.join(orgDF, Seq("orgID"), "inner").select(col("orgID").alias("userOrgID"), col("orgName"))

    val allCourseData = allCourseProgramDetailsWithRatingDF.join(userEnrolmentDF, Seq("courseID"), "inner")

    var courseCompletionWithDetailsDFforMDO = allCourseData.join(courseDetailsWithCompletionStatus, Seq("courseID", "userID"), "inner")
      .join(mdoData, Seq("userOrgID"), "inner")
      //.join(courseBatchData, Seq("courseID"), "left")
      .join(courseStatusUpdateData, Seq("courseID"), "left")

    // number of enrolments
    val courseEnrolled = courseCompletionWithDetailsDFforMDO.where(expr("completionStatus in ('enrolled', 'started', 'in-progress', 'completed')"))
    val courseEnrolledCount = courseEnrolled.groupBy("courseID").agg(
      countDistinct("userID").alias("enrolmentCount"))

    // number of completions
    val courseCompleted = courseCompletionWithDetailsDFforMDO.where(expr("completionStatus = 'completed'"))
    val courseCompletedCount = courseCompleted.groupBy("courseID").agg(
      countDistinct("userID").alias("completedCount"))

    // number of in-progress
    val courseInProgress = courseCompletionWithDetailsDFforMDO.where(expr("completionStatus = 'in-progress'"))
    val courseInProgressCount = courseInProgress.groupBy("courseID").agg(
      countDistinct("userID").alias("inProgressCount"))

    // number of started
    val courseStarted = courseCompletionWithDetailsDFforMDO.where(expr("completionStatus = 'started'"))
    val courseStartedCount = courseStarted.groupBy("courseID").agg(
      countDistinct("userID").alias("startedCount"))

    var df = courseCompletionWithDetailsDFforMDO.join(courseEnrolledCount, Seq("courseID"), "inner")
      .join(courseInProgressCount, Seq("courseID"), "inner").join(courseCompletedCount, Seq("courseID"), "inner")
      .join(courseStartedCount, Seq("courseID"), "inner")

    df = df.withColumn("notStartedCount", df("enrolmentCount") - df("startedCount") - df("inProgressCount") - df("completedCount"))
    df = df.withColumn("Average_Rating", round(col("ratingAverage"), 2))

    df = df.withColumn("CBP_Duration", format_string("%02d:%02d:%02d", expr("courseDuration / 3600").cast("int"),
      expr("courseDuration % 3600 / 60").cast("int"),
      expr("courseDuration % 60").cast("int")
    ))

    var certificateIssued = df.filter(col("issuedCertificates") =!= "[]").select(col("courseID"), col("issuedCertificates"))
    certificateIssued = certificateIssued.groupBy(col("courseID")).agg(count(col("issuedCertificates")).alias("Total_Certificates_Issued"))

    df = df.join(certificateIssued, Seq("courseID"), "left")

//    val caseExpressionBatchID = "CASE WHEN courseBatchEnrolmentType == 'open' THEN 'Null' ELSE courseBatchID END"
//    val caseExpressionBatchName = "CASE WHEN courseBatchEnrolmentType == 'open' THEN 'Null' ELSE courseBatchName END"
//    val caseExpressionStartDate = "CASE WHEN courseBatchEnrolmentType == 'open' THEN 'Null' ELSE courseBatchStartDate END"
//    val caseExpressionEndDate = "CASE WHEN courseBatchEnrolmentType == 'open' THEN 'Null' ELSE courseBatchEndDate END"
//
//    df = df.withColumn("Batch_Start_Date", expr(caseExpressionStartDate))
//    df = df.withColumn("Batch_End_Date", expr(caseExpressionEndDate))
//    df = df.withColumn("Batch_ID", expr(caseExpressionBatchID))
//    df = df.withColumn("Batch_Name", expr(caseExpressionBatchName))

    df = df.withColumn("Batch_Start_Date", lit(""))
    df = df.withColumn("Batch_End_Date", lit(""))
    df = df.withColumn("Batch_ID", lit(""))
    df = df.withColumn("Batch_Name", lit(""))

    val completedOn = df.groupBy("courseID").agg(
      max("courseCompletedTimestamp").alias("LastCompletedOn"),
      min("courseCompletedTimestamp").alias("FirstCompletedOn")
    )

    df = df.join(completedOn, Seq("courseID"), "left")

    df = df.dropDuplicates("courseID").select(
      col("courseOrgName").alias("CBP_Provider"),
      col("courseName").alias("CBP_Name"),
      col("category").alias("CBP_Type"),
      col("Batch_ID"),
      col("Batch_Name"),
      col("Batch_Start_Date"),
      col("Batch_End_Date"),
      col("CBP_Duration"),
      col("enrolmentCount").alias("Enrolled"),
      col("notStartedCount").alias("Not_Started"),
      col("inProgressCount").alias("In_Progress"),
      col("completedCount").alias("Completed"),
      col("Average_Rating").alias("CBP_Rating"),
      from_unixtime(col("courseLastPublishedOn").cast("long"),"dd/MM/yyyy").alias("Last_Published_On"),
      from_unixtime(col("FirstCompletedOn").cast("long"),"dd/MM/yyyy").alias("First_Completed_On"),
      from_unixtime(col("LastCompletedOn").cast("long"),"dd/MM/yyyy").alias("Last_Completed_On"),
      from_unixtime(col("ArchivedOn").cast("long"),"dd/MM/yyyy").alias("CBP_Archived_On"),
      col("Total_Certificates_Issued"),
      col("userOrgID").alias("mdoid")
//      col("courseOrgID").alias("mdoid")
    )

    df = df.coalesce(1)
    val reportPath = s"${conf.courseReportPath}/${today}"
    csvWrite(df, s"/tmp/${reportPath}/full/")
    generateAndSyncReports(df, "mdoid", reportPath, "CBPReport")

    closeRedisConnect()
  }
}
