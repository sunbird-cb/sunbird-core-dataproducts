package org.ekstep.analytics.dashboard.report.course_new

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtilNew._
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}

object CourseReportModelNew extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {
  implicit val className: String = "org.ekstep.analytics.dashboard.report.course_new.CourseReportModelNew"

  override def name() = "CourseReportModelNew"

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

  def processCourseReport(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config

    val today = getDate()
    val reportPath = s"/tmp/${conf.courseReportPath}/${today}/"

    val orgDF = orgDataFrame()

    //Get course data first
    val allCourseProgramDetailsDF = contentDataFrames(false, true)
    val allCourseProgramDetailsDFWithOrgName = allCourseProgramDetailsDF
      .join(orgDF, allCourseProgramDetailsDF.col("courseActualOrgId").equalTo(orgDF.col("orgID")), "left")
      .withColumnRenamed("orgName", "courseOrgName")
    show(allCourseProgramDetailsDFWithOrgName, "allCourseProgramDetailsDFWithOrgName")

    val userRatingDF = userCourseRatingDataframe().groupBy("courseID").agg(
      avg(col("userRating")).alias("rating")
    )
    val cbpDetailsDF = allCourseProgramDetailsDFWithOrgName.join(userRatingDF, Seq("courseID"), "left")
    show(cbpDetailsDF, "cbpDetailsDataFrame")

    val courseResCountDF = allCourseProgramDetailsDF.select("courseID", "courseResourceCount")
    val userEnrolmentDF = userCourseProgramCompletionDataFrame().join(courseResCountDF, Seq("courseID"), "left")
    val allCBPCompletionWithDetailsDF = calculateCourseProgress(userEnrolmentDF)
    show(allCBPCompletionWithDetailsDF, "allCBPCompletionWithDetailsDF")

    val aggregatedDF = allCBPCompletionWithDetailsDF.groupBy("courseID")
      .agg(
        min("courseCompletedTimestamp").alias("earliestCourseCompleted"),
        max("courseCompletedTimestamp").alias("latestCourseCompleted"),
        count("*").alias("enrolledUserCount"),
        sum(when(col("userCourseCompletionStatus") === "in-progress", 1).otherwise(0)).alias("inProgressCount"),
        sum(when(col("userCourseCompletionStatus") === "not-started", 1).otherwise(0)).alias("notStartedCount"),
        sum(when(col("userCourseCompletionStatus") === "completed", 1).otherwise(0)).alias("completedCount"),
        sum(col("issuedCertificateCount")).alias("totalCertificatesIssued")
      )
      .withColumn("firstCompletedOn", to_date(col("earliestCourseCompleted"), "dd/MM/yyyy"))
      .withColumn("lastCompletedOn", to_date(col("latestCourseCompleted"), "dd/MM/yyyy"))
    show(aggregatedDF, "aggregatedDF")

    val allCBPAndAggDF = cbpDetailsDF.join(aggregatedDF, Seq("courseID"), "left")
    show(allCBPAndAggDF, "allCBPAndAggDF")

    val courseBatchDF = courseBatchDataFrame()
    val relevantBatchInfoDF = allCourseProgramDetailsDF.select("courseID", "category")
      .where(expr("category IN ('Blended Program')"))
      .join(courseBatchDF, Seq("courseID"), "left")
      .select("courseID", "batchID", "courseBatchName", "courseBatchStartDate", "courseBatchEndDate")
    show(relevantBatchInfoDF, "relevantBatchInfoDF")

    val curatedCourseDataDFWithBatchInfo = allCBPAndAggDF.join(relevantBatchInfoDF, Seq("courseID"), "left")
    show(curatedCourseDataDFWithBatchInfo, "curatedCourseDataDFWithBatchInfo")

    val finalDf = curatedCourseDataDFWithBatchInfo
      .withColumn("courseLastPublishedOn", to_date(col("courseLastPublishedOn"), "dd/MM/yyyy"))
      .withColumn("courseBatchStartDate", to_date(col("courseBatchStartDate"), "dd/MM/yyyy"))
      .withColumn("courseBatchEndDate", to_date(col("courseBatchEndDate"), "dd/MM/yyyy"))
      .withColumn("Report_Last_Generated_On", date_format(current_timestamp(), "dd/MM/yyyy HH:mm:ss a"))
      .withColumn("ArchivedOn", expr("CASE WHEN courseStatus == 'Retired' THEN lastStatusChangedOn ELSE '' END"))
      .withColumn("ArchivedOn", to_date(col("ArchivedOn"), "dd/MM/yyyy"))
      .select(
        col("courseOrgName").alias("CBP_Provider"),
        col("courseName").alias("CBP_Name"),
        col("category").alias("CBP_Type"),
        col("batchID").alias("Batch_Id"),
        col("courseBatchName").alias("Batch_Name"),
        col("courseBatchStartDate").alias("Batch_Start_Date"),
        col("courseBatchEndDate").alias("Batch_End_Date"),
        col("courseDuration").alias("CBP_Duration"),
        col("enrolledUserCount").alias("Enrolled"),
        col("notStartedCount").alias("Not_Started"),
        col("inProgressCount").alias("In_Progress"),
        col("completedCount").alias("Completed"),
        col("rating").alias("CBP_Rating"),
        col("courseLastPublishedOn").alias("Last_Published_On"),
        col("firstCompletedOn").alias("First_Completed_On"),
        col("lastCompletedOn").alias("Last_Completed_On"),
        col("ArchivedOn").alias("CBP_Archived_On"),
        col("totalCertificatesIssued").alias("Total_Certificates_Issued"),
        col("courseActualOrgId").alias("mdoid"),
        col("Report_Last_Generated_On")
      )
    show(finalDf)

    // csvWrite(finalDf, s"${reportPath}-${System.currentTimeMillis()}-full")

    //finalDf.coalesce(1).write.format("csv").option("header", "true").save(s"${reportPath}-${System.currentTimeMillis()}-full")

    uploadReports(finalDf, "mdoid", reportPath, s"${conf.courseReportPath}/${today}/", "CBPReport")

    closeRedisConnect()
  }


}
