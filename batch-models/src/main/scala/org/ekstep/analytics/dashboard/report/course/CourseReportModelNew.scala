package org.ekstep.analytics.dashboard.report.course

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtilNew._
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}

object CourseReportModelNew extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {
  implicit val className: String = "org.ekstep.analytics.dashboard.report.course.CourseReportModelNew"
  implicit var debug: Boolean = false
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
    val reportPath = s"/tmp/${conf.courseReportPath}/${today}/"

    val orgDF = orgDataFrame()
    val orgHierarchyData = orgHierarchyDataframe()
//    //GET ORG DATW
//    var userDataDF = userProfileDetailsDF(orgDF).withColumn("fullName", col("firstName"))
//      .withColumnRenamed("orgName", "userOrgName")
//      .withColumnRenamed("orgCreatedDate", "userOrgCreatedDate")
//    userDataDF = userDataDF
//      .join(orgHierarchyData, Seq("userOrgName"), "left")
//    show(userDataDF, "userDataDF")

    //Get course data first
    val allCourseProgramDetailsDF = contentDataFrames(false, true)
    val allCourseProgramDetailsDFWithOrgName = allCourseProgramDetailsDF
      .join(orgDF, allCourseProgramDetailsDF.col("courseActualOrgId").equalTo(orgDF.col("orgID")), "left")
      .withColumnRenamed("orgName", "courseOrgName")
    show(allCourseProgramDetailsDFWithOrgName, "allCourseProgramDetailsDFWithOrgName")

    val courseResCountDF = allCourseProgramDetailsDF.select("courseID", "courseResourceCount")

    val userEnrolmentDF = userCourseProgramCompletionDataFrame().join(courseResCountDF, Seq("courseID"), "left")

    show(userEnrolmentDF, "userEnrolmentDF")

    //val userRatingDF = userCourseRatingDataframe()

    val allCourseProgramCompletionWithDetailsDF = calculateCourseProgress(userEnrolmentDF)
    show(allCourseProgramCompletionWithDetailsDF, "allCourseProgramCompletionWithDetailsDF")

    val minMaxCompletionDF = allCourseProgramCompletionWithDetailsDF.groupBy("courseID")
      .agg(
        min("courseEnrolledTimestamp").alias("earliestCourseEnrollment"),
        max("courseEnrolledTimestamp").alias("latestCourseEnrollment")
      )
      .withColumn("firstCompletedOn", to_date(col("earliestCourseEnrollment"), "dd/MM/yyyy"))
      .withColumn("lastCompletedOn", to_date(col("latestCourseEnrollment"), "dd/MM/yyyy"))

    val countOfCertsDF = allCourseProgramCompletionWithDetailsDF.groupBy("courseID")
      .agg(
        count(when(col("issuedCertificates").isNotNull, 1)).alias("totalCetificatesIssued")
      )

    val enrolledUserCount = allCourseProgramCompletionWithDetailsDF.groupBy("courseID")
      .agg(
        count("*").alias("enrolledUserCount")
      )

    val courseProgressCountsDF = allCourseProgramCompletionWithDetailsDF.groupBy("courseID")
      .agg(
        sum(when(col("userCourseCompletionStatus") === "in-progress", 1).otherwise(0)).alias("inProgressCount"),
        sum(when(col("userCourseCompletionStatus") === "not-started", 1).otherwise(0)).alias("notStartedCount"),
        sum(when(col("userCourseCompletionStatus") === "completed", 1).otherwise(0)).alias("completedCount")
      )

    val curatedCourseDataDF = minMaxCompletionDF.join(countOfCertsDF, Seq("courseID"), "inner")
                              .join(enrolledUserCount, Seq("courseID"), "inner")
                              .join(courseProgressCountsDF, Seq("courseID"), "inner")
                              .join(allCourseProgramDetailsDFWithOrgName, Seq("courseID"), "left")


    val courseBatchDF = courseBatchDataFrame()
    val relevantBatchInfoDF = allCourseProgramDetailsDF.select("courseID", "category")
      .where(expr("category IN ('Blended Program')"))
      .join(courseBatchDF, Seq("courseID"), "left")
      .select("courseID", "batchID", "courseBatchName", "courseBatchStartDate", "courseBatchEndDate")
    show(relevantBatchInfoDF, "relevantBatchInfoDF")

    val curatedCourseDataDFWithBatchInfo = curatedCourseDataDF.join(relevantBatchInfoDF, Seq("courseID"), "left")

    val finalDf = curatedCourseDataDFWithBatchInfo
      .withColumn("courseLastPublishedOn", to_date(col("courseLastPublishedOn"), "dd/MM/yyyy"))
      .select(
          col("courseName").alias("CBP_Name"),
          col("category").alias("CBP_Type"),
          col("courseOrgName").alias("CBP_Provider"),
          col("courseActualOrgId"),
          col("courseLastPublishedOn").alias("LastPublishedOn"),
          col("courseDuration").alias("CBP_Duration"),
          col("batchID").alias("BatchID"),
          col("courseBatchName").alias("Batch_Name"),
          col("courseBatchStartDate").alias("Batch_Start_Date"),
          col("courseBatchEndDate").alias("Batch_End_Date"),
          col("firstCompletedOn").alias("First_Completed_On"),
          col("lastCompletedOn").alias("Last_Completed_On"),
          col("totalCetificatesIssued").alias("Total_Certificates_Issued"),
          col("enrolledUserCount").alias("Enrolled"),
          col("notStartedCount").alias("Not_Started"),
          col("inProgressCount").alias("In_Progress"),
          col("completedCount").alias("Completed")
        )

    csvWrite(finalDf, s"${reportPath}-${System.currentTimeMillis()}-full")

//    uploadReports(df, "mdoid", reportPath, s"${conf.courseReportPath}/${today}/")
//
    closeRedisConnect()
  }
}
