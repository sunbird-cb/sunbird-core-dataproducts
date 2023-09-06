package org.ekstep.analytics.dashboard.report.course

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{coalesce, col, countDistinct, expr, from_unixtime, lit, round}
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate, StorageConfig}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.StorageUtil._

object CourseReportModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {
  implicit val className: String = "org.ekstep.analytics.dashboard.report.course.CourseReportModel"
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

    val reportPath = s"/tmp/standalone-reports/course-report/${getDate}/"

    val userDataDF = userProfileDetailsDF().withColumn("Full Name", functions.concat(coalesce(col("firstName"), lit("")), lit(' '),
      coalesce(col("lastName"), lit(""))))
    val userEnrolmentDF = userCourseProgramCompletionDataFrame()
    val org = orgDataFrame();
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF, allCourseProgramDetailsWithRatingDF) =
      contentDataFrames(org, false, false)

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

    // number of enrollments
    val courseEnrolled = courseCompletionWithDetailsDFforMDO.where(expr("completionStatus in ('enrolled', 'started', 'in-progress', 'completed')"))
    val courseEnrolledCount = courseEnrolled.groupBy("courseID").agg(
      countDistinct("userID").alias("enrollmentCount"))

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

    df = df.withColumn("notStartedCount", df("enrollmentCount") - df("startedCount") - df("inProgressCount") - df("completedCount"))
    df = df.withColumn("durationInHour", round(col("courseDuration")/3600, 2))
    df = df.withColumn("Average_Rating", round(col("ratingAverage"), 2))

    df = df.dropDuplicates("courseID", "userID").select(
      col("courseOrgName").alias("CBP_Provider"),
      col("courseName").alias("CBP_Name"),
      col("category").alias("CBP_Type"),
      from_unixtime(col("courseLastPublishedOn").cast("long"),"dd/MM/yyyy").alias("Published_Date"),
      col("durationInHour").alias("CBP_Duration"),
      col("enrollmentCount").alias("Enrolled"),
      col("notStartedCount").alias("Not_Started"),
      col("inProgressCount").alias("In_Progress"),
      col("completedCount").alias("Completed"),
      col("Average_Rating"),
      col("userOrgID").alias("mdoid")
//      col("courseOrgID").alias("mdoid")
    )

    df.repartition(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").partitionBy("mdoid")
      .save(reportPath)

    import spark.implicits._
    val ids = df.select("mdoid").map(row => row.getString(0)).collect().toArray

    removeFile(reportPath + "_SUCCESS")
    renameCSV(ids, reportPath)

    val storageConfig = new StorageConfig(conf.store, conf.container,reportPath)

    val storageService = getStorageService(conf)
    storageService.upload(storageConfig.container, reportPath,
      s"standalone-reports/course-report/${getDate}/", Some(true), Some(0), Some(3), None);

    closeRedisConnect()
  }
}
