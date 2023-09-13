package org.ekstep.analytics.dashboard.verify

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.framework._

import java.io.Serializable


object ValidateDashboardModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.verify.ValidateDashboardModel"
  override def name() = "ValidateDashboardModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(data: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = data.first().timestamp  // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processData(timestamp, config)
    sc.parallelize(Seq())  // return empty rdd
  }

  override def postProcess(data: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq())  // return empty rdd
  }

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   * @param config model config, should be defined at sunbird-data-pipeline:ansible/roles/data-products-deploy/templates/model-config.j2
   */
  def processData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config

    // get user org data frames, no filtering is done here
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    val roleDF = roleDataFrame()
    val userOrgRoleDF = userOrgRoleDataFrame(userOrgDF, roleDF)

    // get all rows from user_enrolments table, only filtering out active=false
    val userCourseProgramCompletionDF = userCourseProgramCompletionDataFrame()

    // get the content hierarchy table
    val hierarchyDF = contentHierarchyDataFrame()

    // get only the ids in enrolments table to filter the hierarchy table
    val progressCourseIDs = userCourseProgramCompletionDF.select(col("courseID")).distinct()

    // get filtered hierarchy
    val filteredHierarchyDF = addHierarchyColumn(progressCourseIDs, hierarchyDF, "courseID", "data")

    // prepare org df for clean joining
    val courseJoinOrgDF = orgDF.select(
      col("orgID").alias("courseOrgID"),
      col("orgName").alias("courseOrgName"),
      col("orgStatus").alias("courseOrgStatus")
    )

    val userCourseProgramCompletionWithDetailsDF = userCourseProgramCompletionDF
      .join(filteredHierarchyDF, Seq("courseID"), "left")
      .withColumn("courseOrgID", col("data.channel"))
      .withColumn("courseName", col("data.name"))
      .withColumn("courseStatus", col("data.status"))
      .withColumn("courseDuration", col("data.duration").cast(FloatType))
      .withColumn("category", col("data.primaryCategory"))
      .withColumn("courseReviewStatus", col("data.reviewStatus"))
      .join(userOrgDF, Seq("userID"), "left")
      .join(courseJoinOrgDF, Seq("courseOrgID"), "left")

    // filter
    val filteredProgressDF = userCourseProgramCompletionWithDetailsDF
      .where(expr("category='Course' AND courseStatus IN ('Live', 'Retired') AND userStatus=1"))

    println(s"Before join: ${userCourseProgramCompletionDF.count()}")
    println(s"Before filter: ${userCourseProgramCompletionWithDetailsDF.count()}")
    println(s"After filter: ${filteredProgressDF.count()}")

    // course df with counts
    val courseProgressCountsDF = filteredProgressDF
      .groupBy("courseID", "courseName", "courseOrgID", "courseOrgName")
      .agg(
        expr("COUNT(userID)").alias("enrolled_count"),
        expr("SUM(CASE WHEN dbCompletionStatus=0 THEN 1 ELSE 0 END)").alias("not_started_count"),
        expr("SUM(CASE WHEN dbCompletionStatus=1 THEN 1 ELSE 0 END)").alias("in_progress_count"),
        expr("SUM(CASE WHEN dbCompletionStatus=2 THEN 1 ELSE 0 END)").alias("completed_count")
      )

    val mdoOnboardedDF = userOrgRoleDF
      .where(expr("userStatus=1 AND userOrgStatus=1 AND role='MDO_ADMIN'"))
      .agg(expr("COUNT(DISTINCT userOrgID)").as("mdoOnboarded"))
    mdoOnboardedDF.show()


    val top5CoursesByCompletionDF = courseProgressCountsDF
      .orderBy(col("completed_count").desc).limit(5)
    show(top5CoursesByCompletionDF, "top5CoursesByCompletionDF")
    top5CoursesByCompletionDF.show()

    val top5MDOsByCompletionDF = filteredProgressDF
      .where(expr("userOrgStatus=1 AND dbCompletionStatus=2"))
      .groupBy("userOrgID", "userOrgName")
      .agg(expr("COUNT(userID)").alias("completed_count"))
      .orderBy(col("completed_count").desc).limit(5)
    show(top5MDOsByCompletionDF, "top5MDOsByCompletionDF")
    top5MDOsByCompletionDF.show()

    val enrollmentByStatusDF = filteredProgressDF
      .agg(
        expr("COUNT(userID)").alias("enrolled_count"),
        expr("SUM(CASE WHEN dbCompletionStatus=0 THEN 1 ELSE 0 END)").alias("not_started_count"),
        expr("SUM(CASE WHEN dbCompletionStatus=1 THEN 1 ELSE 0 END)").alias("in_progress_count"),
        expr("SUM(CASE WHEN dbCompletionStatus=2 THEN 1 ELSE 0 END)").alias("completed_count")
      )
    show(enrollmentByStatusDF, "enrollmentByStatusDF")
    enrollmentByStatusDF.show()

    val uniqueUsersEnrolledDF = filteredProgressDF
      .agg(
        expr("COUNT(DISTINCT(userID))").alias("unique_users_enrolled")
      )
    show(uniqueUsersEnrolledDF, "uniqueUsersEnrolledDF")
    uniqueUsersEnrolledDF.show()

    val uniqueUsersCompletedDF = filteredProgressDF
      .where(expr("dbCompletionStatus=2"))
      .agg(
        expr("COUNT(DISTINCT(userID))").alias("unique_users_completed")
      )
    show(uniqueUsersCompletedDF, "uniqueUsersCompletedDF")
    uniqueUsersCompletedDF.show()

    closeRedisConnect()

  }

}