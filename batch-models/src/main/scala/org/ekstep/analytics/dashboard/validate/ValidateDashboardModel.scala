package org.ekstep.analytics.dashboard.validate

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, countDistinct, expr, last}
import org.apache.spark.sql.types.FloatType
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.framework._

import java.io.Serializable

/*

Prerequisites(PR) -

PR01: user's expected competencies, declared competencies, and competency gaps
PR02: course competency mapping
PR03: user's course progress
PR04: course rating summary
PR05: all competencies from FRAC


Metric  PR      Type                Description

M2.08   1,2     Scorecard           Number of competencies mapped to MDO officials for which there is no CBP on iGOT
M2.11   1       Scorecard           Average number of competency gaps per officer in the MDO
M2.22   1       Scorecard           Average for MDOs: Average number of competency gaps per officer
M3.55   1       Bar-Graph           Total competency gaps in the MDO
M3.56   1,2,3   Stacked-Bar-Graph   Percentage of competency gaps for which CBPs have not been started by officers
M3.57   1,2,3   Stacked-Bar-Graph   Percentage of competency gaps for which CBPs are in progress by officers
M3.58   1,2,3   Stacked-Bar-Graph   Percentage of competency gaps for which CBPs are completed by officers

S3.13   1       Scorecard           Average competency gaps per user
S3.11   4       Leaderboard         Average user rating of CBPs
S3.14   1       Bar-Graph           Total competency gaps
S3.15   1,2,3   Stacked-Bar-Graph   Percentage of competency gaps for which CBPs have not been started by officers
S3.16   1,2,3   Stacked-Bar-Graph   Percentage of competency gaps for which CBPs are in progress by officers
S3.17   1,2,3   Stacked-Bar-Graph   Percentage of competency gaps for which CBPs are completed by officers

C1.01   5       Scorecard           Total number of CBPs on iGOT platform
C1.1    4       Scorecard           Use ratings averaged for ALL CBPs by the provider
C1.03   3       Scorecard           Number of officers who enrolled (defined as 10% completion) for the CBP in the last year
C1.04   2,3     Bar-Graph           CBP enrollment rate (for a particular competency)
C1.05   3       Scorecard           Number of officers who completed the CBP in the last year
C1.06   3       Leaderboard         CBP completion rate
C1.07   4       Leaderboard         average user ratings by enrolled officers for each CBP
C1.09   5       Scorecard           No. of CBPs mapped (by competency)

*/

/**
 * OL01 - user: expected_competency_count
 * OL02 - user: declared_competency_count
 * OL03 - user: (declared_competency intersection expected_competency).count / expected_competency_count
 * OL04 - mdo: average_competency_declaration_rate
 * OL05 - user: competency gap count
 * OL06 - user: enrolled cbp count
 * OL08 - user: competency gaps enrolled percentage
 * OL09 - mdo: average competency gaps enrolled percentage
 * OL10 - user: completed cbp count
 * OL11 - user: competency gap closed count
 * OL12 - user: competency gap closed percent
 * OL13 - mdo: avg competency gap closed percent
 */

/**
 * Model for processing competency metrics
 */
object ValidateDashboardModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.validate.ValidateDashboardModel"
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

    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

    val userCourseProgramCompletionDF = userCourseProgramCompletionDataFrame()

    val courseIDs = userCourseProgramCompletionDF.select(
      col("courseID")).distinct()

    val hierarchyDF = contentHierarchyDataFrame()

    val filteredHierarchyDF = addHierarchyColumn(courseIDs, hierarchyDF, "courseID", "data")

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

    // course df with counts
    val courseProgressCountsDF = filteredProgressDF
      .groupBy("courseID", "courseName", "courseOrgID", "courseOrgName")
      .agg(
        expr("COUNT(userID)").alias("enrolled_count"),
        expr("SUM(CASE WHEN dbCompletionStatus=0 THEN 1 ELSE 0 END)").alias("not_started_count"),
        expr("SUM(CASE WHEN dbCompletionStatus=1 THEN 1 ELSE 0 END)").alias("in_progress_count"),
        expr("SUM(CASE WHEN dbCompletionStatus=2 THEN 1 ELSE 0 END)").alias("completed_count")
      )

    val top5CoursesByCompletionDF = courseProgressCountsDF
      .orderBy(col("completed_count").desc).limit(5)
    show(top5CoursesByCompletionDF, "top5CoursesByCompletionDF")

    val top5MDOsByCompletionDF = filteredProgressDF
      .where(expr("userOrgStatus=1"))
      .groupBy("userOrgName", "userOrgStatus")
      .agg(expr("COUNT(userID)").alias("completed_count"))
      .orderBy(col("completed_count").desc).limit(5)
    show(top5MDOsByCompletionDF, "top5MDOsByCompletionDF")

    val enrollmentByStatusDF = filteredProgressDF
      .agg(
        expr("COUNT(userID)").alias("enrolled_count"),
        expr("SUM(CASE WHEN dbCompletionStatus=0 THEN 1 ELSE 0 END)").alias("not_started_count"),
        expr("SUM(CASE WHEN dbCompletionStatus=1 THEN 1 ELSE 0 END)").alias("in_progress_count"),
        expr("SUM(CASE WHEN dbCompletionStatus=2 THEN 1 ELSE 0 END)").alias("completed_count")
      )
    show(enrollmentByStatusDF, "enrollmentByStatusDF")

    val uniqueUsersEnrolledDF = filteredProgressDF
      .agg(
        expr("COUNT(DISTINCT(userID))").alias("unique_users_enrolled")
      )
    show(uniqueUsersEnrolledDF, "uniqueUsersEnrolledDF")

    val uniqueUsersCompletedDF = filteredProgressDF
      .where(expr("dbCompletionStatus=2"))
      .agg(
        expr("COUNT(DISTINCT(userID))").alias("unique_users_completed")
      )
    show(uniqueUsersCompletedDF, "uniqueUsersCompletedDF")

    closeRedisConnect()

  }

}