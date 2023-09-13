package org.ekstep.analytics.dashboard

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.framework._
import DashboardUtil._
import DataUtil._

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
C1.04   2,3     Bar-Graph           CBP enrolment rate (for a particular competency)
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
object CompetencyMetricsModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.CompetencyMetricsModel"
  override def name() = "CompetencyMetricsModel"

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

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   * @param config model config, should be defined at sunbird-data-pipeline:ansible/roles/data-products-deploy/templates/model-config.j2
   */
  def processCompetencyMetricsData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config

    // obtain and save user org data
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    userDF.drop("userProfileDetails")
    userOrgDF.drop("userProfileDetails")
    kafkaDispatch(withTimestamp(userOrgDF, timestamp), conf.userOrgTopic)

    // obtain and save role count data
    val roleDF = roleDataFrame()
    val userOrgRoleDF = userOrgRoleDataFrame(userOrgDF, roleDF)
    val roleCountDF = roleCountDataFrame(userOrgRoleDF)
    kafkaDispatch(withTimestamp(roleCountDF, timestamp), conf.roleUserCountTopic)

    // obtain and save org role count data
    val orgRoleCount = orgRoleCountDataFrame(userOrgRoleDF)
    kafkaDispatch(withTimestamp(orgRoleCount, timestamp), conf.orgRoleUserCountTopic)

    // org user count
    val orgUserCountDF = orgUserCountDataFrame(orgDF, userDF)
    // validate activeOrgCount and orgUserCountDF count
    validate({orgUserCountDF.count()},
      {userOrgDF.filter(expr("userStatus=1 AND userOrgID IS NOT NULL AND userOrgStatus=1")).select("userOrgID").distinct().count()},
      "orgUserCountDF.count() should equal distinct active org count in userOrgDF")

    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF,
      allCourseProgramDetailsWithRatingDF) = contentDataFrames(orgDF)

    kafkaDispatch(withTimestamp(allCourseProgramDetailsWithRatingDF, timestamp), conf.allCourseTopic)

    // get course competency mapping data, dispatch to kafka to be ingested by druid data-source: dashboards-course-competency
    val allCourseProgramCompetencyDF = allCourseProgramCompetencyDataFrame(allCourseProgramDetailsWithCompDF)
    kafkaDispatch(withTimestamp(allCourseProgramCompetencyDF, timestamp), conf.courseCompetencyTopic)

    // get course completion data, dispatch to kafka to be ingested by druid data-source: dashboards-user-course-program-progress
    val userCourseProgramCompletionDF = userCourseProgramCompletionDataFrame()
    var allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userCourseProgramCompletionDF, allCourseProgramDetailsDF, userOrgDF)
    allCourseProgramCompletionWithDetailsDF = addCourseDurationCompletedColumns(allCourseProgramCompletionWithDetailsDF, hierarchyDF)

    validate({userCourseProgramCompletionDF.count()}, {allCourseProgramCompletionWithDetailsDF.count()}, "userCourseProgramCompletionDF.count() should equal final course progress DF count")
    kafkaDispatch(withTimestamp(allCourseProgramCompletionWithDetailsDF, timestamp), conf.userCourseProgramProgressTopic)

    val liveCourseCompetencyDF = liveCourseCompetencyDataFrame(allCourseProgramCompetencyDF)

    // get user's expected competency data, dispatch to kafka to be ingested by druid data-source: dashboards-expected-user-competency
    val expectedCompetencyDF = expectedCompetencyDataFrame()
    val expectedCompetencyWithCourseCountDF = expectedCompetencyWithCourseCountDataFrame(expectedCompetencyDF, liveCourseCompetencyDF)
    validate({expectedCompetencyDF.count()}, {expectedCompetencyWithCourseCountDF.count()}, "expectedCompetencyDF.count() should equal expectedCompetencyWithCourseCountDF.count()")
    kafkaDispatch(withTimestamp(expectedCompetencyWithCourseCountDF, timestamp), conf.expectedCompetencyTopic)

    // get user's declared competency data, dispatch to kafka to be ingested by druid data-source: dashboards-declared-user-competency
    val declaredCompetencyDF = declaredCompetencyDataFrame()
    kafkaDispatch(withTimestamp(declaredCompetencyDF, timestamp), conf.declaredCompetencyTopic)

    // get frac competency data, dispatch to kafka to be ingested by druid data-source: dashboards-frac-competency
    val fracCompetencyDF = fracCompetencyDataFrame()
    val fracCompetencyWithCourseCountDF = fracCompetencyWithCourseCountDataFrame(fracCompetencyDF, liveCourseCompetencyDF)
    val fracCompetencyWithDetailsDF = fracCompetencyWithOfficerCountDataFrame(fracCompetencyWithCourseCountDF, expectedCompetencyDF, declaredCompetencyDF)
    validate({fracCompetencyDF.count()}, {fracCompetencyWithDetailsDF.count()}, "fracCompetencyDF.count() should equal fracCompetencyWithDetailsDF.count()")
    kafkaDispatch(withTimestamp(fracCompetencyWithDetailsDF, timestamp), conf.fracCompetencyTopic)

    // calculate competency gaps, add course completion status, dispatch to kafka to be ingested by druid data-source: dashboards-user-competency-gap
    val competencyGapDF = competencyGapDataFrame(expectedCompetencyDF, declaredCompetencyDF)
    val competencyGapWithCompletionDF = competencyGapCompletionDataFrame(competencyGapDF, liveCourseCompetencyDF, allCourseProgramCompletionWithDetailsDF)  // add course completion status
    validate({competencyGapDF.count()}, {competencyGapWithCompletionDF.count()}, "competencyGapDF.count() should equal competencyGapWithCompletionDF.count()")
    kafkaDispatch(withTimestamp(competencyGapWithCompletionDF, timestamp), conf.competencyGapTopic)

    val liveRetiredCourseCompletionWithDetailsDF = liveRetiredCourseCompletionWithDetailsDataFrame(allCourseProgramCompletionWithDetailsDF)

    // org user details redis dispatch
    val (orgRegisteredUserCountMap, orgTotalUserCountMap, orgNameMap) = getOrgUserMaps(orgUserCountDF)
    val activeOrgCount = orgDF.where(expr("orgStatus=1")).count()
    val activeUserCount = userDF.where(expr("userStatus=1")).count()
    redisDispatch(conf.redisRegisteredOfficerCountKey, orgRegisteredUserCountMap)
    redisDispatch(conf.redisTotalOfficerCountKey, orgTotalUserCountMap)
    redisDispatch(conf.redisOrgNameKey, orgNameMap)
    redisUpdate(conf.redisTotalRegisteredOfficerCountKey, activeUserCount.toString)
    redisUpdate(conf.redisTotalOrgCountKey, activeOrgCount.toString)

    // officer dashboard metrics redis dispatch
    // OL01 - user: expected_competency_count
    val userExpectedCompetencyCountDF = expectedCompetencyDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("count"), last("orgID").alias("orgID"))
    show(userExpectedCompetencyCountDF, "OL01")
    redisDispatchDataFrame[Long](conf.redisExpectedUserCompetencyCount, userExpectedCompetencyCountDF, "userID", "count")

    // OL02 - user: declared_competency_count
    val userDeclaredCompetencyCountDF = declaredCompetencyDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("count"))
    show(userDeclaredCompetencyCountDF, "OL02")
    redisDispatchDataFrame[Long](conf.redisDeclaredUserCompetencyCount, userDeclaredCompetencyCountDF, "userID", "count")

    // OL03 - user: (declared_competency intersection expected_competency).count / expected_competency_count
    val coveredCompetencyDF = expectedCompetencyDF.join(declaredCompetencyDF, Seq("userID", "competencyID"), "leftouter")
      .na.fill(0, Seq("declaredCompetencyLevel"))
      .where(expr("declaredCompetencyLevel >= expectedCompetencyLevel"))
    val userCoveredCompetencyCountDF = coveredCompetencyDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("coveredCount"))
    val userCompetencyCoverRateDF = userExpectedCompetencyCountDF.join(userCoveredCompetencyCountDF, Seq("userID"), "leftouter")
      .na.fill(0, Seq("coveredCount"))
      .withColumn("rate", expr("coveredCount / count"))
    show(userCompetencyCoverRateDF, "OL03")
    redisDispatchDataFrame[Double](conf.redisUserCompetencyDeclarationRate, userCompetencyCoverRateDF, "userID", "rate")

    // OL04 - mdo: average_competency_declaration_rate
    val orgCompetencyAvgCoverRateDF = userCompetencyCoverRateDF.groupBy("orgID")
      .agg(avg("rate").alias("rate"))
    show(orgCompetencyAvgCoverRateDF, "OL04")
    redisDispatchDataFrame[Double](conf.redisOrgCompetencyDeclarationRate, orgCompetencyAvgCoverRateDF, "orgID", "rate")

    // OL05 - user: competency gap count
    val userCompetencyGapDF = competencyGapDF.where(expr("competencyGap > 0"))
    val userCompetencyGapCountDF = userCompetencyGapDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("count"), last("orgID").alias("orgID"))
    show(userCompetencyGapCountDF, "OL05")
    redisDispatchDataFrame[Long](conf.redisUserCompetencyGapCount, userCompetencyGapCountDF, "userID", "count")

    // OL06 - user: enrolled cbp count (IMPORTANT: excluding completed courses)
    val userCourseEnrolledDF = liveRetiredCourseCompletionWithDetailsDF.where(expr("completionStatus in ('started', 'in-progress')"))
    val userCourseEnrolledCountDF = userCourseEnrolledDF.groupBy("userID").agg(
      countDistinct("courseID").alias("count"))
    show(userCourseEnrolledCountDF, "OL06")
    redisDispatchDataFrame[Long](conf.redisUserCourseEnrolmentCount, userCourseEnrolledCountDF, "userID", "count")

    // OL08 - user: competency gaps enrolled percentage (IMPORTANT: excluding completed ones)
    val userCompetencyGapEnrolledDF = competencyGapWithCompletionDF.where(expr("competencyGap > 0 AND completionStatus in ('started', 'in-progress')"))
    val userCompetencyGapEnrolledCountDF = userCompetencyGapEnrolledDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("enrolledCount"))
    val userCompetencyGapEnrolledRateDF = userCompetencyGapCountDF.join(userCompetencyGapEnrolledCountDF, Seq("userID"), "leftouter")
      .na.fill(0, Seq("enrolledCount"))
      .withColumn("rate", expr("enrolledCount / count"))
    show(userCompetencyGapEnrolledRateDF, "OL08")
    redisDispatchDataFrame[Double](conf.redisUserCompetencyGapEnrolmentRate, userCompetencyGapEnrolledRateDF, "userID", "rate")

    // OL09 - mdo: average competency gaps enrolled percentage
    val orgCompetencyGapAvgEnrolledRateDF = userCompetencyGapEnrolledRateDF.groupBy("orgID")
      .agg(avg("rate").alias("rate"))
    show(orgCompetencyGapAvgEnrolledRateDF, "OL09")
    redisDispatchDataFrame[Double](conf.redisOrgCompetencyGapEnrolmentRate, orgCompetencyGapAvgEnrolledRateDF, "orgID", "rate")

    // OL10 - user: completed cbp count
    val userCourseCompletedDF = liveRetiredCourseCompletionWithDetailsDF.where(expr("completionStatus = 'completed'"))
    val userCourseCompletedCountDF = userCourseCompletedDF.groupBy("userID").agg(
      countDistinct("courseID").alias("count"))
    show(userCourseCompletedCountDF, "OL10")
    redisDispatchDataFrame[Long](conf.redisUserCourseCompletionCount, userCourseCompletedCountDF, "userID", "count")

    // OL11 - user: competency gap closed count
    val userCompetencyGapClosedDF = competencyGapWithCompletionDF.where(expr("competencyGap > 0 AND completionStatus = 'completed'"))
    val userCompetencyGapClosedCountDF = userCompetencyGapClosedDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("closedCount"))
    show(userCompetencyGapClosedCountDF, "OL11")
    redisDispatchDataFrame[Long](conf.redisUserCompetencyGapClosedCount, userCompetencyGapClosedCountDF, "userID", "closedCount")

    // OL12 - user: competency gap closed percent
    val userCompetencyGapClosedRateDF = userCompetencyGapCountDF.join(userCompetencyGapClosedCountDF, Seq("userID"), "leftouter")
      .na.fill(0, Seq("closedCount"))
      .withColumn("rate", expr("closedCount / count"))
    show(userCompetencyGapClosedRateDF,  "OL12")
    redisDispatchDataFrame[Double](conf.redisUserCompetencyGapClosedCount, userCompetencyGapClosedRateDF, "userID", "rate")

    // OL13 - mdo: avg competency gap closed percent
    val orgCompetencyGapAvgClosedRateDF = userCompetencyGapClosedRateDF.groupBy("orgID")
      .agg(avg("rate").alias("rate"))
    show(orgCompetencyGapAvgClosedRateDF, "OL13")
    redisDispatchDataFrame[Double](conf.redisOrgCompetencyGapClosedRate, orgCompetencyGapAvgClosedRateDF, "orgID", "rate")

    closeRedisConnect()

  }

}