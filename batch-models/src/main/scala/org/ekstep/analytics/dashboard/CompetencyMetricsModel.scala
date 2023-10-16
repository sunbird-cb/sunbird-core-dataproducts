package org.ekstep.analytics.dashboard

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.framework._
import DashboardUtil._
import DataUtil._

import java.io.Serializable
import java.text.SimpleDateFormat

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
    import spark.implicits._
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config

    val processingTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(timestamp)
    redisUpdate("dashboard_update_time", processingTime)

    println("Spark Config:")
    println(spark.conf.getAll)

    // obtain and save user org data
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    userDF.drop("userProfileDetails")
    userOrgDF.drop("userProfileDetails")
    // kafkaDispatch(withTimestamp(orgDF, timestamp), conf.orgTopic)
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
    val allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userCourseProgramCompletionDF, allCourseProgramDetailsDF, userOrgDF)
    validate({userCourseProgramCompletionDF.count()}, {allCourseProgramCompletionWithDetailsDF.count()}, "userCourseProgramCompletionDF.count() should equal final course progress DF count")
    kafkaDispatch(withTimestamp(allCourseProgramCompletionWithDetailsDF, timestamp), conf.userCourseProgramProgressTopic)

    // new redis updates - start
    // MDO onboarded, with atleast one MDO_ADMIN/MDO_LEADER
    val orgWithMdoAdminLeaderCount = orgRoleCount.where(expr("role IN ('MDO_ADMIN', 'MDO_LEADER') AND count > 0")).select("orgID").distinct().count()
    val orgWithMdoAdminCount = orgRoleCount.where(expr("role IN ('MDO_ADMIN') AND count > 0")).select("orgID").distinct().count()
    redisUpdate("dashboard_org_with_mdo_admin_leader_count", orgWithMdoAdminLeaderCount.toString)
    redisUpdate("dashboard_org_with_mdo_admin_count", orgWithMdoAdminCount.toString)

    // mdo-wise registered user count
    val activeUsersByMDODF = userDF.where(expr("userStatus=1")).groupBy("userOrgID").agg(count("*").alias("count"))
    redisDispatchDataFrame[Long]("dashboard_user_count_by_user_org", activeUsersByMDODF, "userOrgID", "count")

    // new users registered yesterday
    val usersRegisteredYesterdayDF = userDF
      .withColumn("yesterdayStartTimestamp", date_trunc("day", date_sub(current_timestamp(), 1)).cast("long"))
      .withColumn("todayStartTimestamp", date_trunc("day", current_timestamp()).cast("long"))
    show(usersRegisteredYesterdayDF, "usersRegisteredYesterdayDF")
    val usersRegisteredYesterdayCount = usersRegisteredYesterdayDF
      .where(expr("userCreatedTimestamp >= yesterdayStartTimestamp AND userCreatedTimestamp < todayStartTimestamp and userStatus=1"))
      .count()
    redisUpdate("dashboard_new_users_registered_yesterday", usersRegisteredYesterdayCount.toString)
    println(s"dashboard_new_users_registered_yesterday = ${usersRegisteredYesterdayCount}")

    // cbp-wise live/draft/review/retired/pending-publish course counts
    val allCourseDF = allCourseProgramDetailsWithRatingDF.where(expr("category='Course'"))
    val liveCourseDF = allCourseDF.where(expr("courseStatus='Live'"))
    val draftCourseDF = allCourseDF.where(expr("courseStatus='Draft'"))
    val reviewCourseDF = allCourseDF.where(expr("courseStatus='Review'"))
    val retiredCourseDF = allCourseDF.where(expr("courseStatus='Retired'"))
    val pendingPublishCourseDF = reviewCourseDF.where(expr("courseReviewStatus='Reviewed'"))

    val liveCourseCountByCBPDF = liveCourseDF.groupBy("courseOrgID").agg(count("*").alias("count"))
    redisDispatchDataFrame[Long]("dashboard_live_course_count_by_course_org", liveCourseCountByCBPDF, "courseOrgID", "count")
    val draftCourseCountByCBPDF = draftCourseDF.groupBy("courseOrgID").agg(count("*").alias("count"))
    redisDispatchDataFrame[Long]("dashboard_draft_course_count_by_course_org", draftCourseCountByCBPDF, "courseOrgID", "count")
    val reviewCourseCountByCBPDF = reviewCourseDF.groupBy("courseOrgID").agg(count("*").alias("count"))
    redisDispatchDataFrame[Long]("dashboard_review_course_count_by_course_org", reviewCourseCountByCBPDF, "courseOrgID", "count")
    val retiredCourseCountByCBPDF = retiredCourseDF.groupBy("courseOrgID").agg(count("*").alias("count"))
    redisDispatchDataFrame[Long]("dashboard_retired_course_count_by_course_org", retiredCourseCountByCBPDF, "courseOrgID", "count")
    val pendingPublishCourseCountByCBPDF = pendingPublishCourseDF.groupBy("courseOrgID").agg(count("*").alias("count"))
    redisDispatchDataFrame[Long]("dashboard_pending_publish_course_count_by_course_org", pendingPublishCourseCountByCBPDF, "courseOrgID", "count")

    // MDO with at least one live course
    val orgWithLiveCourseCount = liveCourseDF.select("courseOrgID").distinct().count()
    redisUpdate("dashboard_cbp_with_live_course_count", orgWithLiveCourseCount.toString)

    // Average rating across all live courses, and by CBP
    val avgRatingOverall = liveCourseDF.agg(avg("ratingAverage").alias("ratingAverage")).select("ratingAverage").first().getDouble(0)
    redisUpdate("dashboard_course_average_rating_overall", avgRatingOverall.toString)

    val avgRatingByCBPDF = liveCourseDF.groupBy("courseOrgID").agg(avg("ratingAverage").alias("ratingAverage"))
    redisDispatchDataFrame[Double]("dashboard_course_average_rating_by_course_org", avgRatingByCBPDF, "courseOrgID", "ratingAverage")

    // top 5 courses - by user rating
    //
    // SELECT courseID, courseName, category, courseOrgName, ROUND(AVG(ratingAverage), 1) AS rating_avg, SUM(ratingCount) AS rating_count
    // FROM \"dashboards-course\" WHERE __time = (SELECT MAX(__time) FROM \"dashboards-course\")
    // AND ratingCount>0 AND ratingAverage<=5.0 AND category='Course' AND courseStatus='Live'
    // GROUP BY 1, 2, 3, 4 ORDER BY rating_count * rating_avg DESC LIMIT 5

    // enrollment/not-started/started/in-progress/completion count, live and retired courses
    val liveRetiredCourseEnrolmentDF = allCourseProgramCompletionWithDetailsDF.where(expr("category='Course' AND courseStatus IN ('Live', 'Retired') AND userStatus=1"))
    // started + not-started = enrolled
    val liveRetiredCourseNotStartedDF = liveRetiredCourseEnrolmentDF.where(expr("dbCompletionStatus=0"))
    val liveRetiredCourseStartedDF = liveRetiredCourseEnrolmentDF.where(expr("dbCompletionStatus IN (1, 2)"))
    // in-progress + completed = started
    val liveRetiredCourseInProgressDF = liveRetiredCourseStartedDF.where(expr("dbCompletionStatus=1"))
    val liveRetiredCourseCompletedDF = liveRetiredCourseStartedDF.where(expr("dbCompletionStatus=2"))

    // do both count(*) and countDistinct(userID) aggregates at once
    val enrolmentCountDF = liveRetiredCourseEnrolmentDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val notStartedCountDF = liveRetiredCourseNotStartedDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val startedCountDF = liveRetiredCourseStartedDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val inProgressCountDF = liveRetiredCourseInProgressDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val completedCountDF = liveRetiredCourseEnrolmentDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))

    // unique user counts
    val enrolmentUniqueUserCount = enrolmentCountDF.select("uniqueUserCount").first().getLong(0)
    val notStartedUniqueUserCount = notStartedCountDF.select("uniqueUserCount").first().getLong(0)
    val startedUniqueUserCount = startedCountDF.select("uniqueUserCount").first().getLong(0)
    val inProgressUniqueUserCount = inProgressCountDF.select("uniqueUserCount").first().getLong(0)
    val completedUniqueUserCount = completedCountDF.select("uniqueUserCount").first().getLong(0)

    redisUpdate("dashboard_unique_users_enrolled_count", enrolmentUniqueUserCount.toString)
    redisUpdate("dashboard_unique_users_not_started_count", notStartedUniqueUserCount.toString)
    redisUpdate("dashboard_unique_users_started_count", startedUniqueUserCount.toString)
    redisUpdate("dashboard_unique_users_in_progress_count", inProgressUniqueUserCount.toString)
    redisUpdate("dashboard_unique_users_completed_count", completedUniqueUserCount.toString)
    println(s"dashboard_unique_users_enrolled_count = ${enrolmentUniqueUserCount}")
    println(s"dashboard_unique_users_not_started_count = ${notStartedUniqueUserCount}")
    println(s"dashboard_unique_users_started_count = ${startedUniqueUserCount}")
    println(s"dashboard_unique_users_in_progress_count = ${inProgressUniqueUserCount}")
    println(s"dashboard_unique_users_completed_count = ${completedUniqueUserCount}")

    // counts
    val enrolmentCount = enrolmentCountDF.select("count").first().getLong(0)
    val notStartedCount = notStartedCountDF.select("count").first().getLong(0)
    val startedCount = startedCountDF.select("count").first().getLong(0)
    val inProgressCount = inProgressCountDF.select("count").first().getLong(0)
    val completedCount = completedCountDF.select("count").first().getLong(0)

    redisUpdate("dashboard_enrolment_count", enrolmentCount.toString)
    redisUpdate("dashboard_not_started_count", notStartedCount.toString)
    redisUpdate("dashboard_started_count", startedCount.toString)
    redisUpdate("dashboard_in_progress_count", inProgressCount.toString)
    redisUpdate("dashboard_completed_count", completedCount.toString)
    println(s"dashboard_enrolment_count = ${enrolmentCount}")
    println(s"dashboard_not_started_count = ${notStartedCount}")
    println(s"dashboard_started_count = ${startedCount}")
    println(s"dashboard_in_progress_count = ${inProgressCount}")
    println(s"dashboard_completed_count = ${completedCount}")

    // mdo-wise enrollment/not-started/started/in-progress/completion counts
    val liveRetiredCourseEnrolmentByMDODF = liveRetiredCourseEnrolmentDF.groupBy("userOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    redisDispatchDataFrame[Long]("dashboard_enrolment_count_by_user_org", liveRetiredCourseEnrolmentByMDODF, "userOrgID", "count")
    redisDispatchDataFrame[Long]("dashboard_enrolment_unique_user_count_by_user_org", liveRetiredCourseEnrolmentByMDODF, "userOrgID", "uniqueUserCount")
    val liveRetiredCourseNotStartedByMDODF = liveRetiredCourseNotStartedDF.groupBy("userOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    redisDispatchDataFrame[Long]("dashboard_not_started_count_by_user_org", liveRetiredCourseNotStartedByMDODF, "userOrgID", "count")
    redisDispatchDataFrame[Long]("dashboard_not_started_unique_user_count_by_user_org", liveRetiredCourseNotStartedByMDODF, "userOrgID", "uniqueUserCount")
    val liveRetiredCourseStartedByMDODF = liveRetiredCourseStartedDF.groupBy("userOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    redisDispatchDataFrame[Long]("dashboard_started_count_by_user_org", liveRetiredCourseStartedByMDODF, "userOrgID", "count")
    redisDispatchDataFrame[Long]("dashboard_started_unique_user_count_by_user_org", liveRetiredCourseStartedByMDODF, "userOrgID", "uniqueUserCount")
    val liveRetiredCourseInProgressByMDODF = liveRetiredCourseInProgressDF.groupBy("userOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    redisDispatchDataFrame[Long]("dashboard_in_progress_count_by_user_org", liveRetiredCourseInProgressByMDODF, "userOrgID", "count")
    redisDispatchDataFrame[Long]("dashboard_in_progress_unique_user_count_by_user_org", liveRetiredCourseInProgressByMDODF, "userOrgID", "uniqueUserCount")
    val liveRetiredCourseCompletedByMDODF = liveRetiredCourseCompletedDF.groupBy("userOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    redisDispatchDataFrame[Long]("dashboard_completed_count_by_user_org", liveRetiredCourseCompletedByMDODF, "userOrgID", "count")
    redisDispatchDataFrame[Long]("dashboard_completed_unique_user_count_by_user_org", liveRetiredCourseCompletedByMDODF, "userOrgID", "uniqueUserCount")

    // cbp-wise enrollment/not-started/started/in-progress/completion counts
    val liveRetiredCourseEnrolmentByCBPDF = liveRetiredCourseEnrolmentDF.groupBy("courseOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    redisDispatchDataFrame[Long]("dashboard_enrolment_count_by_course_org", liveRetiredCourseEnrolmentByCBPDF, "courseOrgID", "count")
    redisDispatchDataFrame[Long]("dashboard_enrolment_unique_user_count_by_course_org", liveRetiredCourseEnrolmentByCBPDF, "courseOrgID", "uniqueUserCount")
    val liveRetiredCourseNotStartedByCBPDF = liveRetiredCourseNotStartedDF.groupBy("courseOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    redisDispatchDataFrame[Long]("dashboard_not_started_count_by_course_org", liveRetiredCourseNotStartedByCBPDF, "courseOrgID", "count")
    redisDispatchDataFrame[Long]("dashboard_not_started_unique_user_count_by_course_org", liveRetiredCourseNotStartedByCBPDF, "courseOrgID", "uniqueUserCount")
    val liveRetiredCourseStartedByCBPDF = liveRetiredCourseStartedDF.groupBy("courseOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    redisDispatchDataFrame[Long]("dashboard_started_count_by_course_org", liveRetiredCourseStartedByCBPDF, "courseOrgID", "count")
    redisDispatchDataFrame[Long]("dashboard_started_unique_user_count_by_course_org", liveRetiredCourseStartedByCBPDF, "courseOrgID", "uniqueUserCount")
    val liveRetiredCourseInProgressByCBPDF = liveRetiredCourseInProgressDF.groupBy("courseOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    redisDispatchDataFrame[Long]("dashboard_in_progress_count_by_course_org", liveRetiredCourseInProgressByCBPDF, "courseOrgID", "count")
    redisDispatchDataFrame[Long]("dashboard_in_progress_unique_user_count_by_course_org", liveRetiredCourseInProgressByCBPDF, "courseOrgID", "uniqueUserCount")
    val liveRetiredCourseCompletedByCBPDF = liveRetiredCourseCompletedDF.groupBy("courseOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    redisDispatchDataFrame[Long]("dashboard_completed_count_by_course_org", liveRetiredCourseCompletedByCBPDF, "courseOrgID", "count")
    redisDispatchDataFrame[Long]("dashboard_completed_unique_user_count_by_course_org", liveRetiredCourseCompletedByCBPDF, "courseOrgID", "uniqueUserCount")

    // courses enrolled/completed at-least once, only live courses
    val liveCourseEnrolmentDF = liveRetiredCourseEnrolmentDF.where(expr("courseStatus='Live'"))
    val liveCourseCompletedDF = liveRetiredCourseCompletedDF.where(expr("courseStatus='Live'"))

    val coursesEnrolledInDF = liveCourseEnrolmentDF.select("courseID").distinct()
    val coursesCompletedDF = liveCourseCompletedDF.select("courseID").distinct()

    val coursesEnrolledInIdList = coursesEnrolledInDF.map(_.getString(0)).filter(_.nonEmpty).collectAsList().toArray
    val coursesCompletedIdList = coursesCompletedDF.map(_.getString(0)).filter(_.nonEmpty).collectAsList().toArray

    val coursesEnrolledInCount = coursesEnrolledInIdList.length
    val coursesCompletedCount = coursesCompletedIdList.length

    redisUpdate("dashboard_courses_enrolled_in_at_least_once", coursesEnrolledInCount.toString)
    redisUpdate("dashboard_courses_completed_at_least_once", coursesCompletedCount.toString)
    redisUpdate("dashboard_courses_enrolled_in_at_least_once_id_list", coursesEnrolledInIdList.mkString(","))
    redisUpdate("dashboard_courses_completed_at_least_once_id_list", coursesCompletedIdList.mkString(","))
    println(s"dashboard_courses_enrolled_in_at_least_once = ${coursesEnrolledInCount}")
    println(s"dashboard_courses_completed_at_least_once = ${coursesCompletedCount}")

    // mdo-wise courses completed at-least once
    val liveCourseCompletedAtLeastOnceByMDODF = liveCourseCompletedDF.groupBy("userOrgID").agg(countDistinct("courseID").alias("count"))
    redisDispatchDataFrame[Long]("dashboard_courses_completed_at_least_once_by_user_org", liveCourseCompletedAtLeastOnceByMDODF, "userOrgID", "count")

    // new redis updates - end

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