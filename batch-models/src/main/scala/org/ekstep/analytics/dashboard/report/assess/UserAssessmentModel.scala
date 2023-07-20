package org.ekstep.analytics.dashboard.report.assess

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.framework.{IBatchModelTemplate, _}

import java.io.Serializable


object UserAssessmentModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.assess.UserAssessmentModel"
  override def name() = "UserAssessmentModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(data: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = data.first().timestamp  // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processUserAssessmentData(timestamp, config)
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
  def processUserAssessmentData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config

    val userAssessmentDF = userAssessmentDataFrame()

    // do course de-norm
    // do user de-norm
    // kafka dispatch to dashboard.assess

    // explode children info also
    // kafka dispatch to dashboard.assess.content

    // filter what is needed in the report
    // write report to blob store

    // obtain and save user org data
    val orgDF = orgDataFrame()
    var userDF = userDataFrame()
    var userOrgDF = userOrgDataFrame(orgDF, userDF)
    // validate userDF and userOrgDF counts
    validate({userDF.count()}, {userOrgDF.count()}, "userDF.count() should equal userOrgDF.count()")

    userDF = userDF.drop("userCreatedTimestamp", "userUpdatedTimestamp")
    userOrgDF = userOrgDF.drop("userCreatedTimestamp", "userUpdatedTimestamp")

    // org user count
    val orgUserCountDF = orgUserCountDataFrame(orgDF, userDF)
    // validate activeOrgCount and orgUserCountDF count
    validate({orgUserCountDF.count()},
      {userOrgDF.filter(expr("userStatus=1 AND userOrgID IS NOT NULL AND userOrgStatus=1")).select("userOrgID").distinct().count()},
      "orgUserCountDF.count() should equal distinct active org count in userOrgDF")

    // get course details, attach rating info, dispatch to kafka to be ingested by druid data-source: dashboards-courses
    val allCourseProgramESDF = allCourseProgramESDataFrame(Seq("Course", "Program", "CuratedCollections"))
    val allCourseProgramDF = allCourseProgramDataFrame(allCourseProgramESDF, orgDF)
    val allCourseProgramDetailsWithCompDF = allCourseProgramDetailsWithCompetenciesJsonDataFrame(allCourseProgramDF)
    val allCourseProgramDetailsDF = allCourseProgramDetailsDataFrame(allCourseProgramDetailsWithCompDF)
    val courseRatingDF = courseRatingSummaryDataFrame()
    val allCourseProgramDetailsWithRatingDF = allCourseProgramDetailsWithRatingDataFrame(allCourseProgramDetailsDF, courseRatingDF)
    // validate that no rows are getting dropped b/w allCourseProgramESDF and allCourseProgramDetailsWithRatingDF
    validate({allCourseProgramESDF.count()}, {allCourseProgramDetailsWithRatingDF.count()}, "ES course count should equal final DF with rating count")
    // validate that # of rows with ratingSum > 0 in the final DF is equal to # of rows in courseRatingDF from cassandra
    validate(
      {courseRatingDF.where(expr("categoryLower IN ('course', 'program') AND ratingSum > 0")).count()},
      {allCourseProgramDetailsWithRatingDF.where(expr("LOWER(category) IN ('course', 'program') AND ratingSum > 0")).count()},
      "number of ratings in cassandra table for courses and programs with ratingSum > 0 should equal those in final druid datasource")
    // validate rating data, sanity check
    Seq(1, 2, 3, 4, 5).foreach(i => {
      validate(
        {courseRatingDF.where(expr(s"categoryLower IN ('course', 'program') AND ratingAverage <= ${i}")).count()},
        {allCourseProgramDetailsWithRatingDF.where(expr(s"LOWER(category) IN ('course', 'program') AND ratingAverage <= ${i}")).count()},
        s"Rating data row count for courses and programs should equal final DF for ratingAverage <= ${i}"
      )
    })
    kafkaDispatch(withTimestamp(allCourseProgramDetailsWithRatingDF, timestamp), conf.allCourseTopic)

    // get course competency mapping data, dispatch to kafka to be ingested by druid data-source: dashboards-course-competency
    val allCourseProgramCompetencyDF = allCourseProgramCompetencyDataFrame(allCourseProgramDetailsWithCompDF)
    kafkaDispatch(withTimestamp(allCourseProgramCompetencyDF, timestamp), conf.courseCompetencyTopic)

    // get course completion data, dispatch to kafka to be ingested by druid data-source: dashboards-user-course-program-progress
    val userCourseProgramCompletionDF = userCourseProgramCompletionDataFrame()
    val allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userCourseProgramCompletionDF, allCourseProgramDetailsDF, userOrgDF)
    validate({userCourseProgramCompletionDF.count()}, {allCourseProgramCompletionWithDetailsDF.count()}, "userCourseProgramCompletionDF.count() should equal final course progress DF count")
    kafkaDispatch(withTimestamp(allCourseProgramCompletionWithDetailsDF, timestamp), conf.userCourseProgramProgressTopic)

    val liveCourseCompetencyDF = liveCourseCompetencyDataFrame(allCourseProgramCompetencyDF)

    closeRedisConnect()

  }

}