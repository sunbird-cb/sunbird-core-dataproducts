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

    val reportPath = s"/tmp/standalone-reports/user-enrollment-report/${getDate}/"

    val userDataDF = userProfileDetailsDF().withColumn("fullName", functions.concat(col("firstName"), lit(' '), col("lastName")))
    val userEnrolmentDF = userCourseProgramCompletionDataFrame()
    val org = orgDataFrame();

    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF, allCourseProgramDetailsWithRatingDF)=
      contentDataFrames(org, false, false)

    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    // get the mdoids for which the report are requesting
    val mdoID = conf.mdoIDs
    val mdoIDDF = mdoIDsDF(mdoID)
    val mdoData = mdoIDDF.join(orgDF, Seq("orgID"), "inner").select(col("orgID").alias("userOrgID"), col("orgName"))

    val allCourseData = allCourseProgramDetailsWithRatingDF.join(userEnrolmentDF, Seq("courseID"), "inner")

    var df = allCourseData.join(userDataDF, Seq("userID"), "inner").join(mdoData, Seq("userOrgID"), "inner")
    df = df.withColumn("rating", round(col("ratingAverage"), 1))

    df = df.select(
      col("fullName"),
      col("professionalDetails.designation").alias("designation"),
      col("userOrgName").alias("orgName"),
      col("additionalProperties.tag").alias("tag"),
      col("professionalDetails.group").alias("group"),
      col("additionalProperties.externalSystem"),
      col("additionalProperties.externalSystemId"),
      col("courseName"),
      col("courseDuration").alias("duration"),
      col("courseOrgName"),
      col("courseLastPublishedOn").alias("lastPublishedOn"),
      col("courseStatus").alias("status"),
      col("courseProgress").alias("completionPercentage"),
      col("courseCompletedTimestamp").alias("completedOn"),
      col("ratingAverage").alias("rating"),
      col("userOrgID").alias("mdoid")
    )
    df.show()

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
      s"standalone-reports/user-enrollment-report/${getDate}/", Some(true), Some(0), Some(3), None)

    closeRedisConnect()
  }
}
