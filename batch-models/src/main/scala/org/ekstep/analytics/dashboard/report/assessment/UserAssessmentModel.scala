package org.ekstep.analytics.dashboard.report.assessment

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions.{col, lit}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.StorageUtil
import org.ekstep.analytics.dashboard.StorageUtil.{removeFile, renameCSV}
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate, StorageConfig}

object UserAssessmentModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable{

  implicit val className: String = "org.ekstep.analytics.dashboard.report.assessment.UserAssessmentModel"
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
    processUserAssessmentReport(timestamp, config)
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

  def processUserAssessmentReport(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config

    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    val assessmentDF = userAssessmentDataFrame()

    var assessmentReportMdo = userOrgDF
    assessmentReportMdo = assessmentReportMdo.join(assessmentDF, Seq("userID"), "inner")
    assessmentReportMdo = assessmentReportMdo.withColumn("fullName", functions.concat(col("firstName"), lit(' '), col("lastName")))
    assessmentReportMdo = assessmentReportMdo.select(
      col("fullName"),
      col("assessPrimaryCategory").alias("type"),
      col("assessName").alias("assessmentName"),
      col("assessStatus").alias("assessmentStatus"),
      col("assessPassPercentage").alias("percentageScore"),
      col("maskedEmail").alias("email"),
      col("maskedPhone").alias("phone"),
      col("userOrgName").alias("provider"),
      col("userOrgID").alias("mdoid")
    )

    assessmentReportMdo.show()

    import spark.implicits._
    val ids = assessmentReportMdo.select("mdoid").map(row => row.getString(0)).collect().toArray

    assessmentReportMdo.repartition(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").partitionBy("mdoid")
      .save(s"/tmp/user-assessment-report-mdo/${getDate}/")

    // remove the _SUCCESS file
    removeFile(s"/tmp/user-assessment-report-mdo/${getDate}/_SUCCESS")

    //rename the csv file
    renameCSV(ids, s"/tmp/user-assessment-report-mdo/${getDate}/")

    // upload mdo files - s3://{container}/standalone-reports/user-assessment-report-mdo/{date}/mdoid={mdoid}/{mdoid}.csv
    val storageConfigMdo = new StorageConfig(conf.store, conf.container, s"/tmp/user-assessment-report-mdo/${getDate}")
    val storageService = StorageUtil.getStorageService(conf)

    storageService.upload(storageConfigMdo.container, s"/tmp/user-assessment-report-mdo/${getDate}/",
      s"standalone-reports/user-assessment-report-mdo/${getDate}/", Some(true), Some(0), Some(3), None)


    //ASSESSMENT REPORT FOR CBP
    var assessmentReportCbp = assessmentReportMdo.drop("provider", "type");
    assessmentReportCbp.show()

    val cbpids = assessmentReportCbp.select("mdoid").map(row => row.getString(0)).collect().toArray

    assessmentReportCbp.repartition(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").partitionBy("mdoid")
      .save(s"/tmp/user-assessment-report-cbp/${getDate}/")

    removeFile(s"/tmp/user-assessment-report-cbp/${getDate}/_SUCCESS")
    renameCSV(cbpids, s"/tmp/user-assessment-report-cbp/${getDate}/")

    // upload cbp files - s3://{container}/standalone-reports/user-assessment-report-cbp/{date}/mdoid={mdoid}/{mdoid}.csv
    val storageConfigCbp = new StorageConfig(conf.store, conf.container, s"/tmp/user-assessment-report-cbp/${getDate}")
    storageService.upload(storageConfigCbp.container, s"/tmp/user-assessment-report-cbp/${getDate}/",
      s"standalone-reports/user-assessment-report-cbp/${getDate}/", Some(true), Some(0), Some(3), None)

    closeRedisConnect()
  }
}
