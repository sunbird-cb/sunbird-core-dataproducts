package org.ekstep.analytics.dashboard.report.blendedprogram

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, expr, lit}
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.StorageUtil._
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate, StorageConfig}

import java.io.Serializable


object bpUserAssessmentModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.assess.bpUserAssessmentModel"
  override def name() = "UserAssessmentModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(data: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = data.first().timestamp  // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processBPUserAssessmentData(timestamp, config)
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
  def processBPUserAssessmentData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    val reportName = "bpUserAssessmentReport.csv"
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config

    // obtain user org data
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

    // get course details, with rating info
    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF,
      allCourseProgramDetailsWithRatingDF) = contentDataFrames(orgDF,false,false,true)

    val assessmentDF = assessmentESDataFrame()
    val assessmentWithOrgDF = assessWithOrgDataFrame(assessmentDF, orgDF)
    val assessWithHierarchyDF = assessWithHierarchyDataFrame(assessmentWithOrgDF, hierarchyDF)
    val assessWithDetailsDF = assessWithHierarchyDF.drop("children")
    // kafka dispatch to dashboard.assessment
    kafkaDispatch(withTimestamp(assessWithDetailsDF, timestamp), conf.assessmentTopic)

    val assessChildrenDF = assessmentChildrenDataFrame(assessWithHierarchyDF)
    val userAssessmentDF = userAssessmentDataFrame()
    val userAssessChildrenDF = userAssessmentChildrenDataFrame(userAssessmentDF, assessChildrenDF)
    val userAssessChildrenDetailsDF = userAssessmentChildrenDetailsDataFrame(userAssessChildrenDF, assessWithDetailsDF,
      allCourseProgramDetailsWithRatingDF, userOrgDF)
    // kafka dispatch to dashboard.user.assessment
    kafkaDispatch(withTimestamp(userAssessChildrenDetailsDF, timestamp), conf.userAssessmentTopic)

    userAssessChildrenDetailsDF.show()

    var df = userAssessChildrenDetailsDF
    df = df.withColumn("fullName", functions.concat(col("firstName"), lit(' '), col("lastName")))

    df = df.select(
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

    var dfBlendedProgram = df.where(expr("type='BlendedProgram'"))
    import spark.implicits._
    val idsBlendedProgram = dfBlendedProgram.select("mdoid").map(row => row.getString(0)).collect().toArray

    dfBlendedProgram.repartition(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").partitionBy("mdoid")
      .save(s"/tmp/standalone-reports/blended-program-assessment-report-mdo/${getDate}/")

    // remove the _SUCCESS file
    removeFile(s"/tmp/standalone-reports/blended-program-assessment-report-mdo/${getDate}/_SUCCESS")

   //rename the blended program csv file
    renameCSV(idsBlendedProgram, s"/tmp/standalone-reports/blended-program-assessment-report-mdo/${getDate}/")()

    val storageConfigMdo1 = new StorageConfig(conf.store, conf.container, s"/tmp/standalone-reports/blended-program-assessment-report-mdo/${getDate}")
    val storageService1 = getStorageService(conf)

    storageService1.upload(storageConfigMdo1.container, s"/tmp/standalone-reports/blended-program-assessment-report-mdo/${getDate}/",
      s"standalone-reports/blended-program-assessment-report-mdo/${getDate}/", Some(true), Some(0), Some(3), None)

    val dfBlendedProgramCBP = dfBlendedProgram.drop("provider", "type")
    val cbpidsBlendedProgram = dfBlendedProgramCBP.select("mdoid").map(row => row.getString(0)).collect().toArray

    dfBlendedProgram.repartition(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").partitionBy("mdoid")
      .save(s"/tmp/standalone-reports/blended-program-assessment-report-cbp/${getDate}/")

    removeFile(s"/tmp/standalone-reports/blended-program-assessment-report-cbp/${getDate}/_SUCCESS")
    renameCSV(cbpidsBlendedProgram, s"/tmp/standalone-reports/blended-program-assessment-report-cbp/${getDate}/")()

    // upload cbp files - s3://{container}/standalone-reports/user-assessment-report-cbp/{date}/mdoid={mdoid}/{mdoid}.csv
    val storageConfigBPCbp = new StorageConfig(conf.store, conf.container, s"/tmp/standalone-reports/blended-program-assessment-report-cbp/${getDate}")
    storageService1.upload(storageConfigBPCbp.container, s"/tmp/standalone-reports/blended-program-user-assessment-report-cbp/${getDate}/",
      s"standalone-reports/blended-assessment-report-cbp/${getDate}/", Some(true), Some(0), Some(3), None)


    closeRedisConnect()

  }

}