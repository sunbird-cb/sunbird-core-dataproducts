package org.ekstep.analytics.dashboard.report.warehouse

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput, Redis}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}

import java.io.Serializable

object DataWarehouseModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.warehouse.DataWarehouseModel"
  override def name() = "DataWarehouseModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(data: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = data.first().timestamp  // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()


    syncReportsToPostgres(timestamp, config)
    sc.parallelize(Seq())  // return empty rdd
  }

  override def postProcess(data: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq())  // return empty rdd
  }


  /**
   * Reading all the reports and saving it to postgres. Overwriting the data in postgres
   *
   * @param timestamp unique timestamp from the start of the processing
   * @param config model config, should be defined at sunbird-data-pipeline:ansible/roles/data-products-deploy/templates/model-config.j2
   */
  def syncReportsToPostgres(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    implicit val conf: DashboardConfig = parseConfig(config)
    val today = getDate()
    val dwPostgresUrl = s"jdbc:postgresql://${conf.dwPostgresHost}/${conf.dwPostgresSchema}"
    val appPostgresUrl = s"jdbc:postgresql://${conf.appPostgresHost}/${conf.appPostgresSchema}"


    val user_details = spark.read.option("header", "true")
      .csv(s"/tmp/${conf.userReportPath}/${today}-warehouse")
    saveDataframeToPostgresTable(user_details, dwPostgresUrl, conf.dwUserTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val cbp_details = spark.read.option("header", "true")
      .csv(s"/tmp/${conf.cbaReportPath}/${today}-warehouse")
    saveDataframeToPostgresTable(cbp_details, dwPostgresUrl, conf.dwCourseTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val enrollment_details = spark.read.option("header", "true")
      .csv(s"/tmp/${conf.userEnrolmentReportPath}/${today}-warehouse")
    saveDataframeToPostgresTable(enrollment_details, dwPostgresUrl, conf.dwEnrollmentsTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val assessment_details = spark.read.option("header", "true")
      .csv(s"/tmp/${conf.cbaReportPath}/${today}-warehouse")
    saveDataframeToPostgresTable(assessment_details, dwPostgresUrl, conf.dwAssessmentTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val blended_details = spark.read.option("header", "true")
      .csv(s"/tmp/${conf.blendedReportPath}/${today}-warehouse")
    saveDataframeToPostgresTable(blended_details, dwPostgresUrl, conf.dwBPEnrollmentsTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    //Read the org hierarchy from postgres and then save it the Datawarehouse postgres
    val orgDf = postgresTableAsDataFrame(appPostgresUrl, conf.appOrgHierarchyTable, conf.appPostgresUsername, conf.appPostgresCredential)

    val orgCassandraDF = orgDataFrame().select(col("orgID").alias("sborgid"), col("orgType"))
    val orgDfWithOrgType = orgDf.join(orgCassandraDF, Seq("sborgid"), "inner")

    var orgDwDf = orgDfWithOrgType.select(
        col("sborgid").alias("mdo_id"),
        col("orgname").alias("mdo_name"),
        col("l1orgname").alias("ministry"),
        col("l2orgname").alias("department"),
        col("orgType")
      )
      .withColumn("is_cbp_provider",
        when(col("orgType").cast("int")  === 128 || col("orgType").cast("int")  === 128, lit("Y")).otherwise(lit("N")))
      .withColumn("organization", when(col("ministry").isNotNull && col("department").isNotNull, col("mdo_name")).otherwise(null))
      .withColumn("data_last_generated_on", date_format(current_timestamp(), "dd/MM/yyyy HH:mm:ss a"))

    orgDwDf= orgDwDf.drop("orgType")
    saveDataframeToPostgresTable(orgDwDf, dwPostgresUrl, conf.dwOrgTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    spark.stop()

  }

}
