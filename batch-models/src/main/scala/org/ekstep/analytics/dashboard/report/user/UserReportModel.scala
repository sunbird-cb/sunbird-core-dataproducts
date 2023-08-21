package org.ekstep.analytics.dashboard.report.user

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate, StorageConfig}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.StorageUtil._

object UserReportModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {
  implicit val className: String = "org.ekstep.analytics.dashboard.report.user.UserReportModel"
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
    processUserReport(timestamp, config)
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

  def processUserReport(timestamp: Long, config: Map[String, AnyRef]) (implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config

    val reportPath = s"/tmp/standalone-reports/user-report/${getDate}/"

    // get user roles data
    val userRolesDF = roleDataFrame()     // return - userID, role
    val userDataDF = userProfileDetailsDF().withColumn("fullName", functions.concat(col("firstName"), lit(' '), col("lastName")))
    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

    // get the mdoids for which the report are requesting
    val mdoID = conf.mdoIDs
    val mdoIDDF = mdoIDsDF(mdoID)

    var df = mdoIDDF.join(orgDF, Seq("orgID"), "inner").select(col("orgID").alias("userOrgID"), col("orgName"))

    df = df.join(userDataDF, Seq("userOrgID"), "inner").join(userRolesDF, Seq("userID"), "inner")

    df = df.filter(df("userOrgStatus").equalTo(1)).select(
      col("fullName"),
      col("professionalDetails.designation").alias("designation"),
      col("userOrgName").alias("orgName"),
      col("userOrgID").alias("mdoid"),
      col("userUpdatedTimestamp").alias("createdDate"),
      col("additionalProperties.tag").alias("tag"),
      col("professionalDetails.group").alias("group"),
      col("role").alias("roles"),
      col("additionalProperties.externalSystemId"),
      col("additionalProperties.externalSystem")
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
      s"standalone-reports/user-report/${getDate}/", Some(true), Some(0), Some(3), None);

    closeRedisConnect()
  }
}
