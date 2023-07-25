package org.ekstep.analytics.dashboard.report.user

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, explode_outer, from_json, lit}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.ekstep.analytics.dashboard.DashboardUtil.{debug, _}
import org.ekstep.analytics.dashboard.DataUtil.{getOrgUserDataFrames, roleDataFrame}
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}

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

    // get user org dataframe
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

    // create fullName column
    userOrgDF = userOrgDF.withColumn("fullName", functions.concat(col("firstName"), lit(' '), col("lastName")))

    // get user roles data
    val userRolesDF = roleDataFrame()     // return - userID, role

    var df = userOrgDF.join(userRolesDF, Seq("userID"), "inner")
    df = df.withColumn("profileDetails", from_json(col("userProfileDetails"), profileDetailsSchema))
    df = df.withColumn("professionalDetails", explode_outer(col("profileDetails.professionalDetails")))
    df = df.withColumn("additionalProperties", explode_outer(col("profileDetails.additionalProperties")))
    df = df.select(
      col("fullName"),
      col("userOrgName").alias("orgName"),
      col("userUpdatedTimestamp").alias("createdDate"),
      col("role").alias("roles"),
      col("professionalDetails.designation").alias("designation"),
      col("professionalDetails.group").alias("group"),
      col("additionalProperties.tag").alias("tag")
//        External System Reference Id and External System REference colums
    )

    show(df)

  }

  val professionalDetailsSchema: StructType = StructType(Seq(
    StructField("designation", StringType, true),
    StructField("group", StringType, true)
  ))

  val additionalPropertiesSchema: StructType = StructType(Seq(
    StructField("tag", ArrayType(StringType), true)
  ))

  val profileDetailsSchema: StructType = StructType(Seq(
    StructField("professionalDetails", ArrayType(professionalDetailsSchema) , true),
    StructField("additionalProperties", ArrayType(additionalPropertiesSchema), true)
  ))

}
