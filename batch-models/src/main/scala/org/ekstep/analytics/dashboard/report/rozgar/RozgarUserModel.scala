package org.ekstep.analytics.dashboard.report.rozgar

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}


object RozgarUserModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {
  implicit val className: String = "org.ekstep.analytics.dashboard.report.adhocReports.rozgar.RozgarUserModel"
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

    val today = getDate()
    val reportPath = s"${conf.userReportTempPath}/${today}/"
    val taggedUsersPath = s"${reportPath}${conf.taggedUsersPath}"

    // get user roles data
    val userRolesDF = roleDataFrame()     // return - userID, role

    val userDataDF = userProfileDetailsDF().withColumn("fullName", concat(coalesce(col("firstName"), lit("")), lit(' '),
      coalesce(col("lastName"), lit(""))))

    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    val orgHierarchyData = orgHierarchyDataframe()

    // get the mdoids for which the report are requesting
    val mdoID = conf.mdoIDs
    val mdoIDDF = mdoIDsDF(mdoID)

    var df = mdoIDDF.join(orgDF, Seq("orgID"), "inner").select(col("orgID").alias("userOrgID"), col("orgName"))

    df = df.join(userDataDF, Seq("userOrgID"), "inner").join(userRolesDF, Seq("userID"), "left")
      .join(orgHierarchyData, Seq("userOrgName"),"left")

    df = df.where(expr("userStatus=1"))
    df = df.withColumn("User_Tag", explode(col("additionalProperties.tag"))).filter(col("User_Tag") === "Rozgar Mela")

    df = df.dropDuplicates("userID").select(
      col("fullName").alias("Full_Name"),
      col("professionalDetails.designation").alias("Designation"),
      col("personalDetails.primaryEmail").alias("Email"),
      col("personalDetails.mobile").alias("Phone_Number"),
      col("professionalDetails.group").alias("Group"),
//      col("User_Tag"),
      col("additionalProperties.tag").alias("Tags").cast("string"),
      col("ministry_name").alias("Ministry"),
      col("dept_name").alias("Department"),
      col("userOrgName").alias("Organization"),
      from_unixtime(col("userCreatedTimestamp"),"dd/MM/yyyy").alias("User_Registration_Date"),
      col("role").alias("Roles"),
      col("personalDetails.gender").alias("Gender"),
      col("personalDetails.category").alias("Category"),
      col("additionalProperties.externalSystem").alias("External_System"),
      col("additionalProperties.externalSystemId").alias("External_System_Id"),
      col("userOrgID").alias("mdoid")
    )

    uploadReports(df, "mdoid", taggedUsersPath, s"${conf.userReportPath}/${today}/${conf.taggedUsersPath}")

    closeRedisConnect()
  }
}

