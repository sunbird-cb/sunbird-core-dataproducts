package org.ekstep.analytics.dashboard.verify.derived

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.ekstep.analytics.dashboard.DashboardUtil
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.framework.FrameworkContext


object ValidateDerivedTest extends Serializable {

  def main(args: Array[String]): Unit = {
    val config = testModelConfig()
    implicit val (spark, sc, fc) = DashboardUtil.Test.getSessionAndContext("ValidateDerivedTest", config)
    val res = DashboardUtil.Test.time(test(config));
    Console.println("Time taken to execute script", res._1);
    spark.stop();
  }

  def test(config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    debug = true

    val rootPath = ""

    val jsonFileWFS1 = s"${rootPath}/derived-2023-11-07.json.gz"
    val jsonFileWFS2_04 = s"${rootPath}/derived2-2023-11-07-04.json.gz"
    val jsonFileWFS2_05 = s"${rootPath}/derived2-2023-11-07-05.json.gz"
    val jsonFileWFS2_06 = s"${rootPath}/derived2-2023-11-07-06.json.gz"

    val userDayRootStatsDF_1 = process(jsonFileWFS1, s"${rootPath}/out/summaryDF1")
    val userDayRootStatsDF_2_04 = process(jsonFileWFS2_04, s"${rootPath}/out/summaryDF2_04")
    val userDayRootStatsDF_2_05 = process(jsonFileWFS2_05, s"${rootPath}/out/summaryDF2_05")
    val userDayRootStatsDF_2_06 = process(jsonFileWFS2_06, s"${rootPath}/out/summaryDF2_06")

    val userDayRootStatsDF_1_04 = userDayRootStatsDF_1.where(expr("dateIST = '2023-11-04'"))
    val userDayRootStatsDF_1_05 = userDayRootStatsDF_1.where(expr("dateIST = '2023-11-05'"))
    val userDayRootStatsDF_1_06 = userDayRootStatsDF_1.where(expr("dateIST = '2023-11-06'"))

    Seq(
      ("2023-11-04", userDayRootStatsDF_1_04, userDayRootStatsDF_2_04),
      ("2023-11-05", userDayRootStatsDF_1_05, userDayRootStatsDF_2_05),
      ("2023-11-06", userDayRootStatsDF_1_06, userDayRootStatsDF_2_06)
    ).foreach(x => {
      val (date, userDayRootStatsDF1, userDayRootStatsDF2) = x
      val userID1 = userDayRootStatsDF1.select("uid").distinct()
      val userID2 = userDayRootStatsDF2.select("uid").distinct()
      val usersIn1NotIn2 = userID1.except(userID2)
      val usersIn2NotIn1 = userID2.except(userID1)
      csvWrite(usersIn1NotIn2.coalesce(1), s"${rootPath}/out/diff12-${date}")
      csvWrite(usersIn2NotIn1.coalesce(1), s"${rootPath}/out/diff21-${date}")
    })

  }

  def process(jsonFile: String, outPath: String)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): DataFrame = {
    val summaryDF = getSummaryEventDF(jsonFile)
    val userDayRootStatsDF = analyse(summaryDF, outPath)
    userDayRootStatsDF
  }

  def analyse(summaryDF: DataFrame, path: String): DataFrame = {
    val rootSummaryDF = summaryDF.where(expr("dimensions.type='app'"))

    val dayStatsDF = getStats(summaryDF.groupBy("dateIST"))
    csvWrite(dayStatsDF.coalesce(1), s"${path}/dayStatsDF")

    val dayRootStatsDF = getStats(rootSummaryDF.groupBy("dateIST"))
    csvWrite(dayRootStatsDF.coalesce(1), s"${path}/dayRootStatsDF")

    val userDayStatsDF = getStats(summaryDF.groupBy("dateIST", "uid")).orderBy("dateIST", "uid")
    csvWrite(userDayStatsDF.coalesce(1), s"${path}/userDayStatsDF")

    val userDayRootStatsDF = getStats(rootSummaryDF.groupBy("dateIST", "uid")).orderBy("dateIST", "uid")
    csvWrite(userDayRootStatsDF.coalesce(1), s"${path}/userDayRootStatsDF")

    userDayRootStatsDF
  }

  def getStats(groupedDF: RelationalGroupedDataset): DataFrame = {
    val df = groupedDF.agg(
      count("*").alias("count"),

      from_utc_timestamp(min("ets"), "Asia/Kolkata").alias("etsMin"),
      from_utc_timestamp(max("ets"), "Asia/Kolkata").alias("etsMax"),

      from_utc_timestamp(min("syncts"), "Asia/Kolkata").alias("synctsMin"),
      from_utc_timestamp(max("syncts"), "Asia/Kolkata").alias("synctsMax"),

      count("mid").alias("midCount"),
      countDistinct("mid").alias("midDistinctCount"),

      count("uid").alias("uidCount"),
      countDistinct("uid").alias("uidDistinctCount"),

      count("dimensions.did").alias("didCount"),
      countDistinct("dimensions.did").alias("didDistinctCount"),

      count("dimensions.channel").alias("channelCount"),
      countDistinct("dimensions.channel").alias("channelDistinctCount"),

      count("object.id").alias("objectCount"),
      countDistinct("object.id").alias("objectDistinctCount"),

      sum("edata.eks.interact_events_count").alias("interactCount"),
      sum("edata.eks.time_spent").alias("timeSpent")
    )

    df
  }

  def getSummaryEventDF(jsonFile: String)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): DataFrame = {
    var df = spark.read.json(jsonFile)

    df = df
      .withColumn("ets", to_timestamp(expr("ROUND(ets / 1000, 0)").cast(LongType)))
      .withColumn("syncts", to_timestamp(expr("ROUND(syncts / 1000, 0)").cast(LongType)))
      .withColumn("dateTimeIST", from_utc_timestamp(col("ets"), "Asia/Kolkata"))
      .withColumn("dateIST", to_date(col("dateTimeIST"),"yyyy-MM-dd"))

    show(df)
    df
  }

  def testModelConfig(): Map[String, AnyRef] = {
    val sideOutput = Map(
      "brokerList" -> "",
      "compression" -> "none",
      "topics" -> Map(
        "roleUserCount" -> "dev.dashboards.role.count",
        "orgRoleUserCount" -> "dev.dashboards.org.role.count",
        "allCourses" -> "dev.dashboards.course",
        "userCourseProgramProgress" -> "dev.dashboards.user.course.program.progress",
        "fracCompetency" -> "dev.dashboards.competency.frac",
        "courseCompetency" -> "dev.dashboards.competency.course",
        "expectedCompetency" -> "dev.dashboards.competency.expected",
        "declaredCompetency" -> "dev.dashboards.competency.declared",
        "competencyGap" -> "dev.dashboards.competency.gap",
        "userOrg" -> "dev.dashboards.user.org"
      )
    )
    val modelParams = Map(
      "debug" -> "true",
      "validation" -> "true",

      "redisHost" -> "",
      "redisPort" -> "6379",
      "redisDB" -> "12",

      "sparkCassandraConnectionHost" -> "192.168.3.200",
      "sparkDruidRouterHost" -> "192.168.3.21",
      "sparkElasticsearchConnectionHost" -> "192.168.3.90",
      "fracBackendHost" -> "frac-dictionary.igotkarmayogi.gov.in",

      "cassandraUserKeyspace" -> "sunbird",
      "cassandraCourseKeyspace" -> "sunbird_courses",
      "cassandraHierarchyStoreKeyspace" -> "prod_hierarchy_store",

      "cassandraUserTable" -> "user",
      "cassandraUserRolesTable" -> "user_roles",
      "cassandraOrgTable" -> "organisation",
      "cassandraUserEnrolmentsTable" -> "user_enrolments",
      "cassandraContentHierarchyTable" -> "content_hierarchy",
      "cassandraRatingSummaryTable" -> "ratings_summary",

      "sideOutput" -> sideOutput
    )
    modelParams
  }

}
