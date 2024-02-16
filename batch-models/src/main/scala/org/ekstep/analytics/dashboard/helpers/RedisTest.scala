package org.ekstep.analytics.dashboard.helpers

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.dashboard.{DashboardConfig, DashboardUtil, Redis}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.framework.FrameworkContext
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat

case class RedisData(x: String, y: String) extends Serializable

object RedisTest extends Serializable {

  def main(args: Array[String]): Unit = {
    val config = testModelConfig()
    implicit val (spark, sc, fc) = DashboardUtil.Test.getSessionAndContext("RedisTest", config)
    val res = DashboardUtil.Test.time(test(config));
    Console.println("Time taken to execute script", res._1);
    spark.stop();
  }

  def test(config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    val executionTime = System.currentTimeMillis()
    processData(executionTime, config)
  }

  def processData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = DashboardConfig.parseConfig(config)
    if (conf.debug == "true") DashboardUtil.debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") DashboardUtil.validation = true // set validation to true if explicitly specified in the config

    val processingTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(timestamp)
    val testList = List(List("1", "One") ,List("2", "Two") ,List("3", "Three"),List("4","4"))
    val values = testList.map(x =>(x.head, x(1)))
    import spark.implicits._
    val df = values.toDF("x", "y")

    Redis.update("redis_test_time1", processingTime)
    Redis.updateMapField("redis_test_map_time1", "time", processingTime)
    Redis.dispatchDataFrame[String]("redis_test_df_map1", df, "x", "y")
    Redis.closeRedisConnect()

    val jedis = new Jedis("192.168.3.249", 6379)
    jedis.select(12)
    jedis.set("redis_test_time2", processingTime)
    jedis.hset("redis_test_map_time2", "time", processingTime)
    jedis.hset("redis_test_df_map2", df.toMap[String]("x", "y"))
    jedis.close()

    println("Spark Config:")
    println(spark.conf.getAll)
  }

  def testModelConfig(): Map[String, AnyRef] = {
    val sideOutput = Map(
      "brokerList" -> "192.168.3.249:9092",
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
        "userOrg" -> "dev.dashboards.user.org",
        "org" -> "dev.dashboards.org"
      )
    )
    val modelParams = Map(
      "debug" -> "true",
      "validation" -> "true",

      "redisHost" -> "192.168.3.249",
      "redisPort" -> "6379",
      "redisDB" -> "12",

      "sparkCassandraConnectionHost" -> "192.168.3.211",
      "sparkDruidRouterHost" -> "192.168.3.91",
      "sparkElasticsearchConnectionHost" -> "192.168.3.211",
      "fracBackendHost" -> "frac-dictionary.karmayogi.nic.in",

      "cassandraUserKeyspace" -> "sunbird",
      "cassandraCourseKeyspace" -> "sunbird_courses",
      "cassandraHierarchyStoreKeyspace" -> "dev_hierarchy_store",

      "cassandraUserTable" -> "user",
      "cassandraUserRolesTable" -> "user_roles",
      "cassandraOrgTable" -> "organisation",
      "cassandraUserEnrolmentsTable" -> "user_enrolments",
      "cassandraContentHierarchyTable" -> "content_hierarchy",
      "cassandraRatingSummaryTable" -> "ratings_summary",

      "cutoffTime" -> "60.0",

      "sideOutput" -> sideOutput
    )
    modelParams
  }

}
