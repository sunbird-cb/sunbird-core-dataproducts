package org.ekstep.analytics.dashboard.report.assess

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.FrameworkContext


object UserAssessmentTest extends Serializable {

  def main(args: Array[String]): Unit = {
    val cassandraHost = testModelConfig().getOrElse("sparkCassandraConnectionHost", "localhost").asInstanceOf[String]
    val esHost = testModelConfig().getOrElse("sparkElasticsearchConnectionHost", "localhost").asInstanceOf[String]
    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .appName("UserAssessmentTest")
        .config("spark.master", "local[*]")
        .config("spark.cassandra.connection.host", cassandraHost)
        .config("spark.cassandra.output.batch.size.rows", "10000")
        //.config("spark.cassandra.read.timeoutMS", "60000")
        .config("spark.sql.legacy.json.allowEmptyString.enabled", "true")
        .config("spark.sql.caseSensitive", "true")
        .config("es.nodes", esHost)
        .config("es.port", "9200")
        .getOrCreate()
    implicit val sc: SparkContext = spark.sparkContext
    implicit val fc: FrameworkContext = new FrameworkContext()
    sc.setLogLevel("WARN")
    val res = time(test());
    Console.println("Time taken to execute script", res._1);
    spark.stop();
  }

  def testModelConfig(): Map[String, AnyRef] = {
    val sideOutput = Map(
      "brokerList" -> "",
      "compression" -> "",
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

      "sparkCassandraConnectionHost" -> "10.0.0.7",
      "sparkDruidRouterHost" -> "10.0.0.13",
      "sparkElasticsearchConnectionHost" -> "10.0.0.7",
      "fracBackendHost" -> "frac-dictionary-backend.igot-dev.in",

      "cassandraUserKeyspace" -> "sunbird",
      "cassandraCourseKeyspace" -> "sunbird_courses",
      "cassandraHierarchyStoreKeyspace" -> "dev_hierarchy_store",

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

  def test()(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    val timestamp = System.currentTimeMillis()
    val config = testModelConfig()
    UserAssessmentModel.processUserAssessmentData(timestamp, config)
  }

  def time[R](block: => R): (Long, R) = {
    val t0 = System.currentTimeMillis()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis()
    ((t1 - t0), result)
  }

}
