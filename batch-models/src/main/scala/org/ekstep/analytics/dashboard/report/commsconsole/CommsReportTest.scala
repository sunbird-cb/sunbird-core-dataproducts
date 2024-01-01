package org.ekstep.analytics.dashboard.report.commsconsole

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.dashboard.DashboardUtil
import org.ekstep.analytics.framework.FrameworkContext

object CommsReportTest extends Serializable{

  def main(args: Array[String]): Unit = {

    val config = testModelConfig()
    implicit val (spark, sc, fc) = DashboardUtil.Test.getSessionAndContext("CommsReportTest", config)
    val res = DashboardUtil.Test.time(test(config));
    Console.println("Time taken to execute script", res._1);
    spark.stop();
  }

  def test(config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    CommsReportModel.processCommsConsoleReport(System.currentTimeMillis(), config)
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
        "userAssessment" -> "dev.dashboards.user.assessment",
        "assessment" -> "dev.dashboards.assessment"
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

      "appPostgresHost" -> "192.168.3.178:5432",
      "appPostgresSchema" -> "sunbird",
      "appPostgresUsername" -> "sunbird",
      "appPostgresCredential" -> "sunbird",
      "appOrgHierarchyTable" -> "org_hierarchy_v4",

      "dwPostgresHost" -> "192.168.3.211:5432",
      "dwPostgresSchema" -> "warehouse",
      "dwPostgresUsername" -> "postgres",
      "dwPostgresCredential" -> "Password@12345678",
      "dwUserTable" -> "user_detail",
      "dwCourseTable" -> "cbp",
      "dwEnrollmentsTable" -> "user_enrolment",
      "dwOrgTable" -> "org_hierarchy",
      "dwAssessmentTable" -> "assessment_detail",
      "dwBPEnrollmentsTable" -> "bp_enrollments",

      "cassandraUserTable" -> "user",
      "cassandraUserRolesTable" -> "user_roles",
      "cassandraOrgTable" -> "organisation",
      "cassandraUserEnrolmentsTable" -> "user_enrolments",
      "cassandraContentHierarchyTable" -> "content_hierarchy",
      "cassandraRatingSummaryTable" -> "ratings_summary",
      "cassandraUserAssessmentTable" -> "user_assessment_data",
      "cassandraRatingsTable" -> "ratings",
      "cassandraOrgHierarchyTable" -> "org_hierarchy",

      "store" -> "s3",
      "container" -> "igot",
      "key" -> "aws_storage_key",
      "secret" -> "aws_storage_secret",

      "userReportPath" -> "standalone-reports/user-report",
      "userEnrolmentReportPath" -> "standalone-reports/user-enrolment-report",
      "courseReportPath" -> "standalone-reports/course-report",
      "cbaReportPath" -> "standalone-reports/cba-report",
      "taggedUsersPath" -> "tagged-users/",
      "standaloneAssessmentReportPath" -> "standalone-reports/user-assessment-report-cbp",
      "blendedReportPath" -> "standalone-reports/blended-program-report",
      "orgReportPath" -> "standalone-reports/org-report",
      "commsConsoleReportPath" -> "standalone-reports/comms-console",
      "commsConsolePrarambhEmailSuffix" -> ".kb@karmayogi.in",
      "commsConsoleNumDaysToConsider" -> "15",
      "commsConsoleNumTopLearnersToConsider" -> "60",
      "commsConsoleNumPrarambhTags" -> "rojgaar,rozgaar,rozgar",
      "commsConsoleNumPrarambhCbpIds" -> "do_11359618144357580811,do_113569878939262976132,do_113474579909279744117,do_113651330692145152128,do_1134122937914327041177,do_113473120005832704152,do_1136364244148060161889,do_1136364937253437441916",
      "mdoIDs" -> "0135071359030722569,01358993635114188855",

      "sideOutput" -> sideOutput
    )
    modelParams
  }

}

