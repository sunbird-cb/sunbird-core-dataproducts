package org.ekstep.analytics.dashboard.exhaust

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.dashboard.DashboardConfig
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.framework._

/**
 * Model for processing dashboard data
 */
object DataExhaustModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.DashboardSyncModel"
  override def name() = "DashboardSyncModel"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   * @param config model config, should be defined at sunbird-data-pipeline:ansible/roles/data-products-deploy/templates/model-config.j2
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val orgDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraOrgTable)
    cache.write(orgDF, "org")

    val userDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraUserTable)
    cache.write(userDF, "user")

    val roleDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraUserRolesTable)
    cache.write(roleDF, "role")

    val hierarchyDF = cassandraTableAsDataFrame(conf.cassandraHierarchyStoreKeyspace, conf.cassandraContentHierarchyTable)
    cache.write(hierarchyDF, "hierarchy")

    val ratingSummaryDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraRatingSummaryTable)
    cache.write(ratingSummaryDF, "ratingSummary")

    val orgHierarchyDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraOrgHierarchyTable)
    cache.write(orgHierarchyDF, "orgHierarchy")

    val ratingDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraRatingsTable)
    cache.write(ratingDF, "rating")

    val batchDF = cassandraTableAsDataFrame(conf.cassandraCourseKeyspace, conf.cassandraCourseBatchTable)
    cache.write(batchDF, "batch")

    val enrolmentDF = cassandraTableAsDataFrame(conf.cassandraCourseKeyspace, conf.cassandraUserEnrolmentsTable)
    cache.write(enrolmentDF, "enrolment")

    val contentConsumptionDF = cassandraTableAsDataFrame(conf.cassandraCourseKeyspace, "user_content_consumption")
    cache.write(contentConsumptionDF, "contentConsumption")

    val userAssessmentDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraUserAssessmentTable)
    cache.write(userAssessmentDF, "userAssessment")

    val learnerStatsDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraLearnerStatsTable)
    cache.write(learnerStatsDF, "learnerStats")

    val karmaPointsDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraKarmaPointsTable)
    cache.write(karmaPointsDF, "karmaPoints")

    val userFeedDF = cassandraTableAsDataFrame(conf.cassandraUserFeedKeyspace, conf.cassandraUserFeedTable)
    cache.write(userFeedDF, "userFeed")

    val acbpDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraAcbpTable)
    cache.write(acbpDF, "acbp")


  }

}
