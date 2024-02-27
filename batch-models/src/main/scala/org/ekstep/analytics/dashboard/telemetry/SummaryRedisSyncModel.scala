package org.ekstep.analytics.dashboard.telemetry

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework._


object SummaryRedisSyncModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.telemetry.SummaryRedisSyncModel"
  override def name() = "SummaryRedisSyncModel"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    // Users active
    // SELECT dimension_channel, COUNT(DISTINCT(uid)) as active_count FROM \"summary-events\"
    // WHERE dimensions_type='app' AND __time > CURRENT_TIMESTAMP - INTERVAL '12' MONTH GROUP BY 1
    //val activeUsersLast12MonthsDF = activeUsersLast12MonthsDataFrame()
    //Redis.dispatchDataFrame[Long]("dashboard_active_users_last_12_months_by_org", activeUsersLast12MonthsDF, "orgID", "activeCount")

    // Daily time spent by users
    // SELECT dimension_channel AS orgID, SUM(total_time_spent)/(30 * 3600.0) as timeSpent FROM \"summary-events\"
    // WHERE dimensions_type='app' AND __time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY GROUP BY 1
    val dailyTimeSpentLast30DaysDF = dailyTimeSpentLast30DaysDataFrame()
    Redis.dispatchDataFrame[Float]("dashboard_time_spent_last_30_days_by_org", dailyTimeSpentLast30DaysDF, "orgID", "timeSpent")

    // Daily active users
    // SELECT dimension_channel, COUNT(DISTINCT(actor_id)) as active_count FROM \"summary-events\"
    // WHERE dimensions_type='app' AND __time > CURRENT_TIMESTAMP - INTERVAL '24' HOUR GROUP BY 1
    val activeUsersLast24HoursDF = activeUsersLast24HoursDataFrame()
    Redis.dispatchDataFrame[Long]("dashboard_active_users_last_24_hours_by_org", activeUsersLast24HoursDF, "orgID", "activeCount")


    Redis.closeRedisConnect()

  }

//  def activeUsersLast12MonthsDataFrame()(implicit spark: SparkSession, conf: DashboardConfig) : DataFrame = {
//    val query = """SELECT dimension_channel AS orgID, COUNT(DISTINCT(uid)) as activeCount FROM \"summary-events\" WHERE dimensions_type='app' AND __time > CURRENT_TIMESTAMP - INTERVAL '12' MONTH GROUP BY 1"""
//    var df = druidDFOption(query, conf.sparkDruidRouterHost).orNull
//    if (df == null) return emptySchemaDataFrame(Schema.activeUsersSchema)
//
//    df = df.withColumn("activeCount", expr("CAST(activeCount as LONG)"))  // Important to cast as long otherwise a cast will fail later on
//
//    show(df)
//    df
//  }

  def dailyTimeSpentLast30DaysDataFrame()(implicit spark: SparkSession, conf: DashboardConfig) : DataFrame = {
    val query = """SELECT dimension_channel AS orgID, SUM(total_time_spent)/(30 * 3600.0) as timeSpent FROM \"summary-events\" WHERE dimensions_type='app' AND __time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY GROUP BY 1"""
    var df = druidDFOption(query, conf.sparkDruidRouterHost).orNull
    if (df == null) return emptySchemaDataFrame(Schema.timeSpentSchema)

    df = df.withColumn("timeSpent", expr("CAST(timeSpent as FLOAT)"))  // Important to cast as float otherwise a cast will fail later on

    show(df)
    df
  }

  def activeUsersLast24HoursDataFrame()(implicit spark: SparkSession, conf: DashboardConfig) : DataFrame = {
    val query = """SELECT dimension_channel AS orgID, COUNT(DISTINCT(uid)) as activeCount FROM \"summary-events\" WHERE dimensions_type='app' AND __time > CURRENT_TIMESTAMP - INTERVAL '24' HOUR GROUP BY 1"""
    var df = druidDFOption(query, conf.sparkDruidRouterHost).orNull
    if (df == null) return emptySchemaDataFrame(Schema.activeUsersSchema)

    df = df.withColumn("activeCount", expr("CAST(activeCount as LONG)"))  // Important to cast as long otherwise a cast will fail later on

    show(df)
    df
  }

}
