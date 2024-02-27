package org.ekstep.analytics.dashboard.hall.of.fame

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext

object HallOfFameModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.hall.of.fame.HallOfFameModel"

  override def name() = "HallOfFameModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    // get month start and end dates
    val monthStart = date_format(date_trunc("MONTH", add_months(current_date(), -1)), "yyyy-MM-dd HH:mm:ss")
    val monthEnd = date_format(last_day(add_months(current_date(), -1)), "yyyy-MM-dd 23:59:59")

    // get karma points data
    val karmaPointsData = userKarmaPointsDataFrame().filter(col("credit_date") >= monthStart && col("credit_date") <= monthEnd)

    // get user-org data
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    val userOrgData = userOrgDF.select(col("userID"), col("userOrgID").alias("org_id"), col("userOrgName").alias("org_name"))

    var df = karmaPointsData.join(userOrgData, karmaPointsData.col("userid").equalTo(userOrgData.col("userID")), "full")
      .select(col("userID"),col("points"), col("org_id"), col("org_name"), col("credit_date"))
      .filter(col("org_id") =!= "")

    // calculate average karma points - MDO wise
    df = df.groupBy(col("org_id"), col("org_name"))
      .agg(sum(col("points")).alias("total_kp"), countDistinct(col("userID")).alias("total_users"), max(col("credit_date")).alias("latest_credit_date"))
    df = df.withColumn("average_kp", col("total_kp") / col("total_users"))

    // store Hall of Fame data in cassandra
    df = df.withColumn("month", month(monthStart))
      .withColumn("year", year(monthStart))
      .withColumn("kp_last_calculated", current_timestamp())

    writeToCassandra(df, conf.cassandraUserKeyspace, conf.cassandraHallOfFameTable)

  }
}