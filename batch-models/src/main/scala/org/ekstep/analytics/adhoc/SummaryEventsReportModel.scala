package org.ekstep.analytics.adhoc

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil.{druidDFOption, show}
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework._

object SummaryEventsReportModel extends AbsDashboardModel{implicit val className: String = "org.ekstep.analytics.adhoc.SummaryEventsReportModel"

  def name() = "SummaryEventsReportModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    // Hardcoded start time and end time
    val startTimeString = "2022-07-09 00:00:00"
    val endTimeString = "2022-07-12 00:00:00"
    // Read CSV
    val query = raw"""SELECT * FROM \"summary-events\" WHERE __time >= TIMESTAMP '$startTimeString' and __time < TIMESTAMP '$endTimeString'"""
    var df = druidDFOption(query, conf.sparkDruidRouterHost, limit = 1000000).orNull
    show(df, "raw df")

    if(df != null) {

      // Filter DataFrame to include only rows within the specified time range
      val filteredDF = df

      // Print the filtered DataFrame
      println("Filtered DataFrame:")
      filteredDF.show()
      val filteredDFTemp = filteredDF.select("dimensions_pdata_id")
      show(filteredDFTemp,"filteredDF")

      // Count total events
      val totalEvents = filteredDF.count()

      // Filter DataFrame to get portal users
      val portalEventsDF = filteredDF.filter("dimensions_pdata_id= 'prod.sunbird-cb-portal'")
      val portalEvents = portalEventsDF.count()

      // Filter DataFrame to get mobile users
      val totalMobileEventsDF = filteredDF.filter("dimensions_pdata_id = 'prod.karmayogi-mobile-ios' OR dimensions_pdata_id = 'prod.karmayogi-mobile-android'")
      val totalMobileEvents = totalMobileEventsDF.count()

      // Filter DataFrame to get android events
      val androidEventsDF = filteredDF.filter("dimensions_pdata_id = 'prod.karmayogi-mobile-android'")
      val androidEvents= androidEventsDF.count()

      // Filter DataFrame to get iOS events
      val iosEventsDF = filteredDF.filter("dimensions_pdata_id = 'prod.karmayogi-mobile-ios'")
      val iosEvents= iosEventsDF.count()

      // Filter DataFrame to get admin portal events
      val adminPortalEventsDF = filteredDF.filter("dimensions_pdata_id = 'prod.sunbird-cb-adminportal'")
      val adminPortalEvents= adminPortalEventsDF.count()

      val  spark=SparkSession.builder.getOrCreate()
      // Create DataFrame with the specified column headings and counts
      val summaryDF = spark.createDataFrame(Seq(
        ("startTime", startTimeString),
        ("endTime", endTimeString),
        ("totalEvents", totalEvents.toString),
        ("portalEvents", portalEvents.toString),
        ("total Mobile events", totalMobileEvents.toString),
        ("android events", androidEvents.toString),
        ("iOS events", iosEvents.toString),
        ("admin Portal events", adminPortalEvents.toString)
      )).toDF("Metric", "Count")

      // Transpose DataFrame
      val summaryEventsDF = summaryDF.groupBy().pivot("Metric").agg(first("Count"))
        .select("startTime", "endTime", "totalEvents", "portalEvents", "total Mobile events", "android events", "iOS events", "admin Portal events")

      // Display the transposed DataFrame
      println("SummaryEvents DataFrame:")
      summaryEventsDF.show()

      // Define the output path for the CSV file
      val outputPath = "/home/analytics/spark-test/mileena/SummaryEvents/SummaryEventsReport.csv"

      summaryEventsDF.write.option("header", "true").csv(outputPath)
    }
    spark.stop()
  }

}