package org.ekstep.analytics.dashboard.karma.points

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput, Redis}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}
import org.joda.time.DateTime

import java.util.UUID

object KarmaPointsModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {
  implicit val className: String = "org.ekstep.analytics.dashboard.karma.points.KarmaPointsModel"
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
    processKarmaPoints(timestamp, config)
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

  def processKarmaPoints(timestamp: Long, config: Map[String, AnyRef]) (implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if(conf.validation == "true") validation = true

    val currentDate = DateTime.now()
    val monthStart = currentDate.minusMonths(1).withDayOfMonth(1).getMillis / 1000
    val monthEnd = currentDate.withDayOfMonth(1).getMillis / 1000

    val timeUUIDToTimestamp = udf((timeUUID: String) => (UUID.fromString(timeUUID).timestamp() - 0x01b21dd213814000L) / 10000)

    // filter only previous month's ratings
    val courseRatingKarmaPointsDF = userCourseRatingDataframe()
      .withColumn("credit_date", timeUUIDToTimestamp(col("createdOn")))
      .where(s"credit_date >= '${monthStart}' AND credit_date < '${monthEnd}'")
    show(courseRatingKarmaPointsDF, "courseRatingKarmaPointsDF")

    // get cbp details like course name and category
    val categories = Seq("Course", "Program", "Blended Program", "CuratedCollections", "Standalone Assessment", "Curated Program")
    val cbpDetails = allCourseProgramESDataFrame(categories)
      .where("courseStatus IN ('Live', 'Retired')")
      .select("courseID", "courseName", "category")
    val courseDetails = cbpDetails.where("category='Course'")

    val karmaPointsFromRatingDF = courseRatingKarmaPointsDF
      .join(cbpDetails, Seq("courseID"), "left")
      .withColumn("operation_type", lit("RATING"))
      .withColumn("COURSENAME", col("courseName"))
      .withColumn("addinfo", to_json(struct("COURSENAME")))
      .withColumn("points", lit(2).cast("int"))
      .select(
        col("courseID").alias("context_id"),
        col("userID").alias("userid"),
        col("category").alias("context_type"),
        col("credit_date"),
        col("operation_type"),
        col("addinfo"),
        col("points")
      )
    show(karmaPointsFromRatingDF, "karmaPointsFromRatingDF")

    val userCourseCompletionDF = userCourseProgramCompletionDataFrame(datesAsLong = true)
      .where(s"dbCompletionStatus=2 AND courseCompletedTimestamp >= '${monthStart}' AND courseCompletedTimestamp < '${monthEnd}'")
      .join(courseDetails, Seq("courseID"), "inner")
    show(userCourseCompletionDF, "userCourseCompletionDF")

    val firstCompletionDataDF = userCourseCompletionDF
      .groupByLimit(Seq("userID"), "courseCompletedTimestamp", 4)
      .drop("rowNum")
    show(firstCompletionDataDF, "firstCompletionDataDF")

    val coursesWithAssessmentDF = userAssessmentDataFrame()
      .where("assessUserStatus='SUBMITTED' AND assessChildID IS NOT NULL")
      .select("courseID")
      .distinct()
      .withColumn("hasAssessment", lit(true))
    show(coursesWithAssessmentDF, "coursesWithAssessmentDF")

    val karmaPointsFromCourseCompletionDF = firstCompletionDataDF
      .join(coursesWithAssessmentDF, Seq("courseID"), "left")
      .na.fill(value = false, Seq("hasAssessment"))
      .withColumn("operation_type", lit("COURSE_COMPLETION"))
      .withColumn("COURSE_COMPLETION", lit(true))
      .withColumn("COURSENAME", col("courseName"))
      .withColumn("ACBP", lit(false))
      .withColumn("ASSESSMENT", col("hasAssessment"))
      .withColumn("addinfo", to_json(struct("COURSE_COMPLETION", "COURSENAME", "ACBP", "ASSESSMENT")))
      .withColumn("points", when(col("hasAssessment"), lit(10)).otherwise(lit(5)))
      .select(
        col("courseID").alias("context_id"),
        col("userID").alias("userid"),
        col("category").alias("context_type"),
        col("courseCompletedTimestamp").alias("credit_date"),
        col("operation_type"),
        col("addinfo"),
        col("points")
      )
    show(karmaPointsFromCourseCompletionDF, "karmaPointsFromCourseCompletionDF")

    // union both karma points dataframes, and write to cassandra
    val allKarmaPointsDF = karmaPointsFromRatingDF.union(karmaPointsFromCourseCompletionDF)
    show(allKarmaPointsDF, "allKarmaPointsDF")
    writeToCassandra(allKarmaPointsDF, conf.cassandraUserKeyspace, conf.cassandraKarmaPointsTable)

    // write to lookup table
    val lookupDataDF = allKarmaPointsDF
      .select("userid", "context_type", "context_id", "operation_type", "credit_date")
      .withColumn("user_karma_points_key", concat_ws("|", col("userid"), col("context_type"), col("context_id")))
      .drop("userid", "context_type", "context_id")
    show(lookupDataDF, "lookupDataDF")
    writeToCassandra(lookupDataDF, conf.cassandraUserKeyspace, conf.cassandraKarmaPointsLookupTable)

    // update summary table
    val existingSummaryData = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraKarmaPointsSummaryTable)
      .select(col("userid"), col("total_points").alias("existing_total_points"))
    show(existingSummaryData, "existingSummaryData")

    val summaryDataDF = allKarmaPointsDF
      .select("userid", "points", "context_id")
      .groupBy("userid")
      .agg(sum(col("points")).alias("points"))
      .join(existingSummaryData, Seq("userid"), "full")
      .na.fill(0, Seq("existing_total_points", "points"))
      .withColumn("total_points", expr("existing_total_points + points"))
      .select("userid", "total_points")
    show(summaryDataDF, "summaryDataDF")
    writeToCassandra(summaryDataDF, conf.cassandraUserKeyspace, conf.cassandraKarmaPointsSummaryTable)

    Redis.closeRedisConnect()
  }

}
