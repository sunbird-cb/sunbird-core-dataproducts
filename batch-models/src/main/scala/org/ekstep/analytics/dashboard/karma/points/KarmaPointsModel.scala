package org.ekstep.analytics.dashboard.karma.points

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

object KarmaPointsModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {
  implicit val className: String = "org.ekstep.analytics.dashboard.KarmaPointsModel"
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

    val dateFormatWithTime = "yyyy-MM-dd HH:mm:ss"
    val monthStart = date_format(date_trunc("MONTH", add_months(current_date(), -1)), dateFormatWithTime)
    val monthEnd = date_format(last_day(add_months(current_date(), -1)), "yyyy-MM-dd 23:59:59")


    val convertTimeUUIDToDateStringUDF = udf((timeUUID: String) => {
      val timestamp = (UUID.fromString(timeUUID).timestamp() - 0x01b21dd213814000L) / 10000
      val date = new Date(timestamp)
      val dateFormat = new SimpleDateFormat(dateFormatWithTime)
      dateFormat.format(date)
    })

    // get cbp details like course name and category
    val categories = Seq("Course", "Program", "Blended Program", "CuratedCollections", "Standalone Assessment", "Curated Program")
    val cbpDetails = allCourseProgramESDataFrame(categories).select(col("courseID"), col("courseName"), col("category"))

    // Assign 2 karma points for each rating
    val courseRatingKarmaPointsDF = userCourseRatingDataframe()
      .withColumn("credit_date", convertTimeUUIDToDateStringUDF(col("createdOn"))).filter(col("credit_date").between(monthStart, monthEnd))
    val karmaPointsFromRating = courseRatingKarmaPointsDF.join(cbpDetails, Seq("courseID"), "left")
    val ratingDF = karmaPointsFromRating
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

    updateKarmaPoints(ratingDF, conf.cassandraUserKeyspace, conf.cassandraKarmaPointsTable)

    val userCBPCompletionDF = userCourseProgramCompletionDataFrame().where(col("dbCompletionStatus").equalTo(2))
      .withColumn("formattedCompletionDate", date_format(from_unixtime(col("courseCompletedTimestamp")), dateFormatWithTime))
      .filter(col("formattedCompletionDate").between(monthStart, monthEnd))
    val userCourseCompletionDF = userCBPCompletionDF.join(cbpDetails, Seq("courseID"), "left").where(col("category").equalTo("Course"))
    val firstCompletionDataDF = userCourseCompletionDF.groupByLimit("userID", "formattedCompletionDate", 4).drop("rowNum")
    val userAssessmentDF = userAssessmentDataFrame().select(col("assessChildID"), col("courseID"))
    var karmaPointsFromCourseCompletion = firstCompletionDataDF.join(userAssessmentDF, Seq("courseID"), "left")
      .withColumn("operation_type", lit("COURSE_COMPLETION"))
      .withColumn("hasAssessment", when(col("assessChildID").isNotNull, true).otherwise(false))
      .withColumn("COURSE_COMPLETION", lit(true))
      .withColumn("COURSENAME", col("courseName"))
      .withColumn("ACBP", lit(false))
      .withColumn("ASSESSMENT", col("hasAssessment"))
      .withColumn("addinfo", to_json(struct("COURSE_COMPLETION", "COURSENAME", "ACBP", "ASSESSMENT")))
      .withColumn("points", when(col("hasAssessment"), lit(10)).otherwise(lit(5)))

      karmaPointsFromCourseCompletion = karmaPointsFromCourseCompletion.select(
        col("userID").alias("userid"),
        col("courseID").alias("context_id"),
        col("category").alias("context_type"),
        col("formattedCompletionDate").alias("credit_date"),
        col("addinfo"),col("points"),col("operation_type")
      )
    updateKarmaPoints(karmaPointsFromCourseCompletion, conf.cassandraUserKeyspace, conf.cassandraKarmaPointsTable)
  }
}
