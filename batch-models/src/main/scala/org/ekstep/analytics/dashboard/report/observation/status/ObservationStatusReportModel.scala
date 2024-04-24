package org.ekstep.analytics.dashboard.report.observation.status

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

import java.text.SimpleDateFormat

object ObservationStatusReportModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.observation.status.ObservationStatusReportModel"

  override def name() = "ObservationStatusReportModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    val today = getDate()
    JobLogger.log("Querying mongo database to get report configurations")
    val observationStatusReportColumnsConfig = getReportConfig("observationStatusReport")
    val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
    val observationStatusReportColumnsConfigMap = mapper.readValue(observationStatusReportColumnsConfig, classOf[Map[String, String]])
    val reportColumnsMap = observationStatusReportColumnsConfigMap("reportColumns").asInstanceOf[Map[String, String]]
    val userProfileColumnsMap = observationStatusReportColumnsConfigMap("userProfileColumns").asInstanceOf[Map[String, String]]
    val sortingColumns = observationStatusReportColumnsConfigMap("sortingColumns")
    val columnsToBeQueried = reportColumnsMap.keys.mkString(",") + ",userProfile"
    val userProfileSchema = StructType(userProfileColumnsMap.keys.toSeq.map(key => StructField(key, StringType, nullable = true)))
    val reportColumns = reportColumnsMap.keys.toList.map(key => col(key).as(reportColumnsMap(key)))
    val userProfileColumns = userProfileColumnsMap.keys.toList.map(key => col(s"parsedProfile.$key").as(userProfileColumnsMap(key)))
    val requiredCsvColumns = reportColumns ++ userProfileColumns
    val reportPath = s"${conf.mlReportPath}/${today}/ObservationCompletedSubmissionsReport" 

    /**
     * Check to see if there is any solutionId are passed from config if Yes generate report only for those ID's
     * If not generate report for all unique solutionId's from druid sl-observation-meta datasource.
     */
    val solutionIds = conf.solutionIDs
    if (solutionIds != null && solutionIds.trim.nonEmpty) {
      JobLogger.log("Processing report requests from the configurations")
      val solutionIdsDF = getSolutionIdsAsDF(solutionIds)

      solutionIdsDF.collect().foreach { row =>
        val solutionId = row.getString(0)
        val solutionName = row.getString(1)
        JobLogger.log(s"Started processing report request for solutionId: $solutionId")
        generateObservationStatusReport(solutionId, solutionName)
      }
    } else {
      JobLogger.log("Processing report requests for all solutionId's")
      JobLogger.log("Querying druid to get all the unique solutionId's")
      val solutionIdsDF = loadAllUniqueSolutionIds("sl-observation-meta")

      if (conf.includeExpiredSolutionIDs) {
        JobLogger.log("Generating report for all the expired solutionId's also")
        solutionIdsDF.collect().foreach { row =>
          val solutionId = row.getString(0)
          val solutionName = row.getString(1)
          JobLogger.log(s"Started processing report request for solutionId: $solutionId")
          generateObservationStatusReport(solutionId, solutionName)
        }
      } else {
        JobLogger.log("Query mongodb to get solution end-date for all the unique solutionId's")
        val solutionsEndDateDF = getSolutionsEndDate(solutionIdsDF)
        solutionsEndDateDF.collect().foreach { row =>
          val solutionId = row.getString(0)
          val solutionName = row.getString(1)
          val endDate = new SimpleDateFormat("yyyy-MM-dd").format(row.getDate(1))
          if (endDate != null) {
            JobLogger.log(s"Started processing report request for solutionId: $solutionId")
            if (isSolutionWithinReportDate(endDate)) {
              JobLogger.log(s"Solution with Id $solutionId will ends on $endDate")
              generateObservationStatusReport(solutionId, solutionName)
            } else {
              JobLogger.log(s"Solution with Id $solutionId has ended on $endDate date, Hence not generating the report for this ID ")
            }
          } else {
            JobLogger.log(s"End Date for solutionId: $solutionId is NULL, Hence skipping generating the report for this ID ")
          }
        }
      }

      /**
       * This method takes the endDate and checks if that date is within the Report Date
       *
       * @param endDate
       * @return
       */
      def isSolutionWithinReportDate(endDate: String): Boolean = {
        val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
        val today = LocalDate.now()
        val updatedDate = today.minusDays(conf.gracePeriod.toInt)
        val endDateOfSolution = formatter.parseLocalDate(endDate)
        endDateOfSolution.isEqual(today) || (endDateOfSolution.isAfter(today) || endDateOfSolution.isAfter(updatedDate)) || endDateOfSolution.isEqual(updatedDate)
      }
    }
    JobLogger.log("Zipping the csv content folder and syncing to blob storage")
    zipAndSyncReports(s"${conf.localReportDir}/${reportPath}", reportPath)
    JobLogger.log("Successfully zipped folder and synced to blob storage")

    /**
     * This method takes solutionId to query, parse userProfile JSON, append status data and sort the CSV
     *
     * @param solutionId
     */
    def generateObservationStatusReport(solutionId: String, solutionName: String) = {
      val dataSource = "sl-observation-meta"
      val originalSolutionDf = getSolutionIdData(columnsToBeQueried, dataSource, solutionId)
      JobLogger.log(s"Successfully executed druid query for solutionId: $solutionId")
      val solutionWithUserProfileDF = processProfileData(originalSolutionDf, userProfileSchema, requiredCsvColumns)
      JobLogger.log(s"Successfully parsed userProfile key for solutionId: $solutionId")
      val finalSolutionDf = appendObservationStatusData(solutionWithUserProfileDF)
      JobLogger.log(s"Successfully added observation status details for solutionId: $solutionId")
      val columnsMatch = validateColumns(finalSolutionDf, sortingColumns.split(",").map(_.trim))

      if (columnsMatch == true) {
        val columnsOrder = sortingColumns.split(",").map(_.trim)
        val sortedFinalDF = finalSolutionDf.select(columnsOrder.map(col): _*)
        val solutionsName = solutionName
          .replaceAll("[^a-zA-Z0-9\\s]", "")
          .replaceAll("\\s+", " ")
          .trim()
        generateReport(sortedFinalDF, s"${reportPath}", fileName = s"${solutionsName}-${solutionId}", fileSaveMode = SaveMode.Append)
        JobLogger.log(s"Successfully generated observation status csv report for solutionId: $solutionId")
      } else {
        JobLogger.log(s"Error occurred while matching the dataframe columns with config sort columns for solutionId: $solutionId")
      }
    }

    def appendObservationStatusData(solutionDf: DataFrame): DataFrame = {
      JobLogger.log("Processing for observation completed status")
      val completedStatus = getObservationStatusCompletedData(solutionDf)
      val filteredCompletedStatus = completedStatus.filter(col("Status of Submission").isNull || col("Status of Submission") === "")
        .select("Observation Submission Id", "Status of Submission")

      if (!filteredCompletedStatus.isEmpty) {
        JobLogger.log("Processing for observation inprogress status")
        val inProgressStatus = getObservationStatusInProgressData(filteredCompletedStatus)
        val joinedCompletedAndInProgressDF = completedStatus
          .join(inProgressStatus, Seq("Observation Submission Id"), "left")
          .select(
            completedStatus("*"),
            coalesce(inProgressStatus("Status of Submission"), completedStatus("Status of Submission")).as("Updated Status of Submission")
          ).drop("Status of Submission")
          .withColumnRenamed("Updated Status of Submission", "Status of Submission")
        val filteredInProgressStatus = joinedCompletedAndInProgressDF.filter(col("Status of Submission").isNull || col("Status of Submission") === "")
          .select("Observation Submission Id", "Status of Submission")

        if (!filteredInProgressStatus.isEmpty) {
          JobLogger.log("Processing for observation started status")
          val joinedStartedAndInProgressDF = joinedCompletedAndInProgressDF.na.fill("Started", Seq("Status of Submission"))
          return joinedStartedAndInProgressDF
        } else {
          return joinedCompletedAndInProgressDF
        }
      }
      completedStatus
    }

  }

}
