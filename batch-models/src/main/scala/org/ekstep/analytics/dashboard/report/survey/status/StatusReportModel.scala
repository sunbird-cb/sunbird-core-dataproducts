package org.ekstep.analytics.dashboard.report.survey.status

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

import java.text.SimpleDateFormat

object StatusReportModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.survey.status.StatusReportModel"

  override def name() = "StatusReportModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    val today = getDate()
    JobLogger.log("Querying mongo database to get report configurations")
    val surveyStatusReportColumnsConfig = getReportConfig("surveyStatusReport")
    val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
    val surveyStatusReportColumnsConfigMap = mapper.readValue(surveyStatusReportColumnsConfig, classOf[Map[String, String]])
    val reportColumnsMap = surveyStatusReportColumnsConfigMap("reportColumns").asInstanceOf[Map[String, String]]
    val userProfileColumnsMap = surveyStatusReportColumnsConfigMap("userProfileColumns").asInstanceOf[Map[String, String]]
    val sortingColumns = surveyStatusReportColumnsConfigMap("sortingColumns")
    val columnsToBeQueried = reportColumnsMap.keys.mkString(",") + ",userProfile"
    val userProfileSchema = StructType(userProfileColumnsMap.keys.toSeq.map(key => StructField(key, StringType, nullable = true)))
    val reportColumns = reportColumnsMap.keys.toList.map(key => col(key).as(reportColumnsMap(key)))
    val userProfileColumns = userProfileColumnsMap.keys.toList.map(key => col(s"parsedProfile.$key").as(userProfileColumnsMap(key)))
    val requiredCsvColumns = reportColumns ++ userProfileColumns
    val reportPath = s"${conf.mlReportPath}/${today}/SurveyCompletedSubmissionsReport"

    /**
     * Check to see if there is any solutionId are passed from config if Yes generate report only for those ID's
     * If not generate report for all unique solutionId's from druid sl-survey-meta datasource.
     */
    val solutionIds = conf.solutionIDs
    if (solutionIds != null && solutionIds.trim.nonEmpty) {
      JobLogger.log("Processing report requests from the configurations")
      val solutionIdsDF = getSolutionIdsAsDF(solutionIds)

      solutionIdsDF.collect().foreach { row =>
        val solutionId = row.getString(0)
        val solutionName = row.getString(1)
        JobLogger.log(s"Started processing report request for solutionId: $solutionId")
        generateSurveyStatusReport(solutionId, solutionName)
      }
    } else {
      JobLogger.log("Processing report requests for all solutionId's")
      JobLogger.log("Querying druid to get all the unique solutionId's")
      val solutionIdsDF = loadAllUniqueSolutionIds("sl-survey-meta")

      if (conf.includeExpiredSolutionIDs) {
        JobLogger.log("Generating report for all the expired solutionId's also")
        solutionIdsDF.collect().foreach { row =>
          val solutionId = row.getString(0)
          val solutionName = row.getString(1)
          JobLogger.log(s"Started processing report request for solutionId: $solutionId")
          generateSurveyStatusReport(solutionId, solutionName)
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
              generateSurveyStatusReport(solutionId, solutionName)
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
     * @param solutionId
     */
    def generateSurveyStatusReport(solutionId: String, solutionName: String) = {
      val dataSource = "sl-survey-meta"
      val originalSolutionDf = getSolutionIdData(columnsToBeQueried, dataSource, solutionId)
      JobLogger.log(s"Successfully executed druid query for solutionId: $solutionId")
      val finalSolutionDf = processProfileData(originalSolutionDf, userProfileSchema, requiredCsvColumns)
      JobLogger.log(s"Successfully parsed userProfile key for solutionId: $solutionId")
      val columnsMatch = validateColumns(finalSolutionDf, sortingColumns.split(",").map(_.trim))

      if (columnsMatch == true) {
        val columnsOrder = sortingColumns.split(",").map(_.trim)
        val sortedFinalDF = finalSolutionDf.select(columnsOrder.map(col): _*)
        val solutionsName = solutionName.replace(" ", "_")
        generateReport(sortedFinalDF, s"${reportPath}", fileName = s"${solutionsName}-${solutionId}", fileSaveMode = SaveMode.Append)
        JobLogger.log(s"Successfully generated survey question csv report for solutionId: $solutionId")
      } else {
        JobLogger.log(s"Error occurred while matching the dataframe columns with config sort columns for solutionId: $solutionId")
      }
    }

  }

}
