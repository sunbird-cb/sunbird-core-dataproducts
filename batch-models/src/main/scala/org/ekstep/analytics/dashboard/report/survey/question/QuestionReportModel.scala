package org.ekstep.analytics.dashboard.report.survey.question

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

import java.text.SimpleDateFormat

object QuestionReportModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.survey.question.QuestionReportModel"

  override def name() = "QuestionReportModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val today = getDate()
    val surveyQuestionReportColumnsConfig = conf.surveyQuestionReportColumnsConfig
    val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
    val surveyQuestionReportColumnsConfigMap = mapper.readValue(surveyQuestionReportColumnsConfig, classOf[Map[String, String]])
    val reportColumnsMap = surveyQuestionReportColumnsConfigMap("reportColumns").asInstanceOf[Map[String, String]]
    val userProfileColumnsMap = surveyQuestionReportColumnsConfigMap("userProfileColumns").asInstanceOf[Map[String, String]]
    val sortingColumns = surveyQuestionReportColumnsConfigMap("sortingColumns")
    val columnsToBeQueried = reportColumnsMap.keys.mkString(",") + ",userProfile"
    val userProfileSchema = StructType(userProfileColumnsMap.keys.toSeq.map(key => StructField(key, StringType, nullable = true)))
    val reportColumns = reportColumnsMap.keys.toList.map(key => col(key).as(reportColumnsMap(key)))
    val userProfileColumns = userProfileColumnsMap.keys.toList.map(key => col(s"parsedProfile.$key").as(userProfileColumnsMap(key)))
    val requiredCsvColumns = reportColumns ++ userProfileColumns
    val reportPath = s"${conf.mlReportPath}/SurveyQuestionsReport"

    /**
     * Check to see if there is any solutionId are passed from config if Yes generate report only for those ID's
     * If not generate report for all unique solutionId's from druid sl-survey datasource.
     */
    val solutionIds = conf.solutionIDs
    if (solutionIds != null && solutionIds.trim.nonEmpty) {
      JobLogger.log("Processing report requests for specified solutionId's")
      val solutionIdsDF = getSolutionIdsAsDF(solutionIds)

      solutionIdsDF.collect().foreach { row =>
        val solutionId = row.getString(0)
        JobLogger.log(s"Started processing report request for solutionId: $solutionId")
        generateSurveyQuestionReport(solutionId)
      }
    } else {
      JobLogger.log("Querying druid to get all the unique solutionId's")
      val solutionIdsDF = loadAllUniqueSolutionIds("sl-survey")

      /**
       * Query mongodb to get solution end-date for all solutionIdsDF
       */
      JobLogger.log("Query mongodb to get solution end-date for all the unique solutionId's")
      val solutionsEndDateDF = getSolutionsEndDate(solutionIdsDF)

      /**
       * Process each solutionId and generate survey question report
       */
      solutionsEndDateDF.collect().foreach { row =>
        val solutionId = row.getString(0)
        val endDate = new SimpleDateFormat("yyyy-MM-dd").format(row.getDate(1))
        if (endDate != null) {
          JobLogger.log(s"Started processing report request for solutionId: $solutionId")
          if (isSolutionWithinReportDate(endDate)) {
            JobLogger.log(s"Solution with Id $solutionId will ends on $endDate")
            generateSurveyQuestionReport(solutionId)
          } else {
            JobLogger.log(s"Solution with Id $solutionId has ended on $endDate date, Hence not generating the report for this ID ")
          }
        } else {
          JobLogger.log(s"End Date for solutionId: $solutionId is NULL, Hence skipping generating the report for this ID ")
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

    /**
     * This method takes solutionId to query, parse userProfile JSON and sort the CSV
     * @param solutionId
     */
    def generateSurveyQuestionReport(solutionId: String) = {
      val dataSource = "sl-survey"
      val originalSolutionDf = getSolutionIdData(columnsToBeQueried, dataSource, solutionId)
      JobLogger.log(s"Successfully executed druid query for solutionId: $solutionId")
      val finalSolutionDf = processProfileData(originalSolutionDf, userProfileSchema, requiredCsvColumns)
      JobLogger.log(s"Successfully parsed userProfile key for solutionId: $solutionId")
      val columnsMatch = validateColumns(finalSolutionDf, sortingColumns.split(",").map(_.trim))

      if (columnsMatch == true) {
        val columnsOrder = sortingColumns.split(",").map(_.trim)
        val sortedFinalDF = finalSolutionDf.select(columnsOrder.map(col): _*)
        generateReport(sortedFinalDF, s"${reportPath}", fileName = s"${solutionId}-${today}")
        JobLogger.log(s"Successfully generated survey question report for solutionId: $solutionId")
        syncReports(s"${conf.localReportDir}/${reportPath}", reportPath)
        JobLogger.log(s"Successfully synced report to blob storage for solutionId: $solutionId")
      } else {
        JobLogger.log(s"Error occurred while matching the data frame columns with config sort columns for solutionId: $solutionId")
      }
    }

  }

}
