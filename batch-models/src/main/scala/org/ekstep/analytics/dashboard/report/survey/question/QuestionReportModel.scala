package org.ekstep.analytics.dashboard.report.survey.question

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
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

    val reportPath = s"${conf.mlReportPath}/SurveyQuestionsReport"

    val solutionIds = conf.solutionIDs
    if (solutionIds != null && solutionIds.trim.nonEmpty) {
      JobLogger.log("Processing report requests from the configurations")
      val solutionIdsDF = getSolutionIdsAsDF(solutionIds)

      solutionIdsDF.collect().foreach { row =>
        val solutionId = row.getString(0)
        JobLogger.log(s"Started processing report request for solutionId: $solutionId")
        val solutionDataDF = getSolutionData(solutionId)
        generateReport(solutionDataDF, s"${reportPath}", fileName = s"${solutionId}-${today}")
        syncReports(s"${conf.localReportDir}/${reportPath}", reportPath)
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
            val solutionDataDF = getSolutionData(solutionId)
            generateReport(solutionDataDF, s"${reportPath}", fileName = s"${solutionId}-${today}")
            syncReports(s"${conf.localReportDir}/${reportPath}", reportPath)
          } else {
            JobLogger.log(s"Solution with Id $solutionId has ended on $endDate date, Hence not generating the report for this ID ")
          }
        } else {
          JobLogger.log(s"End Date for solutionId: $solutionId is NULL, Hence skipping generating the report for this ID ")
        }
      }

      def isSolutionWithinReportDate(endDate: String): Boolean = {
        val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
        val today = LocalDate.now()
        val updatedDate = today.minusDays(conf.gracePeriod.toInt)
        val endDateOfSolution = formatter.parseLocalDate(endDate)
        endDateOfSolution.isEqual(today) || (endDateOfSolution.isAfter(today) || endDateOfSolution.isAfter(updatedDate)) || endDateOfSolution.isEqual(updatedDate)
      }

    }
  }

}
