package org.ekstep.analytics.dashboard.report.survey.question

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

import java.text.SimpleDateFormat

object QuestionReportModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.survey.question.QuestionReportModel"

  override def name() = "QuestionReportModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val today = getDate()

    //TODO remove loc after validating blob storage upload
    val loc = "/Users/user/Desktop/survey_question_report/"

    val reportPath = s"${conf.mlReportPath}/SurveyQuestionsReport"

    val solutionIds = conf.solutionIDs
    if (solutionIds != null && solutionIds.trim.nonEmpty) {
      //TODO revert println to JobLogger.log
      println("Processing report requests from the configurations")
      val solutionIdsDF = getSolutionIdsAsDF(solutionIds)
      solutionIdsDF.show(false)

      solutionIdsDF.collect().foreach { row =>
        val solutionId = row.getString(0)
        //TODO revert println to JobLogger.log
        println(s"Started processing report request for solutionId: $solutionId")
        val solutionDataDF = getSolutionData(solutionId)
        solutionDataDF.show(false)
        csvWrite(solutionDataDF.coalesce(1), s"${loc}${solutionId}.csv")
      }
    } else {
      println(solutionIds)
      println("No id present ")

      //TODO revert println to JobLogger.log
      println("Querying druid to get all the unique solutionId's")
      val solutionIdsDF = loadAllUniqueSolutionIds("sl-survey")
      solutionIdsDF.show(false)

      /**
       * Query mongodb to get solution end-date for all solutionIdsDF
       */
      //TODO revert println to JobLogger.log
      println("Query mongodb to get solution end-date for all the unique solutionId's")
      val solutionsEndDateDF = getSolutionsEndDate(solutionIdsDF)
      solutionsEndDateDF.show(false)

      /**
       * Process each solutionId and generate survey question report
       */
      solutionsEndDateDF.collect().foreach { row =>

        val solutionId = row.getString(0)
        val endDate = new SimpleDateFormat("yyyy-MM-dd").format(row.getDate(1))

        if (endDate != null) {
          //TODO revert println to JobLogger.log
          println(s"Started processing report request for solutionId: $solutionId")
          if (isSolutionWithinReportDate(endDate)) {
            //TODO revert println to JobLogger.log
            println(s"Solution with Id $solutionId will ends on $endDate")
            val solutionDataDF = getSolutionData(solutionId)
            solutionDataDF.show(false)
            csvWrite(solutionDataDF.coalesce(1), s"${loc}${solutionId}.csv")

            generateReport(solutionDataDF, s"${reportPath}", fileName = s"${solutionId}-${today}")
            syncReports(s"${conf.localReportDir}/${reportPath}", reportPath)

          } else {
            //TODO revert println to JobLogger.log
            println(s"Solution with Id $solutionId has ended on $endDate date, Hence not generating the report for this ID ")
          }
        } else {
          //TODO revert println to JobLogger.log
          println(s"End Date for solutionId: $solutionId is NULL, Hence skipping generating the report for this ID ")
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

/**
 *
 * Grace period is 2 days which will be coming from config
 *
 * if today's date is 2024-02-26
 * end date of a survey is 2024-02-26
 * we should generate the report
 *
 * if today's date is 2024-02-26
 * end date of a survey is 2024-02-28
 * we should generate the report
 *
 * if today's date is 2024-02-26
 * end date of a survey is 2024-02-24
 * we should generate the report
 *
 * if today's date is 2024-02-26
 * end date of a survey is 2024-02-23
 * we should not generate the report
 *
 * if today's date is 2024-02-26
 * end date of a survey is 2024-01-17
 * we should not generate the report
 *
 * */
