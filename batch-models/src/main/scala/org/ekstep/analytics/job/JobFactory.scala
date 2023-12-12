package org.ekstep.analytics.job

import org.ekstep.analytics.dashboard.CompetencyMetricsJob
import org.ekstep.analytics.dashboard.report.rozgar.enrolment.RozgarEnrolmentJob
import org.ekstep.analytics.dashboard.report.assess.UserAssessmentJob
import org.ekstep.analytics.dashboard.report.cba.CourseBasedAssessmentJob
import org.ekstep.analytics.dashboard.report.enrolment.UserEnrolmentJob
import org.ekstep.analytics.dashboard.report.user.UserReportJob
import org.ekstep.analytics.dashboard.report.course.CourseReportJob
import org.ekstep.analytics.dashboard.survey.nps.NpsJob
import org.ekstep.analytics.dashboard.report.course_new.CourseReportJobNew
import org.ekstep.analytics.dashboard.report.enrolment_new.UserEnrolmentJobNew
import org.ekstep.analytics.dashboard.report.rozgar.RozgarUserJob

import scala.reflect.runtime.universe
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.exception.JobNotFoundException
import org.ekstep.analytics.job.batch.VideoStreamingJob
import org.ekstep.analytics.job.metrics.MetricsAuditJob
import org.ekstep.analytics.job.summarizer.DruidQueryProcessor
import org.ekstep.analytics.job.summarizer.ExperimentDefinitionJob
import org.ekstep.analytics.job.summarizer.MonitorSummarizer
import org.ekstep.analytics.job.summarizer.WorkFlowSummarizer
import org.ekstep.analytics.job.summarizer.WorkFlowSummarizer2
import org.ekstep.analytics.job.updater.ContentRatingUpdater
import org.ekstep.analytics.exhaust.OnDemandDruidExhaustJob

/**
 * @author Santhosh
 */

object JobFactory {
  @throws(classOf[JobNotFoundException])
  def getJob(jobType: String): IJob = {
    jobType.toLowerCase() match {
      case "monitor-job-summ" =>
        MonitorSummarizer
      case "wfs" =>
        WorkFlowSummarizer
      case "competency-metrics" =>
        CompetencyMetricsJob
      case "assessment-metrics" =>
        UserAssessmentJob
      case "user-report" =>
        UserReportJob
      case "user-enrolment-report" =>
        UserEnrolmentJobNew
      case "course-report" =>
        CourseReportJobNew
      case "course-based-assessment-report" =>
        CourseBasedAssessmentJob
      case "rozgar-user-report" =>
        RozgarUserJob
      case "rozgar-enrolment-report" =>
        RozgarEnrolmentJob
      case "video-streaming" =>
        VideoStreamingJob
      case "telemetry-replay" =>
        EventsReplayJob
      case "summary-replay" =>
        EventsReplayJob
      case "content-rating-updater" =>
        ContentRatingUpdater
      case "experiment" =>
        ExperimentDefinitionJob
      case "audit-metrics-report" =>
        MetricsAuditJob
      case "druid_reports" =>
        DruidQueryProcessor
      case "druid-dataset" =>
        OnDemandDruidExhaustJob
      case "survey-nps" =>
        NpsJob
      case _ =>
        reflectModule(jobType);
    }
  }

  def reflectModule(jobClass: String): IJob = {
    try {
      val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
      val module = runtimeMirror.staticModule(jobClass)
      val obj = runtimeMirror.reflectModule(module)
      obj.instance.asInstanceOf[IJob]
    } catch {
      case ex: Exception =>
        throw new JobNotFoundException("Unknown job type found")
    }
  }

}
