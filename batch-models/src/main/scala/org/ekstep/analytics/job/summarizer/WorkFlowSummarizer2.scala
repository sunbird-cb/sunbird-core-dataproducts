package org.ekstep.analytics.job.summarizer

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobDriver}
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.model.WorkFlowSummaryModel2

object WorkFlowSummarizer2 extends optional.Application with IJob {
  
    implicit val className = "org.ekstep.analytics.job.WorkFlowSummarizer2"
    
    def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobLogger.log("Started executing Job")
        JobDriver.run("batch", config, WorkFlowSummaryModel2);
        JobLogger.log("Job Completed.")
    }
}