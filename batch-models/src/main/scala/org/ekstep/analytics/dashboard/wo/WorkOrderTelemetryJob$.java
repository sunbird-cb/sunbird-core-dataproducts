package org.ekstep.analytics.dashboard.wo;object WorkOrderTelemetryJob extends optional.Application with IJob {

        implicit val className = "org.ekstep.analytics.dashboard.WorkOrderTelemetryJob"

        def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
                implicit val sparkContext: SparkContext = sc.getOrElse(null);
                JobLogger.log("Started executing Job")
                // JobDriver.run("batch", config, WorkOrderTelemetryModel);
                JobLogger.log("Job Completed.")
                }
        }
