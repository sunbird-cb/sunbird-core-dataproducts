package org.ekstep.analytics.dashboard.report.user

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext


object UserReportModel extends AbsDashboardModel {
  implicit val className: String = "org.ekstep.analytics.dashboard.report.user.UserReportModel"
  override def name() = "UserReportModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val today = getDate()

    // get user roles data
    val userRolesDF = roleDataFrame().groupBy("userID").agg(concat_ws(", ", collect_list("role")).alias("role")) // return - userID, role

    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

    val orgHierarchyData = orgHierarchyDataframe()

    // get the mdoids for which the report are requesting
    // val mdoID = conf.mdoIDs
    // val mdoIDDF = mdoIDsDF(mdoID)

    // var df = mdoIDDF.join(orgDF, Seq("orgID"), "inner").select(col("orgID").alias("userOrgID"), col("orgName"))

    val userData = userOrgDF
      .join(userRolesDF, Seq("userID"), "left")
      .join(orgHierarchyData, Seq("userOrgID"), "left")
      .dropDuplicates("userID")
      .withColumn("Tag", concat_ws(", ", col("additionalProperties.tag")))
      .where(expr("userStatus=1"))

    val fullReportDF = userData
      .withColumn("Report_Last_Generated_On", date_format(current_timestamp(), "dd/MM/yyyy HH:mm:ss a"))
      .select(
        col("userID"),
        col("userOrgID"),
        col("userCreatedBy"),
        col("fullName").alias("Full_Name"),
        col("professionalDetails.designation").alias("Designation"),
        col("personalDetails.primaryEmail").alias("Email"),
        col("personalDetails.mobile").alias("Phone_Number"),
        col("professionalDetails.group").alias("Group"),
        col("Tag"),
        col("ministry_name").alias("Ministry"),
        col("dept_name").alias("Department"),
        col("userOrgName").alias("Organization"),
        from_unixtime(col("userCreatedTimestamp"), "dd/MM/yyyy").alias("User_Registration_Date"),
        col("role").alias("Roles"),
        col("personalDetails.gender").alias("Gender"),
        col("personalDetails.category").alias("Category"),
        col("additionalProperties.externalSystem").alias("External_System"),
        col("additionalProperties.externalSystemId").alias("External_System_Id"),
        col("userOrgID").alias("mdoid"),
        col("Report_Last_Generated_On"),
        from_unixtime(col("userOrgCreatedDate"), "dd/MM/yyyy").alias("MDO_Created_On")
      )
      .coalesce(1)

    val reportPath = s"${conf.userReportPath}/${today}"
    // generateReport(fullReportDF, s"${reportPath}-full")
    val mdoWiseReportDF = fullReportDF.drop("userID", "userOrgID", "userCreatedBy")

    generateAndSyncReports(mdoWiseReportDF, "mdoid", reportPath, "UserReport")

    val df_warehouse = userData
      .withColumn("data_last_generated_on", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss a"))
      .select(
        col("userID").alias("user_id"),
        col("userOrgID").alias("mdo_id"),
        col("fullName").alias("full_name"),
        col("professionalDetails.designation").alias("designation"),
        col("personalDetails.primaryEmail").alias("email"),
        col("personalDetails.mobile").alias("phone_number"),
        col("professionalDetails.group").alias("groups"),
        col("Tag").alias("tag"),
        col("userVerified").alias("is_verified_karmayogi"),
        from_unixtime(col("userCreatedTimestamp"), "dd/MM/yyyy").alias("user_registration_date"),
        col("role").alias("roles"),
        col("personalDetails.gender").alias("gender"),
        col("personalDetails.category").alias("category"),
        col("userCreatedBy").alias("created_by_id"),
        col("additionalProperties.externalSystem").alias("external_system"),
        col("additionalProperties.externalSystemId").alias("external_system_id"),
        col("data_last_generated_on")
      )

    generateReport(df_warehouse.coalesce(1), s"${reportPath}-warehouse")

    Redis.closeRedisConnect()

  }
}
