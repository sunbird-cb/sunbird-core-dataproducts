package org.ekstep.analytics.adhoc.mandatory.nonmandatory

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.ekstep.analytics.adhoc.DataUtilUserDetails.Schema.getOrgUserDataFrames
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext

object UserProfileDetails extends AbsDashboardModel {
  implicit val className: String = "org.ekstep.analytics.adhoc.mandatory.nonmandatory.UserProfileDetails"
  override def name() = "UserProfileDetails"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    userDF.show(false)
    val count = userDF.rdd.count()
    println("count - " + count)

    val replaceNewlines = udf((text: String) => {
      if (text != null) text.replaceAll("\n", "") else null
    })

    var fullDetailsDF = userDF.dropDuplicates("userID")
      .select(
        col("userID"),
        col("userUpdatedTimestamp"),
        col("userOrgID"),
        col("professionalDetails.designation").alias("Designation"),
        col("personalDetails.primaryEmail").alias("Email"),
        col("personalDetails.mobile").alias("Phone_Number"),
        col("professionalDetails.group").alias("Group"),
        col("personalDetails.dob").alias("M_dob"),
        col("personalDetails.nationality").alias("M_nationality"),
        col("personalDetails.firstname").alias("M_firstname1"),
        col("personalDetails.domicileMedium").alias("M_domicileMedium"),
        col("personalDetails.gender").alias("M_gender"),
        col("personalDetails.maritalStatus").alias("M_maritalStatus"),
        col("personalDetails.category").alias("M_category"),
        col("personalDetails.mobile").alias("M_mobile"),
        col("personalDetails.primaryEmail").alias("M_primaryEmail"),
        col("personalDetails.pincode").alias("M_pincode"),
        col("personalDetails.postalAddress").alias("M_postalAddress1"),
        col("userVerified").alias("NM_verifiedKarmayogi"),
        col("personalDetails.telephone").alias("NM_telephone"),
        col("personalDetails.knownLanguages").alias("NM_knownLanguages[0]"),
        col("academicsDetails"),col("professionalDetails")
      )
      .coalesce(1).distinct()

    fullDetailsDF = fullDetailsDF
      .withColumn("M_postalAddress",replaceNewlines(col("M_postalAddress1")))
      .withColumn("M_firstname", replaceNewlines(col("M_firstname1")))
      .withColumn("NM_nameOfInstitute1", when(col("academicsDetails").isNull && size(col("academicsDetails")) == 0, "").otherwise( col("academicsDetails.nameOfInstitute")))
      .withColumn("NM_yearOfPassing1", when(col("academicsDetails").isNull && size(col("academicsDetails")) == 0, "").otherwise( col("academicsDetails.yearOfPassing")))
      .withColumn("NM_industry1", when(col("professionalDetails").isNull && size(col("professionalDetails")) == 0, "").otherwise( col("professionalDetails.industry")))
      .withColumn("NM_designation1", when(col("professionalDetails").isNull && size(col("professionalDetails")) == 0, "").otherwise( col("professionalDetails.designation")))
      .withColumn("NM_location1", when(col("professionalDetails").isNull && size(col("professionalDetails")) == 0, "").otherwise( col("professionalDetails.location")))
      .withColumn("NM_doj1", when(col("professionalDetails").isNull && size(col("professionalDetails")) == 0, "").otherwise( col("professionalDetails.doj")))
      .withColumn("NM_professionalDetailsDesc", when(col("professionalDetails").isNull && size(col("professionalDetails")) == 0, "").otherwise( col("professionalDetails.description")))
      .withColumn("NM_description1", replaceNewlines(col("NM_professionalDetailsDesc")))
      .withColumn("TestNewLine", lit("2024-05-01-userDetails"))
      .drop("academicsDetails", "professionalDetails","M_postalAddress1", "M_firstname1", "NM_professionalDetailsDesc").distinct()
    fullDetailsDF.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", true).save("/home/analytics/spark_test/mandatoryFields")

        Redis.closeRedisConnect()

  }


}

