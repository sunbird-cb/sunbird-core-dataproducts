package org.ekstep.analytics.adhoc

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.dashboard.{DashboardConfig}
import org.ekstep.analytics.dashboard.DashboardUtil._

import java.io.Serializable
import scala.collection.mutable.ListBuffer


object DataUtilUserDetails extends Serializable {

  /*
  organizes schema def at one place
   */
  object Schema extends Serializable {

    /* schema definitions for user profile details */
    val profileCompetencySchema: StructType = StructType(Seq(
      StructField("id",  StringType, nullable = true),
      // StructField("name",  StringType, nullable = true),
      StructField("status",  StringType, nullable = true),
      StructField("competencyType",  StringType, nullable = true),
      StructField("competencySelfAttestedLevel",  StringType, nullable = true), // this is sometimes an int other times a string
      StructField("competencySelfAttestedLevelValue",  StringType, nullable = true)
    ))
    val professionalDetailsSchema: StructType = StructType(Seq(
      StructField("designation", StringType, nullable = true),
      StructField("group", StringType, nullable = true),
      StructField("organisationType", StringType, nullable = true),
      StructField("nameOther", StringType, nullable = true),
      StructField("industry", StringType, nullable = true),
      StructField("industryOther", StringType, nullable = true),
      StructField("designationOther", StringType, nullable = true),
      StructField("location", StringType, nullable = true),
      StructField("locationOther", StringType, nullable = true),
      StructField("responsibilities", StringType, nullable = true),
      StructField("doj", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("completePostalAddress", StringType, nullable = true),
      StructField("additionalAttributes", StringType, nullable = true)
    ))
    val personalDetailsSchema: StructType = StructType(Seq(
      StructField("phoneVerified", StringType, nullable = true),
      StructField("gender", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("mobile", StringType, nullable = true),
      StructField("primaryEmail", StringType, nullable = true),
      StructField("firstname", StringType, nullable = true),
      StructField("middlename", StringType, nullable = true),
      StructField("surname", StringType, nullable = true),
      StructField("dob", StringType, nullable = true),
      StructField("nationalIdentifier", StringType, nullable = true),
      StructField("nationality", StringType, nullable = true),
      StructField("officialEmail", StringType, nullable = true),
      StructField("personalEmail", StringType, nullable = true),
      StructField("countryCode", StringType, nullable = true),
      StructField("telephone", StringType, nullable = true),
      StructField("maritalStatus", StringType, nullable = true),
      StructField("postalAddress", StringType, nullable = true),
      StructField("pincode", StringType, nullable = true),
      StructField("domicileMedium", StringType, nullable = true),
      StructField("knownLanguages", StringType, nullable = true)
    ))
    val additionalPropertiesSchema: StructType = StructType(Seq(
      StructField("tag", ArrayType(StringType), nullable = true),
      StructField("externalSystemId", StringType, nullable = true),
      StructField("externalSystem", StringType, nullable = true)
    ))
    val employmentDetailsSchema: StructType = StructType(Seq(
      StructField("service",StringType, nullable = true),
      StructField("cadre",StringType, nullable = true),
      StructField("allotmentYearOfService",StringType, nullable = true),
      StructField("dojOfService",StringType, nullable = true),
      StructField("payType",StringType, nullable = true),
      StructField("civilListNo",StringType, nullable = true),
      StructField("employeeCode",StringType, nullable = true),
      StructField("officialPostalAddress",StringType, nullable = true),
      StructField("pinCode",StringType, nullable = true),
      StructField("departmentName",StringType, nullable = true)
    ))
    val academicDetailsSchema: StructType = StructType(Seq(
      StructField("type",StringType, nullable = true),
      StructField("nameOfQualification",StringType, nullable = true),
      StructField("nameOfInstitute",StringType, nullable = true),
      StructField("yearOfPassing",StringType, nullable = true)
    ))
    val skillDetailsSchema: StructType = StructType(Seq(
      StructField("additionalSkills",StringType, nullable = true),
      StructField("certificateDetails",StringType, nullable = true)
    ))
    val interestDetailsSchema: StructType = StructType(Seq(
      StructField("professional", ArrayType(StringType), nullable = true),
      StructField("hobbies", ArrayType(StringType), nullable = true)
    ))

    def makeProfileDetailsSchema(competencies: Boolean = false, additionalProperties: Boolean = false, professionalDetails: Boolean = false, allProfileDeatils: Boolean = false): StructType = {
      val fields = ListBuffer(
        StructField("verifiedKarmayogi", BooleanType, nullable = true),
        StructField("mandatoryFieldsExists", BooleanType, nullable = true),
        StructField("personalDetails", personalDetailsSchema, nullable = true)
      )
      if (competencies) {
        fields.append(StructField("competencies", ArrayType(profileCompetencySchema), nullable = true))
      }
      if (additionalProperties) {
        fields.append(StructField("additionalProperties", additionalPropertiesSchema, nullable = true))
        fields.append(StructField("additionalPropertis", additionalPropertiesSchema, nullable = true))
      }
      if (professionalDetails) {
        fields.append(StructField("professionalDetails", ArrayType(professionalDetailsSchema), nullable = true))
      }
      if(allProfileDeatils){
        fields.append(StructField("employmentDetails", employmentDetailsSchema, nullable = true))
        fields.append(StructField("academics", ArrayType(academicDetailsSchema), nullable = true))
        fields.append(StructField("skills", skillDetailsSchema, nullable = true))
        fields.append(StructField("interests", interestDetailsSchema, nullable = true))

      }
      StructType(fields)
    }

  def timestampStringToLong(df: DataFrame, cols: Seq[String], format: String = "yyyy-MM-dd HH:mm:ss:SSSZ"): DataFrame = {
    var resDF = df
    cols.foreach(c => {
      resDF = resDF.withColumn(c, to_timestamp(col(c), format))
        .withColumn(c, col(c).cast("long"))
    })
    resDF
  }

  def orgDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    var orgDF = cache.load("org")
      .select(
        col("id").alias("orgID"),
        col("orgname").alias("orgName"),
        col("status").alias("orgStatus"),
        col("createddate").alias("orgCreatedDate"),
        col("organisationtype").alias("orgType"),
        col("organisationsubtype").alias("orgSubType")
      ).na.fill("", Seq("orgName"))

    orgDF = timestampStringToLong(orgDF, Seq("orgCreatedDate"))

    show(orgDF, "orgDataFrame")

    orgDF
  }

  def userDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val profileDetailsSchema = Schema.makeProfileDetailsSchema(additionalProperties = true, professionalDetails = true, allProfileDeatils=true)

    var userDF =
      cache.load("user")
        .select(
          col("id").alias("userID"),
          col("rootorgid").alias("userOrgID"),
          col("profiledetails").alias("userProfileDetails"),
          col("updateddate").alias("userUpdatedTimestamp")
        )
        .na.fill("", Seq("userOrgID"))
        .na.fill("{}", Seq("userProfileDetails"))
        .withColumn("profileDetails", from_json(col("userProfileDetails"), profileDetailsSchema))
        .withColumn("personalDetails", col("profileDetails.personalDetails"))
        .withColumn("professionalDetails", explode_outer(col("profileDetails.professionalDetails")))
        .withColumn("userVerified", when(col("profileDetails.verifiedKarmayogi").isNull, false).otherwise(col("profileDetails.verifiedKarmayogi")))
        .withColumn("userMandatoryFieldsExists", col("profileDetails.mandatoryFieldsExists"))
        .withColumn("userPhoneVerified", expr("LOWER(personalDetails.phoneVerified) = 'true'"))
        .withColumn("employmentDetails", col("profileDetails.employmentDetails"))
        .withColumn("academicsDetails", explode_outer(col("profileDetails.academics")))
        .withColumn("skillDetails", col("profileDetails.skills")).withColumn("interestDetails", col("profileDetails.interests"))

    userDF = userDF
      .withColumn("additionalProperties",
        if (userDF.columns.contains("profileDetails.additionalPropertis")) {
          col("profileDetails.additionalPropertis")
        } else {
          col("profileDetails.additionalProperties")
        })
      .drop("profileDetails", "userProfileDetails")

    val arrayDF = userDF.select(col("personalDetails"),col("professionalDetails"))
    arrayDF.show(false)
    show(userDF, "userDataFrame")

    userDF
  }

  def userDetailsDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val profileDetailsSchema = Schema.makeProfileDetailsSchema(additionalProperties = true, professionalDetails = true, allProfileDeatils=true)
    var userDF = cache.load("user")
      .select(
        col("id").alias("userID"),
        col("firstname").alias("firstName"),
        col("lastname").alias("lastName"),
        col("maskedemail").alias("maskedEmail"),
        col("maskedphone").alias("maskedPhone"),
        col("rootorgid").alias("userOrgID"),
        col("status").alias("userStatus"),
        col("profiledetails").alias("userProfileDetails"),
        col("createddate").alias("userCreatedTimestamp"),
        col("updateddate").alias("userUpdatedTimestamp"),
        col("createdby").alias("userCreatedBy")
      )
      .na.fill("", Seq("userOrgID", "firstName", "lastName"))
      .na.fill("{}", Seq("userProfileDetails"))
      .withColumn("profileDetails", from_json(col("userProfileDetails"), profileDetailsSchema))
      .withColumn("personalDetails", col("profileDetails.personalDetails"))
      .withColumn("professionalDetails", explode_outer(col("profileDetails.professionalDetails")))
      .withColumn("userVerified", when(col("profileDetails.verifiedKarmayogi").isNull, false).otherwise(col("profileDetails.verifiedKarmayogi")))
      .withColumn("userMandatoryFieldsExists", col("profileDetails.mandatoryFieldsExists"))
      .withColumn("userPhoneVerified", expr("LOWER(personalDetails.phoneVerified) = 'true'"))
      .withColumn("fullName", concat_ws(" ", col("firstName"), col("lastName"))).withColumn("employmentDetails", col("profileDetails.employmentDetails"))
      .withColumn("academicsDetails", explode_outer(col("profileDetails.academics")))
      .withColumn("skillDetails", col("profileDetails.skills")).withColumn("interestDetails", col("profileDetails.interests"))

    userDF = userDF
      .withColumn("additionalProperties",
        if (userDF.columns.contains("profileDetails.additionalPropertis")) {
          col("profileDetails.additionalPropertis")
        } else {
          col("profileDetails.additionalProperties")
        })
      .drop("profileDetails", "userProfileDetails")

    userDF = timestampStringToLong(userDF, Seq("userCreatedTimestamp", "userUpdatedTimestamp"))
    show(userDF, "userDataFrame")

    userDF
  }

  def userOrgDataFrame(orgDF: DataFrame, userDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {

    val joinOrgDF = orgDF.select(
      col("orgID").alias("userOrgID"),
      col("orgName").alias("userOrgName"),
      col("orgStatus").alias("userOrgStatus"),
      col("orgCreatedDate").alias("userOrgCreatedDate"),
      col("orgType").alias("userOrgType"),
      col("orgSubType").alias("userOrgSubType")
    )

    val df = userDF.join(joinOrgDF, Seq("userOrgID"), "left")
    show(df, "userOrgDataFrame")

    df
  }

  def getOrgUserDataFrames()(implicit spark: SparkSession, conf: DashboardConfig): (DataFrame, DataFrame, DataFrame) = {
    // obtain and save user org data
    val orgDF = orgDataFrame()
    val userDF = userDetailsDataFrame()
    val userOrgDF = userOrgDataFrame(orgDF, userDF)
    // validate userDF and userOrgDF counts
    validate({userDF.count()}, {userOrgDF.count()}, "userDF.count() should equal userOrgDF.count()")

    (orgDF, userDF, userOrgDF)
  }

}}
