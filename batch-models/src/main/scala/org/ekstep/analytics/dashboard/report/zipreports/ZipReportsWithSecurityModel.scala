package org.ekstep.analytics.dashboard.report.zipreports

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import net.lingala.zip4j.ZipFile
import net.lingala.zip4j.model.ZipParameters
import net.lingala.zip4j.model.enums.EncryptionMethod
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import java.io.{File}
import java.nio.file.{Files, Paths, StandardCopyOption}


/**
 * Model for processing the operational reports into a single password protected zip file
 */
object ZipReportsWithSecurityModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.ZipReportsWithSecurityModel"
  override def name() = "ZipReportsWithSecurityModel"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    // Start of merging folders

    // Define variables for source, destination directories and date.
    val prefixDirectoryPath = conf.prefixDirectoryPath
    val destinationDirectoryPath = conf.destinationDirectoryPath
    val specificDate = getDate()

    // Method to traverse all the report folders within the source folder and check for specific date folder
    def traverseDirectory(directory: File): Unit = {
      // Get the list of files and directories in the current directory
      val files = directory.listFiles()
      if (files != null) {
        // Iterate over each file or directory
        for (file <- files) {
          // If it's a directory, recursively traverse it
          if (file.isDirectory) {
            // Check if the current directory contains the specific date folder
            val dateFolder = new File(file, specificDate)
            if (dateFolder.exists() && dateFolder.isDirectory) {
              // Inside the specific date folder, iterate over mdoid folders
              traverseDateFolder(dateFolder)
            }
          }
        }
      }
    }

    // Method to traverse the date folder and its subdirectories in each report folder
    def traverseDateFolder(dateFolder: File): Unit = {
      // Get the list of files and directories in the date folder
      val files = dateFolder.listFiles()
      if (files != null) {
        // Iterate over mdoid folders
        for (mdoidFolder <- files if mdoidFolder.isDirectory) {
          // Inside each mdoid folder, copy all CSV files to the destination directory
          copyFiles(mdoidFolder, destinationDirectoryPath)
        }
      }
    }

    // Method to copy all CSV files inside an mdoid folder to the destination directory
    def copyFiles(mdoidFolder: File, destinationDirectoryPath: String): Unit = {
      // Create destination directory if it does not exist
      val destinationDirectory = Paths.get(destinationDirectoryPath, mdoidFolder.getName)
      if (!Files.exists(destinationDirectory)) {
        Files.createDirectories(destinationDirectory)
      }
      // Get the list of CSV files in the mdoid folder
      val csvFiles = mdoidFolder.listFiles()
      if (csvFiles != null) {
        // Copy all CSV files to the destination directory
        for (csvFile <- csvFiles) {
          val destinationFile = Paths.get(destinationDirectory.toString, csvFile.getName)
          Files.copy(csvFile.toPath, destinationFile, StandardCopyOption.REPLACE_EXISTING)
          println(s"File ${csvFile.getName} copied to ${destinationFile.toAbsolutePath}")
        }
      }
    }

    // Start traversing the source directory
    traverseDirectory(new File(prefixDirectoryPath))

    // End of merging folders

    // Start of zipping the reports and syncing to blob store
    // Define variables for source, blobStorage directories and password.
    val mdoidDirectoryPath = conf.mdoidDirectoryPath
    val password = conf.password

    // Traverse through source directory to create individual zip files (mdo-wise)
    val mdoidFolders = new File(mdoidDirectoryPath).listFiles()
    if (mdoidFolders != null) {
      mdoidFolders.foreach { mdoidFolder =>
        if (mdoidFolder.isDirectory) { // Check if it's a directory
          val zipFilePath = s"${mdoidFolder.getAbsolutePath}/reports.zip"

          // Create a password-protected zip file for the mdoid folder
          val zipFile = new ZipFile(zipFilePath, password.toCharArray)
          val parameters = new ZipParameters()                             
          parameters.setEncryptFiles(true)
          parameters.setEncryptionMethod(EncryptionMethod.ZIP_STANDARD)
          // Add all files within the mdoid folder to the zip file
          mdoidFolder.listFiles().foreach { file =>
            zipFile.addFile(file, parameters)
          }
          // Delete the csvs keeping only the zip file from mdo folders
          mdoidFolder.listFiles().foreach { file =>
            if (file.getName.toLowerCase.endsWith(".csv")) {
              file.delete()
            }
          }
          // Upload the zip file to blob storage
          val mdoid = mdoidFolder.getName

          syncReports(zipFilePath, zipFilePath)
          println(s"Password-protected ZIP file created and uploaded for $mdoid: $zipFilePath")
        }
      }
    } else {
      println("No mdoid folders found in the given directory.")
    }
    // End of zipping the reports and syncing to blob store
  }
}