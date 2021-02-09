package com.dsm.delta

import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

object MergeDemo {
  def main(args: Array[String]): Unit = {
    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")

    val spark = SparkSession
      .builder
      .master("local[*]")
      .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
      .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.hadoop.fs.s3a.access.key", s3Config.getString("access_key"))
      .config("spark.hadoop.fs.s3a.secret.key", s3Config.getString("secret_access_key"))
      .appName("DeltaTable Example")
      .getOrCreate()
    spark.sparkContext.setLogLevel(Constants.ERROR)
    import spark.implicits._

    val deltaMergePath = s"s3a://${s3Config.getString("s3_bucket")}/delta_merge_delta"

    val step = "merge"
    step match {
      case "create" =>
        val deltaMergeDf = List(
          ("Brazil",  2011, 22.029),
          ("India", 2006, 24.73)
        ).toDF("country", "year", "temperature")
        deltaMergeDf.show()
        println("Writing data..")
        deltaMergeDf
          .write
          .mode("overwrite")
          .format("delta")
          .save(deltaMergePath)
        println("Write completed!")

      case "merge" =>
        val deltaMergeDf = DeltaTable.forPath(spark, deltaMergePath)
        println("Creating another data,")
        val updatesDf = List(
          ("Australia", 2019, 24.0),
          ("India", 2006, 25.05),
          ("India", 2010, 27.05)
        ).toDF("country", "year", "temperature")
        updatesDf.show()

        println("Merging them both for matching country and year,")
        deltaMergeDf.alias("delta_merge")
          .merge(updatesDf.alias("updates"),"delta_merge.country = updates.country and delta_merge.year = updates.year")
          .whenMatched()
          .updateExpr(Map("temperature" -> "updates.temperature"))
          .whenNotMatched()
          .insertExpr(Map("country" -> "updates.country", "year" -> "updates.year", "temperature" -> "updates.temperature"))
          .execute()

        deltaMergeDf.toDF.show()

    }

    //deltaMergeDf.write.format("delta").save(deltaMergePath)

    spark.stop()
  }
}
