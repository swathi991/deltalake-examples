package com.dsm.delta

import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

object SchemaEnforcementDemo {
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

    val deltaTablePath = s"s3a://${s3Config.getString("s3_bucket")}/schema_enforcement_delta"

    val step = "append"
    step match {
      case "overwrite" =>
        val data = List(
            ("Brazil",  2011, 22.029),
            ("India", 2006, 24.73)
          )
          .toDF("country", "year", "temperature")
        data.printSchema()
        data.show()
        println("Writing data..")
        data
          .coalesce(1)
          .write
          .format("delta")
          .mode("overwrite")
          .save(deltaTablePath)
        println("Write completed!")

        println("Reading data,")
        DeltaTable.forPath(spark, deltaTablePath).toDF.show()

      case "append" =>
        val newData = List(
          ("Australia", 2019.0, 30.0)
        ).toDF("country", "year", "temperature")
        newData.printSchema()
        newData.show()
        println("Writing data..")
        newData
          .write
          .format("delta")
          .mode("append")
          .save(deltaTablePath)
      }
      spark.stop()
  }
}
