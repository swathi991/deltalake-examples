package com.dsm.delta

import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

object DeletionDemo {
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

    val deltaTablePath = s"s3a://${s3Config.getString("s3_bucket")}/schema_enforcement_delta"

    println("Reading data,")
    val deltaDf = DeltaTable.forPath(spark, deltaTablePath)
    deltaDf.toDF.show()

    println("Deleting record where country = 'India' ..")
    deltaDf.delete("country = 'India'")

    println("Reading updated data,")
    deltaDf.toDF.show()

    spark.stop()
  }
}
