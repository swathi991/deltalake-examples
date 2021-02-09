package com.dsm.df.limitations

import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object SchemaEnforcementDemo {
  def main(args: Array[String]): Unit = {
    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")

    val spark = SparkSession
      .builder
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.access.key", s3Config.getString("access_key"))
      .config("spark.hadoop.fs.s3a.secret.key", s3Config.getString("secret_access_key"))
      .appName("Dataframe Example")
      .getOrCreate()
    spark.sparkContext.setLogLevel(Constants.ERROR)
    import spark.implicits._

    val dfPath = s"s3a://${s3Config.getString("s3_bucket")}/schema_enforcement"

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
          .coalesce (1)
          .write.format ("parquet")
          .mode ("overwrite")
          .save (dfPath)
        println("Write completed!")

        println("Reading data,")
        spark.read.parquet(dfPath).show()

      case "append" =>
        val newData = List(
          ("Australia", 2019.0, 30.0)
        ).toDF("country", "year", "temperature")
        newData.printSchema()
        newData.show()
        println("Writing data..")
        newData
          .coalesce (1)
          .write.format ("parquet")
          .mode ("append")
          .save (dfPath)
        println("Write completed!")

        println("Reading data,")
        spark.read.parquet(dfPath).show()
    }
    spark.stop()
  }
}
