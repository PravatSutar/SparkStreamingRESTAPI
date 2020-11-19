package com.pravat.sutar.analytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ col, from_json }
import org.apache.spark.sql.types.{ IntegerType, StringType, StructType }

import org.apache.spark.sql.types.StructField
import pack.APIGatewayInvoker

object StreamingMain {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[3]").appName("SparkExampleByPravat").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val sfcScoringDS = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "kafkaouttopic")
      .option("startingOffsets", "earliest")
      .option("max.poll.records", 10)
      .option("failOnDataLoss", false)
      .load()

    sfcScoringDS.printSchema()
    import spark.implicits._
    
    val sfcScoringSchema = StructType(Seq(
      StructField("CLIENT", StringType, true),
      StructField("TOKEN_ID", StringType, true),
      StructField("RISK_SCORE", StringType, true),
      StructField("CATEG", StringType, true),
      StructField("REASON1", StringType, true),
      StructField("FLAG", StringType, true),
      StructField("RECORD_ID", StringType, true)))

    val sfcScoringData = sfcScoringDS.selectExpr("cast (value as string) as json")
      .select(from_json($"json", schema = sfcScoringSchema)
        .as("sfcScoringJsonData"))
    val sfcScoringDF = sfcScoringData.select(col("sfcScoringJsonData.*"))
   
    val responseCode = APIGatewayInvoker.sendSFCOutput(sfcScoringDF)
    println("Data send to the API and response code is:", responseCode)

    sfcScoringDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }
}