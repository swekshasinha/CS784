package processing

import baseline.SparkStructuredStreaming
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import schema.CryptoSchema
import utilities.{GeometricMean, HarmonicMean}

object SparkRealTimePriceUpdates {

  def main(args: Array[String]): Unit = {
    StreamingRealTimePriceUpdates("Real-time Price Updates")
  }
}

class StreamingRealTimePriceUpdates(appName: String)
  extends SparkStructuredStreaming(appName: String) {

  val inputDF: DataFrame = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .load()
    print("=============inputDF")
    Dataframe.collect.foreach(println)

  val parsedDF: DataFrame = inputDF.select(
      from_json( col("value").cast("string"), CryptoSchema.schema)
      .as("cryptoUpdate"))
      .select("cryptoUpdate.*")
      print("=============parsedtDF")
      Dataframe.collect.foreach(println)

  val castedDF: DataFrame = parsedDF
    .printSchema();
    .withColumn("price", parsedDF("price").cast("double"))

  val queryPrice: StreamingQuery = castedDF
    .printSchema();
    .writeStream
    .foreachBatch { (batchDF: DataFrame, _: Long) =>
      batchDF
        .show()
        .filter(col("symbol_coin").isNotNull).show(false)
        .write
        .cassandraFormat("realtime_prices", "crypto_updates")
        .mode("append")
        .save()
    }
    .outputMode("update")
    .start()

  val queryPriceConsole: StreamingQuery = castedDF
    .printSchema();
    .writeStream
    .trigger(Trigger.ProcessingTime(interval = "1 second"))
    .mode("append")
    .forat( source = "console")
    .outputMode("update")
    .start()
    print("=============query proce comsole  ======F")

  val geo_mean: GeometricMean.type = GeometricMean
  val har_mean: HarmonicMean.type = HarmonicMean

  val windowedDF: DataFrame = castedDF
    .withWatermark("timestamp", WATERMARK_THRESHOLD)
    .groupBy(
      window(col("timestamp"), WINDOW_DURATION, SLIDE_DURATION),
      col("symbol_coin"))
    .agg(mean(col("price")).as("arithmetic_mean"),
         geo_mean(col("price")).as("geometric_mean"),
         har_mean(col("price")).as("harmonic_mean"))
    .withColumn("start_time",col("window").getField("start"))
    .withColumn("end_time",col("window").getField("end"))
    .drop("window")

  val queryAggregate: StreamingQuery = windowedDF
    .writeStream
    .foreachBatch { (batchDF: DataFrame, _: Long) =>
      batchDF
        .show()
        .filter(col("symbol_coin").isNotNull).show(false)
        .write
        .cassandraFormat("rolling_aggregates", "crypto_updates")
        .mode("append")
        .save()
    }
    .outputMode("update")
    .start()
    print("=============qery agggrreead===DF")

  spark.streams.awaitAnyTermination()

}

object StreamingRealTimePriceUpdates{
  def apply(appName: String): StreamingRealTimePriceUpdates =
    new StreamingRealTimePriceUpdates(appName)
}
