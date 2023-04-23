import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object CryptoSchema {
 val schema: StructType = new StructType()
   .add("name_coin", DataTypes.StringType )
   .add("symbol_coin", DataTypes.StringType)
   .add(name="id", DataTypes.LongType)
   .add(name="uuid", DataTypes.StringType)
   .add("number_of_markets", DataTypes.LongType)
   .add("volume", DataTypes.StringType)
   .add("market_cap", DataTypes.StringType)
   .add("total_supply", DataTypes.DoubleType)
   //price is casted separately to Double from String
   .add("price", DataTypes.StringType)
   .add("percent_change_24hr", DataTypes.StringType)
   .add("timestamp", DataTypes.TimestampType)
}

val inputDF: DataFrame = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "127.0.0.1:9092").option("subscribe", "crypto_topic").load()
val parsedDF: DataFrame = inputDF.withColumn("value",col("value").cast(DataTypes.StringType)).select(from_json( col("value"), CryptoSchema.schema).as("cryptoUpdate")).select("cryptoUpdate.*")

val printQuery = parsedDF.writeStream.outputMode("append").format("console").start()