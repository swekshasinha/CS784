package schema

import org.apache.spark.sql.types.{DataTypes, StructType}

object CryptoSchema {
  val schema: StructType = new StructType().add("name_coin", DataTypes.StringType ).add("symbol_coin", DataTypes.StringType).add(name="id", DataTypes.LongType).add(name="uuid", DataTypes.StringType).add("number_of_markets", DataTypes.LongType).add("volume", DataTypes.StringType).add("market_cap", DataTypes.StringType).add("total_supply", DataTypes.DoubleType).add("price", DataTypes.StringType).add("percent_change_24hr", DataTypes.StringType).add("timestamp", DataTypes.TimestampType)
    //price is casted separately to Double from String
}