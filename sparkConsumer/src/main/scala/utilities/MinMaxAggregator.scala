package utilities

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class MinMaxAggregator extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", DoubleType) :: Nil)

  // Define the intermediate data type
  override def bufferSchema: StructType = StructType(
    StructField("min", DoubleType) ::
    StructField("max", DoubleType) :: Nil
  )

  // Define the output data type
  override def dataType: DataType = StructType(
    StructField("min", DoubleType) ::
    StructField("max", DoubleType) :: Nil
  )

  // Define whether this UDAF is deterministic or not (in this case, it is)
  override def deterministic: Boolean = true

  // Define the initial value for the buffer schema
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Double.MaxValue // Init min = double.max
    buffer(1) = Double.MinValue // Init max = double.min
  }

  // Define how to update the buffer schema based on each new input value
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val currentValue = input.getDouble(0)//????
    buffer(0) = Math.min(buffer.getDouble(0), currentValue)
    buffer(1) = Math.max(buffer.getDouble(1), currentValue)
  }

  // Define how to merge two buffer schemas
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = Math.min(buffer1.getDouble(0), buffer2.getDouble(0))
    buffer1(1) = Math.max(buffer1.getDouble(1), buffer2.getDouble(1))
  }
  
  // Define how to generate the final output value from the buffer schema
  override def evaluate(buffer: Row): Any = {
    Row(buffer.getDouble(0), buffer.getDouble(1))
  }
}