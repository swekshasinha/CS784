import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.Encoders

case class LastMinusFirst(var first: Double, var last: Double)

object LastMinusFirstAggregator extends Aggregator[schema, LastMinusFirst, Double] {
  def zero: LastMinusFirst = LastMinusFirst(Double.MaxValue, Double.MinValue)
  def reduce(buffer: LastMinusFirst, value: Double): LastMinusFirst = {
    if (buffer.first == Double.MaxValue) buffer.first = value
    buffer.last = value
    buffer
  }
  def merge(buffer1: LastMinusFirst, buffer2: LastMinusFirst): LastMinusFirst = {
    val merged = LastMinusFirst(Math.min(buffer1.first, buffer2.first), Math.max(buffer1.last, buffer2.last))
    merged
  }
  def finish(buffer: LastMinusFirst): Double = buffer.last - buffer.first
  def bufferEncoder: Encoder[LastMinusFirst] = Encoders.product[LastMinusFirst]
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}