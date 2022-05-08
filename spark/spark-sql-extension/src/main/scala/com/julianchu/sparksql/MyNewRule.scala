package com.julianchu.sparksql
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types.Decimal

case class MyNewRule(spark: SparkSession) extends Rule[LogicalPlan] {
  logWarning("MyNewRule Start")

  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions  {
    case Multiply(left, right, failOnError) if right.isInstanceOf[Literal] &&
      right.asInstanceOf[Literal].value.isInstanceOf[Decimal] &&
      right.asInstanceOf[Literal].value.asInstanceOf[Decimal].toDouble == 1.0 => left
  }
}

class MySparkSessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule { session => new MyNewRule(session)
    }
  }
}
