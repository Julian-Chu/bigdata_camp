package com.julianchu.sparksql
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules._

case class MyPushDown(spark: SparkSession) extends Rule[LogicalPlan] {
  logWarning("MyPushDown Start")

  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions  {
//    case default => default
    case Multiply(left, right, failOnError) if right.isInstanceOf[Literal]
      && right.asInstanceOf[Literal].value.asInstanceOf[Int] == 1 => left

    case Multiply(left, right, failOnError) if left.isInstanceOf[Literal]
      && left.asInstanceOf[Literal].value.asInstanceOf[Int] == 1 => right
  }
}

class MySparkSessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule { session => new MyPushDown(session)
    }
  }
}
