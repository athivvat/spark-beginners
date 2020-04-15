package school.machines.chapter4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, max, min, row_number, sum}

object WindowFunctions {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("SparkSQLExampleApp")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val simpleData = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    )
    val df = simpleData.toDF("employee_name", "department", "salary")
    df.show()

    // row_number
    val windowSpec  = Window.partitionBy("department").orderBy("salary")

    // Aggregate Functions
    val windowSpecAgg = Window.partitionBy("department")
    val aggDF = df.withColumn("row", row_number.over(windowSpec))
      .withColumn("avg", avg(col("salary")).over(windowSpecAgg))
      .withColumn("sum", sum(col("salary")).over(windowSpecAgg))
      .withColumn("min", min(col("salary")).over(windowSpecAgg))
      .withColumn("max", max(col("salary")).over(windowSpecAgg))
      .where(col("row")===1)
      .select("department", "avg", "sum", "min", "max")
      .show()
  }
}
