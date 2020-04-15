package school.machines.chapter4

import org.apache.spark.sql.SparkSession

object BasicQuery {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("SparkSQLExampleApp")
      .config("spark.master", "local")
      .getOrCreate()

    // Path to your dataset
    val csvFile = "/Users/80094/Workplace/labs/spark-beginners/data/departuredelays.csv"

    // Read and crate a temporary view
    // Infer schema. Note that for larger files you may want to specify schema
    val df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csvFile)

    // Create a temporary view
    df.createOrReplaceTempView("us_delay_flights_tbl")

    spark.sql(
      """SELECT distance, origin, destination,
        |CASE
        |WHEN delay > 360 THEN 'Very Long Delays'
        |WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
        |WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
        |WHEN delay > 0 AND delay < 60 THEN 'Tolerable Delays'
        |WHEN delay = 0 THEN 'NO Delays'
        |ELSE 'Early'
        |END AS Flight_Delays
        |FROM us_delay_flights_tbl
        |ORDER BY distance DESC
        |""".stripMargin).show(20)
  }
}
