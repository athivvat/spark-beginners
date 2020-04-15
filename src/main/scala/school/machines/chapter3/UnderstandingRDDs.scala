package school.machines.chapter3

import org.apache.spark.sql.SparkSession

object UnderstandingRDDs {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("UnderstandingRDDs")
      .config("spark.master", "local")
      .getOrCreate()

    val csvFile = "/Users/80094/Workplace/labs/spark-beginners/data/departuredelays.csv"
    val cancerRDD = spark.sparkContext.textFile(csvFile)
    println(cancerRDD.partitions.size)
  }
}
