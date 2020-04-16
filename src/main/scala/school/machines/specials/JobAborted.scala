package school.machines.specials

import org.apache.spark.sql.SparkSession

object JobAborted {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("JobAborted")
      .config("spark.master", "local")
      .getOrCreate()

    val file = spark.read.textFile("/Users/80094/Workplace/labs/spark-beginners/data/departuredelays.csv")
    file.filter(_.startsWith("banana"))
      .count()
  }
}
