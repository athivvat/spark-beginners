package school.machines.chapter3

import org.apache.spark.sql.types.{StringType, StructType, ArrayType}
import org.apache.spark.sql.{Row, SparkSession}

object DataFrameFilter {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("SparkSQLExampleApp")
      .config("spark.master", "local")
      .getOrCreate()

    val arrayStructureData = Seq(
      Row(Row("James","","Smith"),List("Java","Scala","C++"),"OH","M"),
      Row(Row("Anna","Rose",""),List("Spark","Java","C++"),"NY","F"),
      Row(Row("Julia","","Williams"),List("CSharp","VB"),"OH","F"),
      Row(Row("Maria","Anne","Jones"),List("CSharp","VB"),"NY","M"),
      Row(Row("Jen","Mary","Brown"),List("CSharp","VB"),"NY","M"),
      Row(Row("Mike","Mary","Williams"),List("Python","VB"),"OH","M")
    )

    val arrayStructureSchema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("languages", ArrayType(StringType))
      .add("state", StringType)
      .add("gender", StringType)

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)

    df.printSchema()
    df.show()

    // Filter with column condition
    df.filter(df("state") === "OH").show(false)

    // Filter with SQL expression
    df.filter("gender === 'M'").show(false)
  }
}
