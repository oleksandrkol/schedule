import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.functions._

object CreateOutputData {

  val coder: (String => String) = (arg: String) => {
    val input = arg
    val format = "yyyy-MM-dd"

    val df: SimpleDateFormat = new SimpleDateFormat(format)
    val date: Date = df.parse(input)

    val cal: Calendar = Calendar.getInstance()
    val s = cal.setTime(date)
    "W" + cal.get(Calendar.WEEK_OF_YEAR): String
  }

  val sqlfunc = udf(coder)

  def main(args: Array[String]) {

    Environment.setup()

    val fileHandler = new FileHandler
    val scheduleDataFrame = fileHandler.read(Environment.fileSchedule)

    // Task 1
    scheduleDataFrame.groupBy("DEST").count()
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(Environment.fileTemp)

    fileHandler.merge(Environment.fileTemp, Environment.fileTask1)

    // Task 2
    val select1 = scheduleDataFrame.groupBy("DEST").count()
      .withColumnRenamed("DEST", "AIRP")
      .withColumnRenamed("count", "d_count")
      .coalesce(1).cache()
    val select2 = scheduleDataFrame.groupBy("ORIGIN").count()
      .withColumnRenamed("ORIGIN", "AIRP")
      .withColumnRenamed("count", "o_count")
      .coalesce(1).cache()

    select1.join(select2, "AIRP").select(select1("AIRP"), (select1("d_count") - select2("o_count")).as("delta"))
      .filter("delta <> 0").coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(Environment.fileTemp)

    fileHandler.merge(Environment.fileTemp, Environment.fileTask2)

    // Task 3
    scheduleDataFrame.withColumn("WEEK_OF_YEAR", sqlfunc(col("FL_DATE")))
      .groupBy("DEST", "WEEK_OF_YEAR").count()
      .orderBy("WEEK_OF_YEAR")
      .select("WEEK_OF_YEAR", "DEST", "count")
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(Environment.fileTemp)

    fileHandler.merge(Environment.fileTemp, Environment.fileTask3)

  }
}
