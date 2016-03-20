import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

class FileHandler {

  val conf = new SparkConf()
    .setAppName("Schedule")
    .setMaster("local")

  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  def read(srcPath: String): DataFrame = {
    val inputFilePath = getClass.getClassLoader.getResource(srcPath).getPath

    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(inputFilePath).cache()
  }

  def merge(srcPath: String, dstPath: String): Unit =  {
    FileUtil.fullyDelete(new File(dstPath))
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
    FileUtil.fullyDelete(new File(srcPath))
  }
}
