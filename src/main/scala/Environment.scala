import java.io.File
import org.apache.hadoop.fs.FileUtil

object Environment {

  val fileSchedule = "planes_log.csv.gz"
  val fileTemp = "src/main/resources/tmp/temp.csv"
  val fileTask1= "src/main/resources/output/task1.csv"
  val fileTask2= "src/main/resources/output/task2.csv"
  val fileTask3= "src/main/resources/output/task3.csv"


  def setup () {
    System.setProperty("hadoop.home.dir", "C:\\\\hadoop\\\\")

    FileUtil.fullyDelete(new File("src/main/resources/tmp/"))
    FileUtil.fullyDelete(new File("src/main/resources/output/"))

  }
}
