import org.apache.spark.SparkConf
import org.apache.spark.{rdd, SparkContext, SparkConf}
import org.apache.spark.sql._

/**
  * Created by rutvikparmar on 21/01/16.
  */
object ReadParquet {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("ReadParquet").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val df = sqlContext.read.parquet("/Users/rutvikparmar/Sparksample/out.parquet")

    println(s"Edits so far - ${df.count}\n")

    df.show()
  }
}
