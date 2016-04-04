import org.apache.spark.SparkConf
import org.apache.spark.{rdd, SparkContext, SparkConf}
import org.apache.spark.sql._
import com.databricks.spark.csv._
/**
  * Created by rutvikparmar on 21/01/16.
  */
object ReadParquet {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("ReadParquet").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val df = sqlContext.read.parquet("/Users/rutvikparmar/Sparksample/out.parquet")

    println(s"Edits so far - ${df.count}\n")

    val lrDF = df.select("refererpage","locationpage")

    val lrRDD = lrDF.rdd.map{ row => (row(0).asInstanceOf[String], row(1).asInstanceOf[String]) }

    val cooccurs = lrRDD.map(p => (p, 1)).reduceByKey(_+_)

    val referer_location_weight = cooccurs.map{ case ((r,l),v) => (r,l,v)}

    //val referer = lrRDD.map{ case (r,l) => r}
    //val location = lrRDD.map{ case (r,l) => l}

    val clickstreamDF = referer_location_weight.map(x => RLW(x._1,x._2,x._3)).toDF()

    clickstreamDF.show()
    println(s"Pages Viewed - ${clickstreamDF.count()}\n")
    clickstreamDF.write.parquet("clickstream.parquet")
  }
}

case class RLW(referer: String, location: String, weight: Int)
