import java.io._
import kafka.serializer.DefaultDecoder
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{DecoderFactory, Decoder}
import org.apache.avro.util.Utf8
import org.apache.spark.sql._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.google.common.hash.Hashing


/**
  * Created by rutvikparmar on 16/01/16.
  */
object SparkStreaming {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: SparkStreaming <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args

    val file = new File("Click.xml")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.append("<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:y=\"http://www.yworks.com/xml/graphml\" xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">\n            <key for=\"node\" id=\"d0\" attr.type=\"string\"/>\n            <graph id=\"Graph\" edgedefault=\"directed\">")
    bw.newLine()


    // Create context with 10 second batch interval
    val sparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
      ssc, kafkaParams, topicsSet)

    var location: String = null
    var referer: String = null
    var eventType: String = null
    var sessionId: String = null
    var partyId: String = null

   
    val lines = messages.map(_._2)
    lines.foreachRDD{rdd =>
      rdd.collect.foreach{t =>

        val genericRecord: GenericRecord = byteArrayToDatum(getSchema, t)

        location = getValue(genericRecord, "locationpage", classOf[String])
        referer = getValue(genericRecord, "refererpage", classOf[String])
        sessionId = getValue(genericRecord, "sessionId", classOf[String])
        partyId = getValue(genericRecord, "partyId", classOf[String])
        eventType = getValue(genericRecord, "eventType", classOf[String])

        println(s"SessionId :  $sessionId")
        println(s"PartyId :  $partyId")
        println(s"Referer :  $referer")
        println(s"Location :  $location")
        println(s"EventType : $eventType")

        val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)

        import sqlContext.implicits._

        val clickstreamRDD = sqlContext.sparkContext.parallelize(List(Event(sessionId,referer,location,eventType,partyId)))
        val clickstreamDF = clickstreamRDD.toDF()

        clickstreamDF.write.mode(SaveMode.Append).parquet("out.parquet")

        val nodesXml =
            <node id ={location.toString()}></node>

        val edge = Array(referer,location)

        val edgesXml =
             <edge source={ edge(0) } target={ edge(1) }/>

        bw.append(nodesXml.toString())
        bw.newLine()
        bw.append(edgesXml.toString())
        bw.newLine()



      }
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()

    bw.write("</graph>\n          </graphml>")
    bw.close()
  }

  def byteArrayToDatum(schema: Schema, byteData: Array[Byte]): GenericRecord = {
    val reader: GenericDatumReader[GenericRecord] = new GenericDatumReader[GenericRecord](schema)
    var byteArrayInputStream: ByteArrayInputStream = null
    try {
      byteArrayInputStream = new ByteArrayInputStream(byteData)
      val decoder: Decoder = DecoderFactory.get.binaryDecoder(byteArrayInputStream, null)
      return reader.read(null, decoder)
    }
    catch {
      case e: IOException => {
        return null
      }
    } finally {
      try {
        byteArrayInputStream.close
      }
      catch {
        case e: IOException => {
        }
      }
    }
  }

  def getSchema: Schema = {
    val schemaStr: String = "{\"namespace\": \"io.divolte.examples.record\",\n" +
      "\"type\": \"record\",\n" + "\"name\": \"Record\",\n" +
      "\"fields\": [\n" +
      "{ \"name\": \"sessionId\",     \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "{ \"name\": \"refererpage\",   \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "{ \"name\": \"locationpage\",  \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "{ \"name\": \"eventType\",     \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "{ \"name\": \"partyId\",       \"type\": [\"null\", \"string\"], \"default\": null}\n" +
      "\n" +
      "]\n}"
    return new Schema.Parser().parse(schemaStr)
  }

  def getValue[T](genericRecord: GenericRecord, name: String, clazz: Class[T]): T = {
    val obj: AnyRef = genericRecord.get(name)
    if (obj == null) return null.asInstanceOf[T]
    if (obj.getClass eq classOf[Utf8]) {
      return obj.toString.asInstanceOf[T]
    }
    if (obj.getClass eq classOf[Integer]) {
      return obj.asInstanceOf[T]
    }
    return null.asInstanceOf[T]
  }

  def hashId(str: String) = {
    Hashing.md5().hashString(str).asLong()
  }

}
case class Event(sessionId: String,
                 refererpage: String,
                 locationpage: String,
                 eventType: String,
                 partyId:String)



object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}

