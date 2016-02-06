/**
  * Created by rutvikparmar on 05/02/16.
  */
import java.io.{IOException, ByteArrayInputStream}
import java.util.concurrent.{ExecutorService, Executors}
import java.util.{Properties, UUID}
import kafka.message.{Message, MessageAndMetadata}
import kafka.utils.{Logging, VerifiableProperties}
import org.apache.avro.io.{DecoderFactory, Decoder}
import org.apache.avro.util.Utf8
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericData, GenericRecord}
import kafka.consumer.{KafkaStream, ConsumerConfig, Consumer}

object ScalaAvroKafka extends App{
  val example = new ScalaConsumerExample(args(0), args(1), args(2))
  example.run(args(3).toInt)
}

class ScalaConsumerExample(val zookeeper: String,
                           val groupId: String,
                           val topic: String) extends Logging {

  val config = createConsumerConfig(zookeeper, groupId)
  val consumer = Consumer.create(config)
  var executor: ExecutorService = null

  def shutdown() = {
    if (consumer != null)
      consumer.shutdown()
    if (executor != null)
      executor.shutdown()
  }

  def createConsumerConfig(zookeeper: String, groupId: String): ConsumerConfig = {
    val props = new Properties()
    props.put("zookeeper.connect", zookeeper)
    props.put("group.id", groupId)
    props.put("auto.offset.reset", "largest")
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    val config = new ConsumerConfig(props)
    config
  }

  def run(numThreads: Int) = {
    val topicCountMap = Map(topic -> numThreads)
    val consumerMap = consumer.createMessageStreams(topicCountMap)
    val streams = consumerMap.get(topic).get

    executor = Executors.newFixedThreadPool(numThreads)
    var threadNumber = 0
    for (stream <- streams) {
      executor.submit(new ScalaConsumerTest(stream, threadNumber))
      threadNumber += 1
    }
  }
}

class ScalaConsumerTest(val stream: KafkaStream[Array[Byte], Array[Byte]], val threadNumber: Int) extends Logging with Runnable {
  def run {
    val it = stream.iterator()

    while(it.hasNext())
    {

      var location: String = null
      var referer: String = null
      var sessionId: String = null
      var partyId: String = null
      var eventType: String = null

      val messageAndMetadata: MessageAndMetadata[Array[Byte], Array[Byte]] = it.next
      val genericRecord: GenericRecord = byteArrayToDatum(getSchema, messageAndMetadata.message)
      location = getValue(genericRecord, "locationpage", classOf[String])
      referer = getValue(genericRecord, "refererpage", classOf[String])
      sessionId = getValue(genericRecord, "sessionId", classOf[String])
      partyId = getValue(genericRecord, "partyId", classOf[String])
      eventType = getValue(genericRecord, "eventType", classOf[String])


      println(s"SessionId :  $sessionId")
      println(s"PartyId :  $partyId")
      println(s"Referer :  $referer")
      println(s"Location :  $location")
      println(s"EventType: $eventType")

    }

    System.out.println("Shutting down Thread: " + threadNumber)
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
    val schemaStr: String = "{\"namespace\": \"io.divolte.examples.record\",\n" + "\"type\": \"record\",\n" + "\"name\": \"Record\",\n" + "\"fields\": [\n" + "{ \"name\": \"sessionId\",     \"type\": [\"null\", \"string\"], \"default\": null},\n" + "{ \"name\": \"refererpage\",   \"type\": [\"null\", \"string\"], \"default\": null},\n" + "{ \"name\": \"locationpage\",  \"type\": [\"null\", \"string\"], \"default\": null},\n" + "{ \"name\": \"eventType\",     \"type\": [\"null\", \"string\"], \"default\": null},\n" + "{ \"name\": \"partyId\",       \"type\": [\"null\", \"string\"], \"default\": null}\n" + "\n" + "]\n}"
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

}