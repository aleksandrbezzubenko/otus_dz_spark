import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import spray.json.DefaultJsonProtocol._
import spray.json.{JsonFormat, pimpAny}

import java.util.Properties

object Producer {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:29092")

  val producer = new KafkaProducer(props, new StringSerializer, new StringSerializer)

  implicit val bookFormatter: JsonFormat[Book] = jsonFormat7(Book)

  def send(booksInfo: Seq[Book], topic: String): Unit = {
    booksInfo.foreach {
      book =>
        val bookJson = book.toJson.toString()
        producer.send(new ProducerRecord(topic, bookJson, bookJson))
    }

    producer.close()
  }
}
