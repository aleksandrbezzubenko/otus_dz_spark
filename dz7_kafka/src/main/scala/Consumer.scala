import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util
import java.util.Properties
import scala.collection.JavaConverters.{asJavaCollectionConverter, asScalaBufferConverter, iterableAsScalaIterableConverter}

object Consumer {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:29092")
  props.put("group.id", "consumer1")

  val maxMsgForPartition = 5

  val consumer = new KafkaConsumer(props, new StringDeserializer, new StringDeserializer)

  def get(topic: String): Unit = {
    consumer.subscribe(List(topic).asJavaCollection)

    val partitions = consumer.partitionsFor(topic).asScala
    val topicList = new util.ArrayList[TopicPartition]()

    partitions.foreach {
      p =>
        topicList.add(new TopicPartition(p.topic(), p.partition()))
    }

    val messages = consumer.poll(Duration.ofSeconds(1))
    consumer.seekToEnd(topicList)
    val partitionOffset = new util.HashMap[String, Long]()
    topicList.asScala.foreach(partition => partitionOffset.put(partition.toString, consumer.position(partition) - 1))
    val tmp = new util.HashMap[String, List[String]]()

    for (message <- messages.asScala) {

      val key = s"${message.topic()}-${message.partition()}"

      if (message.offset() < partitionOffset.get(key) && message.offset() >= partitionOffset.get(key) - maxMsgForPartition) {

        val msgWithOffset = s"offset: ${message.offset()} | msg: ${message.value()}"

        if (!tmp.containsKey(key)) {

          tmp.put(key, List(msgWithOffset))

        } else {

          val currentMsgList = tmp.get(key)
          if (currentMsgList.length <= maxMsgForPartition){
            val newMsgList = msgWithOffset :: currentMsgList
            tmp.put(key, newMsgList)
          }

        }
      }
    }

    topicList.asScala.foreach(partition => {
      val key = partition.toString
      println(s"Topic: $key")
      tmp.getOrDefault(key, List()).foreach(println)
    }
    )

    consumer.close()
  }
}
