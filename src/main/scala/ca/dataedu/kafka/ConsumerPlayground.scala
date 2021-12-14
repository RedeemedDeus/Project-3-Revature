package ca.dataedu.kafka

import java.time.Duration

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

import java.io._

import scala.collection.JavaConverters._

object ConsumerPlayground extends App {

  //INITIATE SPARK SESSION//
  System.setProperty("hadoop.home.dir", "C:\\hadoop")
  val spark = SparkSession
    .builder
    .appName("Kafka Streaming")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  println("Created Spark Session")
  spark.sparkContext.setLogLevel("ERROR")

  val topicName = "sql_dolphins" //hadoop_elephants

  val consumerProperties = new Properties()
  consumerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092") //25.58.43.190
  consumerProperties.setProperty(GROUP_ID_CONFIG, "group-id-2")
  consumerProperties.setProperty(AUTO_OFFSET_RESET_CONFIG, "latest")
  consumerProperties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, classOf[IntegerDeserializer].getName)
  consumerProperties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProperties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false")

  val consumer = new KafkaConsumer[Int, String](consumerProperties)
  consumer.subscribe(List(topicName).asJava)

  //CREATE AND OPEN FILE WRITER
  val fileObject = new File("output/transactions.csv")
  val printWriter = new PrintWriter(new FileOutputStream(fileObject))




  while (true) {
    val polledRecords: ConsumerRecords[Int, String] = consumer.poll(Duration.ofSeconds(1))

    if (!polledRecords.isEmpty) {
      println(s"Polled ${polledRecords.count()} records")
      val recordIterator = polledRecords.iterator()

      while (recordIterator.hasNext) {
        val record: ConsumerRecord[Int, String] = recordIterator.next()
        println(s"| ${record.key()} | ${record.value()} ") //| ${record.partition()} | ${record.offset()} |")
        //val csvTrip = record.value()

        //WRITE THE KAFKA STREAM TO THE FILE
        printWriter.write(record.value() + "\n")

      }

    }

    //FLUSH THE FILE
    printWriter.flush()

  }

}
