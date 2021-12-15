package ca.dataedu.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}


object ProducerPlayground extends App{

  val topicName = "sql_dolphins"

  val producerProperties = new Properties()
  producerProperties.setProperty(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092" //25.58.43.190
  )
  producerProperties.setProperty(
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName
  )
  producerProperties.setProperty(
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName
  )

  val producer = new KafkaProducer[Int, String](producerProperties)

  //DATA TO CREATE ENTRIES
  val customer_names = List("SpaceX", "Blue Origin", "Orbital Sciences Corporation", "Boeing", "Northrop Grumman Innovation Systems", "Sierra Nevada Corporation", "Scaled Composites", "The Spaceship Company", "NASA", "Lockheed Martin", "ESA", "JAXA", "Rocket Lab", "Virgin Galactic", "Copenhagen Suborbitals", "ROSCOSMOS", "CNSA")
  val customer_countries = List("United States", "United States", "United States", "United States", "United States", "United States", "United States", "United States", "United States", "United States", "France", "Japan", "New Zealand", "England", "Denmark", "Russia", "China")
  val customer_cities = List("Hawthorne CA", "Kent WA", "Dulles VA", "Chicago IL", "Dulles VA", "Sparks NV", "Mojave CA", "Mojave CA", "Washington DC", "Bethesda MD", "Paris", "Tokyo", "Auckland", "London", "Copenhagen", "Moscow", "Beijing")
  val customer_weightings = List(20, 15, 5, 10, 10, 5, 3, 3, 15, 10, 3, 4, 1, 2, 1, 3, 3)
  val customer_index = for (i <- 0 until customer_weightings.length; j <- 0 until customer_weightings(i)) yield {i}

  val product_names = List("Dragon Capsule", "Falcon 9 Rocket", "Dream Chaser Cargo System", "Biconic Farrier", "Second-stage Fuselage", "Life Support Systems", "Reaction Wheels", "Air Jordans", "Geosynchronous Satellite", "Docking Ports (x3)", "Space Junk")
  val product_categories = List("Rocket", "Rocket", "System", "Part", "Part", "System", "Part", "Misc.", "Satellite", "Part", "Misc.")
  val product_prices = List("$26000.00", "$10000000.00", "$40000.00", "$999.95", "$9800.00", "$78653.34", "$199.99", "Priceless", "$68000.00", "$2999.97", "$0.00")
  val product_weightings = List(10, 10, 5, 20, 20, 20, 20, 1, 5, 30, 5)
  val product_index = for (i <- 0 until product_weightings.length; j <- 0 until product_weightings(i)) yield {i}

  val payment_types = List("Mastercard", "Discover", "Capital One", "Zelle Transfer", "UPI", "Google Wallet", "Apple Pay")

  val failure_reasons = List("Invalid CVV", "Not Enough Balance", "Incorrect Payment Address", "Suspicious Purchase Activity", "They're totally using this to make a bomb...")

  val r = scala.util.Random
  var now = java.time.Instant.now
  //  val file = scala.tools.nsc.io.File("transactions.csv")
  //  val file = new File("input/transactions.csv" )
  //  val printWriter = new PrintWriter(file)
  //  file.delete()

  //DATA GENERATOR AND MESSAGE PUSHER
  for(i <- 1 to 2000){
    val rand_customer = customer_index(r.nextInt(customer_index.length))
    val rand_product = product_index(r.nextInt(product_index.length))
    val rand_payment = payment_types(r.nextInt(payment_types.length))
    val rand_quantity = (-Math.log(r.nextDouble())*10).toInt + 1
    val rand_txn_id = (r.alphanumeric take 10).mkString
    val rand_success = if (r.nextInt(200) == 0) "N" else "Y"
    val rand_reason = if(rand_success == "Y") " " else failure_reasons(r.nextInt(failure_reasons.length))
    val rand_time_pass = r.nextInt((1500 * (Math.sin(2 * Math.PI * now.getEpochSecond / 86400) + 1) + 100).toInt)
    now = now.plusSeconds(rand_time_pass)

    //order_id, customer_id, customer_name, product_id, product_name, product_category, payment_type, qty, price, datetime, country, city, ecommerce_website_name, payment_txn_id, payment_txn_success, failure_reason 
    val transaction = List(i, 101 + rand_customer, customer_names(rand_customer), 10001 + rand_product, product_names(rand_product), product_categories(rand_product), rand_payment, rand_quantity, product_prices(rand_product), now, customer_countries(rand_customer), customer_cities(rand_customer), "AllTheSpaceYouNeed.com", rand_txn_id, rand_success, rand_reason).mkString(",")

    //SENDS A MESSAGE TO THE TOPIC
    producer.send(new ProducerRecord[Int, String](topicName, i, transaction))

    //  println(transaction)
    //  file.appendAll(transaction + "\n")
    //  printWriter.write(transaction + "\n")
  }


  //  producer.send(new ProducerRecord[Int, String](topicName, 10, "Message 1"))
  //  producer.send(new ProducerRecord[Int, String](topicName, 20, "Message 2"))
  //  producer.send(new ProducerRecord[Int, String](topicName, 30, "Message 3"))
  //  producer.send(new ProducerRecord[Int, String](topicName, 40, "Message 4"))
  //  producer.send(new ProducerRecord[Int, String](topicName, 50, "Message 5"))
  //  producer.send(new ProducerRecord[Int, String](topicName, 60, "Message 6"))

  producer.flush() //producer.sends works async and just to ake sure alle the messages are published

  //  def customPartitioned(key: Int): Int = key % 3
}
