package ca.dataedu.kafka

import java.util.Properties
import collection.mutable.ListBuffer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}


object ProducerPlayground extends App{
  def chooseRandomItem[B](list: List[(B, Int)])(rand: Int): B = {
    list
      .scanLeft(list(1)._1, rand)((remainder, item) => item match { case (item, weight) => (item, remainder._2 - weight) })
      .drop(1)
      .filter(item => item match { case (item, weight) => weight <= 0 })
      .head
      ._1
  }

  def giveItemWeight[B](item: B)(weight: Int): (B, Int) = {
    return (item, weight)
  }

  def defaultWeighted[B](item: B): (B, Int) = {
    return giveItemWeight(item)(1)
  }

  def registerWeightedItem[B](list_buffer: ListBuffer[(B, Int)])(item: B, weight: Int) = {
    list_buffer.append(giveItemWeight(item)(weight))
  }

  case class Customer(id: Int, name: String, country: String, city: String)
  case class Product(id: Int, name: String, category: String, price: String)

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

  var customer_list = ListBuffer[(Customer, Int)]()
  var product_list = ListBuffer[(Product, Int)]()

  val registerCustomer = registerWeightedItem(customer_list)(_, _)
  val registerProduct = registerWeightedItem(product_list)(_, _)


  //DATA TO CREATE ENTRIES
  registerCustomer(Customer(1, "SpaceX", "United States", "Hawthorne CA"), 20)
  registerCustomer(Customer(2, "Blue Origin", "United States", "Kent WA"), 15)
  registerCustomer(Customer(3, "Orbital Sciences Corporation", "United States", "Dulles VA"), 5)
  registerCustomer(Customer(4, "Boeing", "United States", "Chicago IL"), 10)
  registerCustomer(Customer(5, "Northrop Grumman Innovation Systems", "United States", "Dulles VA"), 10)
  registerCustomer(Customer(6, "Sierra Nevada Corporation", "United States", "Sparks NV"), 5)
  registerCustomer(Customer(7, "Scaled Composites", "United States", "Mojave CA"), 3)
  registerCustomer(Customer(8, "The Spaceship Company", "United States", "Mojave CA"), 3)
  registerCustomer(Customer(9, "NASA", "United States", "Washington DC"), 15)
  registerCustomer(Customer(10, "Lockheed Martin", "United States", "Bethesda MD"), 10)
  registerCustomer(Customer(11, "ESA", "France", "Paris"), 3)
  registerCustomer(Customer(12, "JAXA", "Japan", "Tokyo"), 4)
  registerCustomer(Customer(13, "Rocket Lab", "New Zealand", "Auckland"), 1)
  registerCustomer(Customer(14, "Virgin Galactic", "England", "London"), 2)
  registerCustomer(Customer(15, "Copenhagen Suborbitals", "Denmark", "Copenhagen"), 1)
  registerCustomer(Customer(16, "ROSCOSMOS", "Russia", "Moscow"), 3)
  registerCustomer(Customer(17, "CNSA", "China", "Beijing"), 3)

  registerProduct(Product(1, "Dragon Capsule", "Launch Vehicle", "$26000.00"), 15)
  registerProduct(Product(2, "Falcon 9 Rocket", "Launch Vehicle", "$62000000.00"), 15)
  registerProduct(Product(3, "Dream Chaser Cargo System", "System", "$40000.00"), 5)
  registerProduct(Product(4, "Biconic Farrier", "Rocket Part", "$999.95"), 20)
  registerProduct(Product(5, "Second-stage Fuselage", "Rocket Part", "$9800.00"), 20)
  registerProduct(Product(6, "Life Support Systems", "System", "$78653.34"), 20)
  registerProduct(Product(7, "Reaction Wheels", "Rocket Part", "$199.99"), 20)
  registerProduct(Product(8, "Air Jordans", "Misc.", "Priceless"), 2)
  registerProduct(Product(9, "Geosynchronous Satellite", "Satellite", "$68000.00"), 5)
  registerProduct(Product(10, "Docking Ports (x3)", "Rocket Part", "$2999.97"), 20)
  registerProduct(Product(11, "Space Junk", "Misc.", "$0.00"), 15)
  registerProduct(Product(12, "Mk2 Inline Cockpit", "Rocket Part", "$19999.99"), 5)
  registerProduct(Product(13, "RC-L01 Remote Guidance Unit", "System", "$424242.42"), 25)
  registerProduct(Product(14, "Rockomax Jumbo-64 Fuel Tank", "Rocket Part", "$1349.99"), 5)
  registerProduct(Product(15, "PB-X150 Xenon Container", "Rocket Part", "$200.00"), 20)
  registerProduct(Product(16, "FTX-2 External Fuel Duct", "Rocket Part", "$89.99"), 5)
  registerProduct(Product(17, "48-7S Liquid Fuel Engine", "Rocket Part", "$22313.43"), 5)
  registerProduct(Product(18, "Vernor Engine", "Rocket Part", "$9999.99"), 25)
  registerProduct(Product(19, "FL-A5 Adapter", "Rocket Part", "$800.00"), 25)
  registerProduct(Product(20, "Modular Girder Segment (x100)", "Rocket Part", "$9999.00"), 15)
  registerProduct(Product(21, "TT18-A Launch Stability Enhancer", "Rocket Part", "$50000.00"), 25)
  registerProduct(Product(22, "TT-70 Radial Decoupler", "Rocket Part", "$2000.00"), 10)
  registerProduct(Product(23, "Hydraulic Detachment Manifold", "Rocket Part", "$4500.00"), 25)
  registerProduct(Product(24, "Shock Cone Intake", "Rocket Part", "$1984.00"), 15)
  registerProduct(Product(25, "Advanced Nose Cone - Type B", "Rocket Part", "$6000.00"), 25)
  registerProduct(Product(26, "Small Delta Wing (x2)", "Rocket Part", "$1999.98"), 15)
  registerProduct(Product(27, "LY-01 Fixed Landing Gear", "Rocket Part", "$894.93"), 20)
  registerProduct(Product(28, "TR-2L Ruggedized Vehicular Wheel", "Rocket Part", "$478.99"), 20)
  registerProduct(Product(29, "Heat Shield (2.5m)", "Rocket Part", "$4000.00"), 5)
  registerProduct(Product(30, "Thermal Control System", "System", "$120000.00"), 35)
  registerProduct(Product(31, "OX-STAT Photovoltaic Panels", "Rocket Part", "$3.99"), 15)
  registerProduct(Product(32, "Fuel Cell Array", "Rocket Part", "$900.00"), 25)
  registerProduct(Product(33, "Z-200 Rechargeable Battery Bank", "Rocket Part", "$1037.00"), 20)
  registerProduct(Product(34, "HG-5 High Gain Antenna", "Rocket Part", "$137.00"), 15)
  registerProduct(Product(35, "2HOT Thermometer", "Rocket Part", "$0.99"), 20)
  registerProduct(Product(36, "Mystery Goo™ Containment Unit", "Rocket Part", "$1.99"), 5)
  registerProduct(Product(37, "SENTINEL Infrared Telescope", "Satellite", "$30000000.00"), 5)
  registerProduct(Product(38, "James Webb Space Telescope", "Satellite", "$9700000000.00"), 5)
  registerProduct(Product(39, "Low-Earth Satellite", "Satellite", "$1000000"), 25)
  registerProduct(Product(40, "Soyuz-2 Launch System", "Launch Vehicle", "$217000000.00"), 20)
  registerProduct(Product(41, "Atlast V Rocket", "Launch Vehicle", "$9800000"), 15)
  registerProduct(Product(42, "Polar Satellite Launch Vehicle", "Launch Vehicle", "$8740000.00"), 15)
  registerProduct(Product(43, "Northrop Grumman Pegasus", "Launch Vehicle", "$185000000.00"), 10)
  registerProduct(Product(44, "Saturn V", "Launch Vehicle", "$123456789.00"), 5)
  registerProduct(Product(45, "Sputnik 1", "Satellite", "$0.02"), 5)

  val payment_types = List("Mastercard", "Discover", "Capital One", "Zelle Transfer", "UPI", "Google Wallet", "Apple Pay")

  val failure_reasons = List("Invalid CVV", "Not Enough Balance", "Incorrect Payment Address", "Suspicious Purchase Activity", "They're totally using this to make a bomb...")

  val r = scala.util.Random
  var now = java.time.Instant.now

  val customer_weight_sum = customer_list.result.map(_._2).sum
  val product_weight_sum = product_list.result.map(_._2).sum

  val randomCustomer = chooseRandomItem(customer_list.result())(_: Int)
  val randomProduct = chooseRandomItem(product_list.result())(_: Int)

  //  val file = scala.tools.nsc.io.File("transactions.csv")
  //  val file = new File("input/transactions.csv" )
  //  val printWriter = new PrintWriter(file)
  //  file.delete()

  //DATA GENERATOR AND MESSAGE PUSHER
  for(i <- 1 to 2000){
    val rand_product = randomProduct(scala.util.Random.nextInt(product_weight_sum) + 1)
    val rand_customer = randomCustomer(scala.util.Random.nextInt(customer_weight_sum) + 1)
    val rand_payment = payment_types(r.nextInt(payment_types.length))
    val rand_quantity = (-Math.log(r.nextDouble())*10).toInt + 1
    val rand_txn_id = (r.alphanumeric take 10).mkString
    val rand_success = if (r.nextInt(180) == 0) "N" else "Y"
    val rand_reason = if(rand_success == "Y") " " else failure_reasons(r.nextInt(failure_reasons.length))
    val rand_time_pass = r.nextInt((1500 * (Math.sin(2 * Math.PI * now.getEpochSecond / 86400) + 1) + 100).toInt)
    now = now.plusSeconds(rand_time_pass)

    // order_id, customer_id, customer_name, product_id, product_name, product_category, payment_type, qty, price, datetime, country, city, ecommerce_website_name, payment_txn_id, payment_txn_success, failure_reason 
    val transaction = List(i, 101 + rand_customer.id, rand_customer.name, 10001 + rand_product.id, rand_product.name, rand_product.category, rand_payment, rand_quantity, rand_product.price, now, rand_customer.country, rand_customer.city, "AllTheSpaceYouNeed.com", rand_txn_id, rand_success, rand_reason).mkString(",")


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
