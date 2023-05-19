package edu.sogang.datagen

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


import java.io.File
import java.lang.Thread.sleep
import java.util.Properties

import scala.annotation.tailrec
import scala.io.BufferedSource
import scala.sys.exit


object StreamProducer {
  def main(args: Array[String]): Unit = {
    print("test")
    val usage =
      """
    Usage: java -jar ASSEMBLED_JAR_PATH --config-filename FILE_NAME
    """
    println("test1")
    val argList = args.toList
    type ArgMap = Map[Symbol, Any]
    println("test2")
    @tailrec
    def parseArgument(map: ArgMap, list: List[String]): ArgMap = {
      list match {
        case Nil =>
          map
        case "--config-filename" :: value :: tail =>
          parseArgument(map ++ Map('confFileName -> value), tail)
        case option :: tail =>
          println("Unknown option : " + option)
          println(usage)
          exit(1)
      }
    }
    println("test3")
    val parsedArgs = parseArgument(Map(), argList)
    println("test4")
    val prop = new Properties()
//    val inputSteam = this.getClass.getClassLoader.getResourceAsStream(parsedArgs('confFileName).toString)
//
//    try {
//      prop.load(inputSteam)
//    } catch {
//      case e: IOException =>
//        e.printStackTrace()
//    } finally {
//      inputSteam.close()
//    }

    // app config
//    val msgInterval = prop.get("msgInterval").asInstanceOf[String].toInt
//    val dataDir = prop.get("dataDir").asInstanceOf[String]
    val msgInterval = 200
    val dataDir = "/home/jeeseob/experiment/data/clusterdata-2011-2/task_events"
    val topic = "clusterdata"
//
//    // kafka config
//    val topic = prop.get("topic").asInstanceOf[String]
//    val bootstrapServers = prop.get("bootstrapServers").asInstanceOf[String]
//    val keySerializer = prop.get("keySerializer").asInstanceOf[String]
//    val valueSerializer = prop.get("valueSerializer").asInstanceOf[String]
//    val compressionType = prop.get("compressionType").asInstanceOf[String]
//
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("compression.type", "lz4")

    var i = 0
    val producer = new KafkaProducer[String, String](props)

    def getListOfFiles(dir: String): List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
      } else {
        List[File]()
      }
    }

    for (file <- getListOfFiles(dataDir).sorted) {
      val source: BufferedSource = io.Source.fromFile(file.getAbsoluteFile)
      println(file.getAbsoluteFile)
      for (line <- source.getLines) {

        val startTime = System.nanoTime()
        // TODO: we have to consider the kafka partition by changing key value
        val record = new ProducerRecord[String, String](topic, i.toString, line)
        producer.send(record)
        i += 1
        println(s"submit : $i, $line")
        val endTime = System.nanoTime()

        val elapsedTimeInSecond = (endTime - startTime) / 1000000

        if (msgInterval - elapsedTimeInSecond > 0) {
          sleep(msgInterval - elapsedTimeInSecond)
        }
      }
      source.close()
    }

    producer.close()
  }
}
