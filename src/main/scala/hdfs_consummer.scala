import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.io.PrintWriter
import java.util
import java.util.logging.Level


object hdfs_consummer extends App {

  import java.util.Properties

  val TOPIC="peaceland"

  val  props = new Properties()
  props.put("bootstrap.servers", "127.0.0.1:9092")

  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "something")

  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(util.Collections.singletonList(TOPIC))

  import java.util.logging.Logger

  Logger.getLogger("org").setLevel(Level.WARNING)
  Logger.getLogger("akka").setLevel(Level.WARNING)
  Logger.getLogger("kafka").setLevel(Level.WARNING)

  val properties = System.getProperties
  properties.setProperty("HADOOP_USER_NAME", "hadoop")
  val conf = new Configuration()
  conf.set("fs.defaultFS", "hdfs://localhost:9000")
  conf.set("HDAOOP_USER_NAME", "hadoop")
  val fs= FileSystem.get(conf)

  // Pour eviter un var, on crée les writer à l'avance. Il faut donc connaitre le nombre de fichiers que l'on souhaite écrire.
  // Ici, on se limitera à 100 fichiers .json. Chacun contenant 1000 messages de drones
  val writers = (0 to 100).map(i => new PrintWriter(fs.create(new Path("/tmp/mySample" + i.toString+".json"))))
  var compte : Int = 1


  def run_forever(): Unit = {
    val records=consumer.poll(100)
    records.forEach( record => {
      val filenumber :Int = compte/1000
      println(filenumber)
      //println("**********************")
      val res = record.value().replace("\\","").dropRight(1).substring(1)


      if (compte % 1000 != 0) {
        //writers((compte / 1000).toInt ).write(res +"," + "\n")
        writers((compte/1000).toInt).println(res + ",")
        compte += 1
      }
      else {
        writers(filenumber-1).println(res)
        writers(filenumber-1).close()
        compte += 1
        println("On cree un nouveau fichier")
      }
    })
    run_forever()
  }

  run_forever()
  writers.map(writer => writer.close())

}
