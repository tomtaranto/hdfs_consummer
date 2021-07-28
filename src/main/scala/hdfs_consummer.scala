import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.io.{BufferedWriter, OutputStreamWriter, PrintWriter}
import java.util
import java.util.logging.Level
import org.apache.hadoop.fs.FSDataOutputStream

object hdfs_consummer extends App {

  import java.util.Properties

  val TOPIC="peaceland"

  val  props = new Properties()
  props.put("bootstrap.servers", "127.0.0.1:9092")

  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "something")
  props.put("partition.assignment.strategy","org.apache.kafka.clients.consumer.StickyAssignor")

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
  conf.set("dfs.client.block.write.replace-datanode-on-failure.policy","ALWAYS")
  conf.set("dfs.client.block.write.replace-datanode-on-failure.best-effort","true")
  val fs= FileSystem.get(conf)
  fs.setReplication(new Path("/tmp/mySample"),1)

  // NOTRE SEUL VAR AUTORISEE POUR LE COMPTAGE DES MESSAGES
  var compte : Int = 1

  println("READY TO WRITE")

  // fonction récursive pour tourner en continu
  def run_forever(): Unit = {
    // lecture dans la stream
    val records=consumer.poll(100)

    // pour chaque message
    records.forEach( record => {
      // numero du fichier dans lequel on va écrire
      val filenumber :Int = compte/100
      println(filenumber.toString)
      // on supprime les guillemets du début et de la fin. Et on enlève les doubles "\"
      // parceque kafka rajoute automatique des guillemets autour de notre string et échappe les "\"
      val res = record.value().replace("\\","").dropRight(1).substring(1)
      println(res)
      // si on a pas encore lu 1000 messages, on ajoute dans le fichier courant
      if (compte % 100 != 0) {
        // écrire le message avec une virgule à la fin (pour avoir un beau format json)
        writeAsString("/tmp/mySample" + (compte/100).toInt.toString+".json", res+",")

        // on augmente notre compteur de message
        compte += 1
      }
      else { // sinon on termine l'écriture dans le fichier précédent

        writeAsString("/tmp/mySample" + (filenumber-1).toString+".json", res)

        // on augmente notre compteur de message
        compte += 1

        // on affiche dans la console qu'on a créé un nouveau fichier
        println("On cree un nouveau fichier")
      }
    })
    // RECURSION
    run_forever()
  }

  // première appel de la fonction récursive
  run_forever()

  def writeAsString(hdfsPath: String, content: String) {
    val path: Path = new Path(hdfsPath)
    if (fs.exists(path)) {
      //fs.delete(path, true)
      val dataOutputStream: FSDataOutputStream = fs.append(path)
      val bw: BufferedWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream, "UTF-8"))
      bw.write(content)
      bw.newLine()
      bw.close()
    }
    else {
      val dataOutputStream: FSDataOutputStream = fs.create(path)
      val bw: BufferedWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream, "UTF-8"))
      bw.write(content)
      bw.close()
    }

  }

}
