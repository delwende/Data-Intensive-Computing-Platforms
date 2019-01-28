package sparkstreaming
import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object KafkaSpark {
  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()//<FILL IN>
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")

	
    // make a connection to Kafka and read (key, value) pairs from it
    //<FILL IN>
    val conf = new SparkConf().setAppName("kafka data AVG").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("checkpoint")
    val kafkaConf = Map("metadata.broker.list" -> "localhost:9092",
			"zookeeper.connect" -> "localhost:2181",
			"group.id" -> "kafka-spark-streaming",
			"zookeeper.connection.timeout.ms" -> "1000")//<FILL IN>

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, Set("avg"))
    val pairs = messages.map(v => v._2.split(",")).map(m => (m(0), m(1).toDouble))

    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[(Double,Int)]): (String, Double) = {
	// if a new (key,value)come we check if the state already exist (if there is avg with that key ) 
	// we load the previous avg from the state  add with the new value and divide by two to get the average
		
		val previousState = state.getOption.getOrElse((0.0,0))
		val Sum = previousState._1 + value.get
		val count = previousState._2 + 1
		state.update((Sum,count))
		(key, Sum/count)
    }
    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    // store the result in Cassandra
    stateDstream.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))


    ssc.start()
    ssc.awaitTermination()
    session.close()
  }
}
