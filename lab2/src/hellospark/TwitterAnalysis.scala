import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.spark.SparkContext._


import org.apache.spark.{SparkContext, SparkConf}
import scala.io.Source

import org.apache.spark.SparkContext
import edu.stanford.nlp.util.CoreMap
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import scala.collection.JavaConverters._
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}


import java.util.Properties
object TwitterAnalysis {

  def main(args: Array[String]): Unit = {
	val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
	val session = cluster.connect()
	session.execute("CREATE KEYSPACE IF NOT EXISTS twitter WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
	session.execute("CREATE TABLE IF NOT EXISTS twitter.aggregated (word text PRIMARY KEY, count float)")
	val spark = SparkSession.builder.appName("Hello Spark").master("local[*]").getOrCreate()
	val sc = spark.sparkContext
	
	val ibmDS = spark.read.json("IBMtweets.json")
    import spark.implicits._
    ibmDS.select("text").rdd.map(text => (sentiment(text(0).asInstanceOf[String]), 1)).reduceByKey(_ + _).saveToCassandra("twitter","aggregated")

        val result1 = ibmDS.select("user.location","text").collect.map(r => Map(ibmDS.select("user.location","text").columns.zip(r.toSeq):_*))
        //can you help to apply reduce function I have (location,(sentiment,1)). may be you need to add 1 to reduce.
       val result2 = result1.map(p => (p.getOrElse("location", null).asInstanceOf[String], (sentiment(p.getOrElse("text", null).asInstanceOf[String]),1)))
       result2.foreach {case (key, value) => println (key + "-->" + value)}

	spark.stop()
	  
  }

  def sentiment(tweets: String): String = {
    var mainSentiment = 0
    var longest = 0;
    val sentimentText = Array("Very Negative", "Negative", "Neutral", "Positive", "Very Positive")
    val props = new Properties();
    props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
    new StanfordCoreNLP(props).process(tweets).get(classOf[CoreAnnotations.SentencesAnnotation]).asScala.foreach((sentence: CoreMap) => {
      val sentiment = RNNCoreAnnotations.getPredictedClass(sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree]));
      val partText = sentence.toString();
      if (partText.length() > longest) {
        mainSentiment = sentiment;
        longest = partText.length();
      }
    })
    sentimentText(mainSentiment)
  }
}
