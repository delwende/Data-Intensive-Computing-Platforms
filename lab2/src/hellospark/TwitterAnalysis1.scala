import org.apache.spark.sql.SparkSession

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

import java.util.Properties
import scala.io._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import scala.util.parsing.json.JSON
object TwitterAnalysis1 {

  def main(args: Array[String]): Unit = {
    
	val spark = SparkSession.builder.appName("Hello Spark").master("local[2]").getOrCreate()
	val sc = spark.sparkContext
    val tweets = "eliane is nice persone"
    val ibmDS = spark.read.json("IBMtweets.json")

//import spark.implicits._
//ibmDS.rdd.map(item => (sentiment(text(0).asInstanceOf[String]), 1)).reduceByKey(_ + _).saveToCassandra("twitter
import spark.implicits._
val result1 = ibmDS.select("user.location","text").collect.map(r => Map(ibmDS.select("user.location","text").columns.zip(r.toSeq):_*))
val result2 = result1.map(p => ((p.getOrElse("location", null).asInstanceOf[String], sentiment(p.getOrElse("text", null).asInstanceOf[String])),1))
val result3= result2.reduce((x, y) => x + y)
val r4 = result3.map { case ((name, food), total) => (name, (food, total)) }
//val result = ibmDS.select("user.location","text").map(t =>t(1).asInstanceOf[String]).collection()

//println(result2)
r4.foreach {case (key, value) => println (key + "-->" + value)}
//println(result1)
//println(ibmDS.select("user.location","text").show(1))
//val json = Source.fromFile("IBMtweets.json")

    // parse
    //val mapper = new ObjectMapper() with ScalaObjectMapper
    //mapper.registerModule(DefaultScalaModule)
    //val parsedJson = mapper.readValue[Map[String, Object]](json.reader())
    //println(parsedJson)

//println(parseWithLiftweb(Source.fromFile("IBMtweets.json")))



val json = sc.wholeTextFiles("IBMtweets.json").map(tuple => tuple._2.replace("\n", "").trim)

val df =  spark.read.json(json)
val peopleArray = df.collect.map(r => Map(df.columns.zip(r.toSeq):_*))
//val people = Map(peopleArray.map(p => (p.getOrElse("user", null), p)):_*)
//println(people)
//println(ibmDS.printSchema())
spark.stop()
	  
  }
def parseWithJackson(json: BufferedSource): Map[String, Object] = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.readValue[Map[String, Object]](json.reader())
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
