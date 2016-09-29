import java.sql.Timestamp

import com.mongodb.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

/**
  * Created by NaranjO on 29/9/16.
  */
object KB {

  case class Players(pts: Int)

  case class Box(players: Players, won: Int)

  case class Teams(name: String, abbreviation: String, score: Int, home: Boolean, won: Int)

  case class Game( box: Box ,date: Timestamp, teams: Array[Teams])

  def main(args: Array[String]) {

    val mongoConfig = new SparkConf()
    mongoConfig.setAppName("MongoSpark")
    mongoConfig.setMaster("local[4]")
    mongoConfig.set("spark.mongodb.input.uri", "mongodb://localhost:27017/NBA.games?readPreference=primaryPreferred")


    val sc = new SparkContext(mongoConfig)
    val sqlContext = SQLContext.getOrCreate(sc)

    val rdd = sc.loadFromMongoDB()
    //    val aggregatedRdd = rdd.withPipeline(Seq(Document.parse("{$match: { 'box.players.player': 'Kobe Bryant' },{$unwind: '$box'},{$match: {'box.players.player': 'Kobe Bryant'}},{$unwind: '$box.players'},{$match: { 'box.players.player': 'Kobe Bryant' }}}")))
    val aggregatedRdd = rdd.withPipeline(Seq(Document.parse("{$match: { 'box.players.player': 'Kobe Bryant' }}"),
      Document.parse("{$unwind: '$box'}"),
      Document.parse("{$match: {'box.players.player': 'Kobe Bryant'}}"),
      Document.parse("{$unwind: '$box.players'}"),
      Document.parse("{$match: { 'box.players.player': 'Kobe Bryant' }}")
    ))

    val test = aggregatedRdd.toDF[Game]
    test.show()
    val test2 = test.filter("month(date)=11")
    println(test2.count)
    test2.show()
//    println(aggregatedRdd.count)
//    println(aggregatedRdd.first.toJson)
//    val test = aggregatedRdd.toDF()
//    test.printSchema()
////    test.show()
//    val test2 = test.map(document => println(document.schema)
  }

}
