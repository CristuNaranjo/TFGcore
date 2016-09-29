//
import java.sql.Timestamp

import com.mongodb.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document


/**
 * Hello world!
 *
 */

object App {

  def main(args: Array[String]): Unit = {


    val mongoConfig = new SparkConf()
    mongoConfig.setAppName("MongoSpark")
    mongoConfig.setMaster("local[4]")
    mongoConfig.set("spark.mongodb.input.uri", "mongodb://localhost:27017/NBA.games?readPreference=primaryPreferred")
   /* mongoConfig.set("spark.mongodb.input.database", "NBA")
    mongoConfig.set("spark.mongodb.input.collection", "games")
    mongoConfig.set("spark.mongodb.input.readPreference.name", "primaryPreferred")*/


    val sc = new SparkContext(mongoConfig)
    val sqlContext = SQLContext.getOrCreate(sc)

//    val rdd = MongoSpark.load(sc)
    val rdd = sc.loadFromMongoDB()
//    val aggregatedRdd = rdd.withPipeline(Seq(Document.parse("{$match: { 'box.players.player': 'Kobe Bryant' },{$unwind: '$box'},{$match: {'box.players.player': 'Kobe Bryant'}},{$unwind: '$box.players'},{$match: { 'box.players.player': 'Kobe Bryant' }}}")))
    val aggregatedRdd = rdd.withPipeline(Seq(Document.parse("{$match: { 'box.players.player': 'Kobe Bryant' }}"),
                        Document.parse("{$unwind: '$box'}"),
                        Document.parse("{$match: {'box.players.player': 'Kobe Bryant'}}"),
                        Document.parse("{$unwind: '$box.players'}"),
                         Document.parse("{$match: { 'box.players.player': 'Kobe Bryant' }}")
    ))
    println(aggregatedRdd.count)
    println(aggregatedRdd.first.toJson)
    val test = aggregatedRdd.toDF()
    test.printSchema()



//    val dfardd = aggregatedRdd.toDF[Game]()
//    dfardd.show()
//    dfardd.printSchema()

    /*root
 |-- box: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- players: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- ast: integer (nullable = false)
 |    |    |    |    |-- pts: integer (nullable = false)
 |    |    |    |    |-- trb: integer (nullable = false)
 |    |    |-- team: struct (nullable = true)
 |    |    |    |-- ast: integer (nullable = false)
 |    |    |    |-- pts: integer (nullable = false)
 |    |    |    |-- trb: integer (nullable = false)
 |    |    |-- wont: integer (nullable = false)
 |-- date: timestamp (nullable = true)
 |-- teams: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- abbreviation: string (nullable = true)
 |    |    |-- score: integer (nullable = false)
 |    |    |-- home: boolean (nullable = false)
 |    |    |-- won: integer (nullable = false)*/


//    val test = dfardd.select("box")
//    val test2 = test.map()
//    dfardd.registerTempTable("MongoTable")
//
//    val selection = sqlContext.sql("SELECT * FROM MongoTable WHERE box.players.*.pts = 'Kobe Bryant' ")
//    selection.show()
    /* Cuenta los partidos de Kobe
    println(aggregatedRdd.count)
    println(aggregatedRdd.first.toJson)*/



//    val sqlContext = SQLContext.getOrCreate(sc)
    //DataFrame por defecto
//    val dataframe = MongoSpark.load(sqlContext)
//    //println("Aqui viene el Schema.")
//    //df.printSchema()
//    dataframe.show(20);

    //DataFrame personalizado con los datos que quiera...
//    val explicitDF = MongoSpark.load[Game](sqlContext)
//    explicitDF.printSchema()
//    explicitDF.show()



//    val dataframeInferred = MongoSpark.load[Game](sqlContext)
//    val dataframeExplicit = MongoSpark.load[Game](sqlContext)
//    val dataset = MongoSpark.load[Game](sqlContext).as[Game]()
    //Dataset
//    val dataset= explicitDF.as[Game]
//    dataset.show()
//    val dataset = aggregatedRdd.as
//    dataset.show()
  /*  dataset.registerTempTable("games")*/



   /* Cuenta todos los partidos de la mongo
    println(rdd.count)
    println(rdd.first.toJson)*/

   /* val rdd = sc.loadFromMongoDB()
    println(rdd.count)
    println(rdd.first.toJson)*/

  }

//  Global case classes
  case class Players(ast:Int, blk:Int, drb: Int, fg:Int, fg3: Int, fg3_pct: String, fg3a: Int, fg_pct: String, fga: Int,
                     ft: Int, ft_pct: String, fta: Int, mp: String, orb: Int, pf:Int, player: String,plus_minus: String, pts: Int,
                     stl: Int, tov: Int, trb: Int)

  case class Team(ast:Int, blk:Int, drb: Int, fg:Int, fg3: Int, fg3_pct: String, fg3a: Int, fg_pct: String, fga: Int,
                  ft: Int, ft_pct: String, fta: Int, mp: String, orb: Int, pf:Int, pts: Int,
                  stl: Int, tov: Int, trb: Int )
  case class Box(players: Array[Players], team: Team, wont: Int)

  case class Teams(name: String, abbreviation: String, score: Int, home: Boolean, won: Int)

  case class Game( box: Array[Box] ,date: Timestamp, teams: Array[Teams])

  //Kobe case class, only ast, point,trb
 /* case class Players(ast:Int, pts: Int, trb: Int)

  case class Team(ast:Int, pts: Int, trb: Int )
  case class Box(players: Array[Players], team: Team, wont: Int)

  case class Teams(name: String, abbreviation: String, score: Int, home: Boolean, won: Int)

  case class Game( box: Array[Box] ,date: Timestamp, teams: Array[Teams])*/
}