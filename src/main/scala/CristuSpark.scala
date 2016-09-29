import java.sql.Timestamp

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import com.mongodb.spark._
import org.bson.Document
import org.apache.spark.sql.functions.{udf, lit}
import scala.util.Try

/**
  * Created by NaranjO on 29/9/16.
  */

object CristuSpark {

  //Clases para quedarme solo con los puntos de cada jugador
  case class Jugadores(pts: Int)

  case class Box(players: Jugadores, won: Int)

  case class Equipos(name: String, abbreviation: String, score: Int, home: Boolean, won: Int)

  case class Partido(box: Box, date: Timestamp, teams: Array[Equipos])


  val mongoConfig = new SparkConf()
  mongoConfig.setAppName("MongoSpark")
  mongoConfig.setMaster("local[4]")
  mongoConfig.set("spark.mongodb.input.uri", "mongodb://localhost:27017/NBA.games?readPreference=primaryPreferred")

  val sc = new SparkContext(mongoConfig)
  val sqlContext = SQLContext.getOrCreate(sc)
  val rdd = sc.loadFromMongoDB()
  rdd.cache()


  def prediccion(fecha: Array[String], abbrL: String, localPlayers: Array[String], abbrV: String, visitPlayers: Array[String]): Unit = {
    resultadosPrevios(abbrL, abbrV, localPlayers, visitPlayers)
  }

  def configure(): Unit = {

  }

  def resultadosPrevios(local: String, visitante: String, localPlayers: Array[String], visitPlayers: Array[String]): Unit = {
    //Filtrar mongoDB para obtener los resultados previos entre cada jugador y el equipo rival
    //Primero filtro para obtener los partidos del jugador
    //Locales
    for (i <- 0 to localPlayers.length - 1) {
      var puntos: Array[Int] = Array()
      var player = localPlayers(i)
      println(localPlayers(i))
      val rddJugador = CristuSpark.rdd.withPipeline(Seq(Document.parse("{$match: { 'box.players.player': " + "'" + player + "'" + " }}"),
        Document.parse("{$unwind: '$box'}"),
        Document.parse("{$match: {'box.players.player':" + "'" + player + "'" + "}}"),
        Document.parse("{$unwind: '$box.players'}"),
        Document.parse("{$match: { 'box.players.player': " + "'" + player + "'" + " }}")))
      val dfJugador = rddJugador.toDF[Partido]
      dfJugador.cache()
      dfJugador.printSchema()
      println(dfJugador.count)
      puntos.foreach(println)
      //Filtro para obtener los partidos anteriores contra ese equipo jugando como local
      val partidosVSVisitante = dfJugador.select("*").where(dfJugador("teams").getItem(1).getField("abbreviation") === visitante )
      val result = partidosVSVisitante.select(partidosVSVisitante("box.players.pts"))
      result.show()
    }


  }

  def resultadosMes(): Unit = {

  }

  def resultadosMesHistorico(): Unit = {

  }

  def main(args: Array[String]) {

  }
}