import java.sql.Timestamp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import com.mongodb.spark._
import org.bson.Document
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
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
    //Colecciono datos
    println("Aqui en prediccion....")
    val resultados = resultadosPrevios(fecha,abbrL, abbrV, localPlayers, visitPlayers)
    val resultLocales = resultados._1
    val resultVisitiantes = resultados._2
//    resultLocales.take(resultLocales.count().toInt).foreach(println)
//    resultVisitiantes.take(resultVisitiantes.count().toInt).foreach(println)
    //Filtro para cada jugador y covierto los datos en LabeledPoints
    val localesAgrupados =resultLocales.groupByKey()
    val localesLP = localesAgrupados.map(tuple => {
      val player = tuple._1
      val pts = tuple._2
      var cont = 0.0
      var arrayLBPts: Array[LabeledPoint] = Array()
      for (elem <- pts){
        val lpPts = LabeledPoint(cont, Vectors.dense(elem.toDouble))
        arrayLBPts :+= lpPts
        cont+=1
        println(cont)
      }
      arrayLBPts.foreach(println)
      (player,sc.parallelize(arrayLBPts))
    })
    //Para cada LabeledPoint realizo la regresion linear

    val dataModel = localesLP.map(tuple => {
      val player = tuple._1
      val parsedData = tuple._2
      // Building the model
      val numIterations = 100
      val stepSize = 0.00000001
      val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)
      (player,parsedData,model)
    })
    // Evaluate model on training examples and compute training error
    val valuesAndPreds = dataModel.map(tuple=>{
      val player = tuple._1
      val parsedData = tuple._2
      val model = tuple._3
      val values = parsedData.map(point => {
        val prediction = model.predict(point.features)
        (point.label,prediction)
      })
      (player, values)
    })

    val MSE = valuesAndPreds.map(tuple => {
      val player = tuple._1
      val values = tuple._2
      val MSEp = values.map{case(v, p) => math.pow((v - p), 2)}.mean()
      println("Player: " + player, "MSE: " + MSEp)
      (player, MSEp)
    })

    MSE.take(MSE.count().toInt).foreach(println)

    println("Saliendo de prediccion...")
}


  def resultadosPrevios(fecha: Array[String],local: String, visitante: String, localPlayers: Array[String], visitPlayers: Array[String]): (RDD[(String, Int)], RDD[(String, Int)]) = {
    var totalLocalResults: Array[(String, Int)] = Array()
    var totalVisitResults: Array[(String,Int)] = Array()

    //Filtrar mongoDB para obtener los resultados previos entre cada jugador y el equipo rival
    //Primero filtro para obtener los partidos del jugador
    //Locales
    for (i <- 0 until localPlayers.length) {
      var puntos: Array[Int] = Array()
      var player = localPlayers(i)
      println(player)
      val rddJugador = CristuSpark.rdd.withPipeline(Seq(Document.parse("{$match: { 'box.players.player': " + "'" + player + "'" + " }}"),
        Document.parse("{$unwind: '$box'}"),
        Document.parse("{$match: {'box.players.player':" + "'" + player + "'" + "}}"),
        Document.parse("{$unwind: '$box.players'}"),
        Document.parse("{$match: { 'box.players.player': " + "'" + player + "'" + " }}")))
      val dfJugador = rddJugador.toDF[Partido]
      dfJugador.cache()
//      dfJugador.printSchema()
//      println(dfJugador.count)
//      puntos.foreach(println)
      //Filtro para obtener los partidos anteriores contra ese equipo jugando como local
      val partidosVSVisitante = dfJugador.select("*").where(dfJugador("teams").getItem(1).getField("abbreviation") === visitante )
      val resultPlayer = partidosVSVisitante.select(partidosVSVisitante("box.players.pts"))
        .map(puntos => (player,puntos.getInt(0)))
      val resultMesPlayer = resultadosMes(fecha, dfJugador,player)
      val resultMesHisto = resultadosMesHistorico(fecha,dfJugador,player)
      val totalResult = sqlContext.createDataFrame(resultPlayer ++ resultMesPlayer ++ resultMesHisto)
        .withColumnRenamed("_1","player")
        .withColumnRenamed("_2","puntos")
      totalLocalResults = totalLocalResults ++ resultPlayer.collect() ++ resultMesPlayer.collect() ++ resultMesHisto.collect()
    }
    //Visitantes
    for (i <- 0 until visitPlayers.length) {
      var puntos: Array[Int] = Array()
      var player = visitPlayers(i)
      println(player)
      val rddJugador = CristuSpark.rdd.withPipeline(Seq(Document.parse("{$match: { 'box.players.player': " + "'" + player + "'" + " }}"),
        Document.parse("{$unwind: '$box'}"),
        Document.parse("{$match: {'box.players.player':" + "'" + player + "'" + "}}"),
        Document.parse("{$unwind: '$box.players'}"),
        Document.parse("{$match: { 'box.players.player': " + "'" + player + "'" + " }}")))
      val dfJugador = rddJugador.toDF[Partido]
      //Filtro para obtener los partidos anteriores contra ese equipo jugando como visitante
      val partidosVSLocal = dfJugador.select("*").where(dfJugador("teams").getItem(0).getField("abbreviation") === local )
      val resultPlayer = partidosVSLocal.select(partidosVSLocal("box.players.pts")).map(puntos => (player,puntos.getInt(0)))
//      resultPlayer.take(20).foreach(println)
      val resultMesPlayer = resultadosMes(fecha, dfJugador,player)
      val resultMesHisto = resultadosMesHistorico(fecha,dfJugador,player)
      val totalResult = sqlContext.createDataFrame(resultPlayer ++ resultMesPlayer ++ resultMesHisto)
        .withColumnRenamed("_1","player")
        .withColumnRenamed("_2","puntos")
      totalVisitResults = totalVisitResults ++ resultPlayer.collect() ++ resultMesPlayer.collect() ++ resultMesHisto.collect()
    }
    val localReturn = sc.parallelize(totalLocalResults)
    val visitReturn = sc.parallelize(totalVisitResults)
    (localReturn, visitReturn)

  }

  def resultadosMes(fecha: Array[String], dfJugador: DataFrame, player:String): RDD[(String, Int)] = {
    //RDD[(String,Int)]
    var dia = fecha(2).toInt
    var mes = fecha(1).toInt
    var año = fecha(0).toInt

    val filtroDia = "day(date)<"+dia
    val filtroMes = "month(date)="+mes
    val filtroAño = "year(date)="+año
    val resultMes = dfJugador.filter(filtroAño).filter(filtroMes).filter(filtroDia)
      .select("box.players.pts")
      .map(puntos=> (player,puntos.getInt(0)))
    if(dia < 7){
      val seleccion = 30-(7-dia)
      //Añado ultimos 30-7-dia resultados
      val ultimosResult = dfJugador.sort(desc("date")).select("box.players.pts")
        .take(seleccion)
        .map(puntos=> (player,puntos.getInt(0)))
      val resultados = sc.parallelize(resultMes.collect() ++ ultimosResult)
      resultados
    }else{
      resultMes
    }
  }

  def resultadosMesHistorico(fecha: Array[String], dfJugador: DataFrame, player:String): RDD[(String, Int)] = {
    val dia = fecha(2)
    val mes = fecha(1)
    val año = fecha(0)
    val filtroDia = "day(date)<="+dia
    val filtroMes = "month(date)="+mes
    val filtroAño = "year(date)<"+año
    val resultHistorico = dfJugador.filter(filtroAño).filter(filtroMes).filter(filtroDia)
      .select("box.players.pts")
      .map(puntos => (player,puntos.getInt(0)))
    resultHistorico
  }

  def main(args: Array[String]) {

  }
}