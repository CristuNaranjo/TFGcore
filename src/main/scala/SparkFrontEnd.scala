import java.io.{PrintWriter, FileWriter, File}
import java.sql.Timestamp
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import com.mongodb.spark._
import org.bson.Document
import org.apache.spark.sql.functions._

import scala.util.Try
/**
  * Created by NaranjO on 14/11/16.
  */
object SparkFrontEnd {

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
    //println("Aqui en prediccion....")
    val resultados = resultadosPrevios(fecha,abbrL, abbrV, localPlayers, visitPlayers)
    val resultLocales = resultados._1
    val resultVisitiantes = resultados._2
    resultLocales.cache()
    resultVisitiantes.cache()
    resultLocales.take(30).foreach(println)
    resultVisitiantes.take(30).foreach(println)
    //Prediccion para FrontEnd
    val localesAgrupados =resultLocales.groupByKey()
    val localesLP = localesAgrupados.map(tuple => {
      val player = tuple._1
      val pts = tuple._2
      var cont = 0.0
      var arrayLBPts: Array[LabeledPoint] = Array()
      for (elem <- pts){
        val lpPts = LabeledPoint(elem.toDouble, Vectors.dense(cont))
        arrayLBPts :+= lpPts
        cont+=1
        //println(cont)
      }
      //arrayLBPts.foreach(println)
      val dataFrame = sqlContext.createDataFrame(arrayLBPts)
      (player,dataFrame)
    })
    localesLP.cache();
    localesLP.take(30).foreach(println)
    //localesLP.foreach(println)
    //Para cada LabeledPoint realizo la regresion linear
    val dataModel = localesLP.map(tuple => {
      val player = tuple._1
      val parsedData = tuple._2
      // Building the model
      val linReg = new LinearRegression().setMaxIter(100).setFitIntercept(true)
      val model = linReg.fit(parsedData)
      //  train(parsedData, numIterations, stepSize)
      (player,parsedData,model)
    })
    dataModel.cache();
    dataModel.take(30).foreach(println)
    // Evaluate model on training examples and compute training error
    val valuesAndPreds = dataModel.map(tuple=>{
      val player = tuple._1
      val parsedData = tuple._2
      val model = tuple._3
      //model.
      val prediction = model.intercept + model.coefficients(0)*parsedData.count()
      (player,prediction)
    })
    valuesAndPreds.cache()
    valuesAndPreds.take(30).foreach(println)

    val visitantesAgrupados =resultVisitiantes.groupByKey()
    val visitantesLP = visitantesAgrupados.map(tuple => {
      val player = tuple._1
      val pts = tuple._2
      var cont = 0.0
      var arrayLBPtsV: Array[LabeledPoint] = Array()
      for (elem <- pts){
        val lpPtsV = LabeledPoint(elem.toDouble, Vectors.dense(cont))
        arrayLBPtsV :+= lpPtsV
        cont+=1
        //println(cont)
      }
      val dataFrameV = sqlContext.createDataFrame(arrayLBPtsV)
      //arrayLBPts.foreach(println)
      (player,dataFrameV)
    })
    visitantesLP.cache();
    //visitantesLP.foreach(println)
    //Para cada LabeledPoint realizo la regresion linear
    val dataModelV = visitantesLP.map(tuple => {
      val player = tuple._1
      val parsedData = tuple._2
      // Building the model
      val linReg = new LinearRegression().setMaxIter(100).setFitIntercept(true)
      val model = linReg.fit(parsedData)
      (player,parsedData,model)
    })
    dataModelV.cache()
    // Evaluate model on training examples and compute training error
    val valuesAndPredsV = dataModelV.map(tuple=>{
      val player = tuple._1
      val parsedData = tuple._2
      val model = tuple._3
      val prediction = model.intercept + model.coefficients(0)*parsedData.count()
      (player,prediction)
    })
    valuesAndPredsV.cache()
    valuesAndPredsV.take(30).foreach(println)

    writeResults(valuesAndPreds,valuesAndPredsV)

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
      val rddJugador = SparkFrontEnd.rdd.withPipeline(Seq(Document.parse("{$match: { 'box.players.player': " + "'" + player + "'" + " }}"),
        Document.parse("{$unwind: '$box'}"),
        Document.parse("{$match: {'box.players.player':" + "'" + player + "'" + "}}"),
        Document.parse("{$unwind: '$box.players'}"),
        Document.parse("{$match: { 'box.players.player': " + "'" + player + "'" + " }}")))
      val dfJugador = rddJugador.toDF[Partido]
      dfJugador.cache()
      //dfJugador.printSchema()
      //      println(dfJugador.count)
      //      puntos.foreach(println)
      //Filtro para obtener los partidos anteriores contra ese equipo jugando como local
      val partidosVSVisitante = dfJugador.select("*").where(dfJugador("teams").getItem(1).getField("abbreviation") === visitante ) //Añadir condicion, puede estar en item 0
      val resultPlayer = partidosVSVisitante.select(partidosVSVisitante("box.players.pts"))
          .map(puntos => (player,puntos.getInt(0)))
      val resultMesPlayer = resultadosMes(fecha, dfJugador,player)
      val resultMesHisto = resultadosMesHistorico(fecha,dfJugador,player)
      /*  val totalResult = sqlContext.createDataFrame(resultPlayer ++ resultMesPlayer ++ resultMesHisto)
          .withColumnRenamed("_1","player")
          .withColumnRenamed("_2","puntos")*/
      totalLocalResults = totalLocalResults ++ resultPlayer.collect() ++ resultMesPlayer.collect() ++ resultMesHisto.collect()
      println(totalLocalResults.length)
    }
    //Visitantes
    for (i <- 0 until visitPlayers.length) {
      var puntos: Array[Int] = Array()
      var player = visitPlayers(i)
      //println(player)
      val rddJugador = SparkFrontEnd.rdd.withPipeline(Seq(Document.parse("{$match: { 'box.players.player': " + "'" + player + "'" + " }}"),
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
      /*  val totalResult = sqlContext.createDataFrame(resultPlayer ++ resultMesPlayer ++ resultMesHisto)
          .withColumnRenamed("_1","player")
          .withColumnRenamed("_2","puntos")*/
      totalVisitResults = totalVisitResults ++ resultPlayer.collect() ++ resultMesPlayer.collect() ++ resultMesHisto.collect()
      println(totalVisitResults.length)
    }
    val localReturn = sc.parallelize(totalLocalResults)
    val visitReturn = sc.parallelize(totalVisitResults)
    localReturn.cache();
    visitReturn.cache();
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
  def writeResults(local:RDD[(String,Double)] , visit:RDD[(String,Double)] ): Unit = {
    val file = new File("/Users/NaranjO/Documents/TFG/MEAN/predictions.txt")
    val fw = new FileWriter(file);
    val pw = new PrintWriter(fw);
    local.take(20).foreach(tuple => {
      val player = tuple._1
      val pts = tuple._2
      val data = player + "-" + pts.toString + "\n"
      pw.write(data)
    })
    visit.take(20).foreach(tuple => {
      val player = tuple._1
      val pts = tuple._2
      val data = player + "-" + pts.toString + "\n"
      pw.write(data)
    })
    pw.close()
  }

}
