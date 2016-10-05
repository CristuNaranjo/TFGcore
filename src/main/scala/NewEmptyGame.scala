import CristuSpark._

/**
  * Created by NaranjO on 29/9/16.
  */
object NewEmptyGame {

  val abbr ="Boston Celtics - BOS\nBrooklyn (New Jersey) Nets - NJN\nNew York Knicks - NYK\nPhiladelphia 76ers - PHI\nToronto Raptors - TOR\nChicago Bulls - CHI\nCleveland Cavaliers - CLE\nDetroit Pistons - DET\nIndiana Pacers - IND\nMilwaukee Bucks - MIL\nAtlanta Hawks - ATL\nCharlotte Hornets - CHA\nMiami Heat - MIA\nOrlando Magic - ORL\nWashington Wizards - WAS\nDenver Nuggets - DEN\nMinnesota Timberwolves - MIN\nPortland Trail Blazers - POR\nOklahoma City Thunder - OKC\nUtah Jazz - UTA\nGolden State Warriors - GSW\nLos Angeles Clippers - LAC\nLos Angeles Lakers - LAL\nPhoenix Suns - PHO\nSacramento Kings - SAC\nDallas Mavericks - DAL\nHouston Rockets - HOU\nMemphis Grizzlies - MEM\nNew Orleans Pelicans - NOH\nSan Antonio Spurs - SAS\n"

  def main(args: Array[String]) {

    menu()

  }

  def menu(): Unit = {
    println("******************* MENU *******************")
    println("**                                        **")
    println("** 1. Ver abreciaciones de los equipos    **")
    println("** 2. Insertar nuevo partido              **")
    println("**                                        **")
    println("********************************************")
    val input = readLine()
    input match{
      case "1" => {
      println(abbr)
      menu()
    }
      case "2" => {
        nuevoPartido("")
    }
      case _ => {
        println("Seleccione una opción númerica")
        menu()
      }
    }
  }
  def nuevoPartido(abbrL: String) : Unit ={
    var abbrLtmp=""
    var abbrVtmp=""
    if(abbrL.isEmpty){
      println("Equipo local: (Inserta abreviación en mayúsculas)")
      abbrLtmp = readLine()
      if(!abbr.contains(abbrLtmp)){
        println("La abreviación es incorrecta, prueba con una de estas: \n")
        println(abbr)
        nuevoPartido("")
      }
    }else{
      abbrLtmp=abbrL
    }
    println("El equipo local es: " + abbrLtmp)
    println("Equipo visitante: (Inserta abreviación en mayúsculas)")
    abbrVtmp = readLine()
    if(!abbr.contains(abbrVtmp)){
      println("La abreviación es incorrecta, prueba con una de estas: ")
      println(abbr)
      nuevoPartido(abbrLtmp)
    }
    println("El equipo visitante es: " + abbrVtmp)
    println("Inserta jugadores del equipo local, 1 por línea, termina con espacio en blanco: ")
    var output = ""
    def read():Unit={
      val input = readLine()
      if(input.isEmpty) ()
      else {
        output += input+"\n"
        read
      }
    }
    read
    val localPlayers = output.split("\n")
    println("Inserta jugadores del equipo visitante, 1 por línea, termina con espacio en blanco: ")
    output = ""
    read
    val visitPlayers = output.split("\n")

    println("Fecha del partido: (Formato: AÑO-MES-DÍA) ")
    val inputFecha = readLine()
    val fecha = inputFecha.split("-")
    prediccion(fecha, abbrLtmp,localPlayers, abbrVtmp, visitPlayers)

    //crearPartido(fecha, abbrLtmp,localPlayers, abbrVtmp, visitPlayers)
  }
  def crearPartido(fecha: Array[String], abbrLtmp:String,localPlayers:Array[String], abbrVtmp:String, visitPlayers:Array[String]): Unit={
    var Json="{ 'box': [ "
  }

}
