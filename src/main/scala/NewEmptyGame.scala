/**
  * Created by NaranjO on 29/9/16.
  */
object NewEmptyGame {

  val abbr ="Boston Celtics - BOS\nBrooklyn (New Jersey) Nets - NJN\nNew York Knicks - NYK\nPhiladelphia 76ers - PHI\nToronto Raptors - TOR\nChicago Bulls - CHI\nCleveland Cavaliers - CLE\nDetroit Pistons - DET\nIndiana Pacers - IND\nMilwaukee Bucks - MIL\nAtlanta Hawks - ATL\nCharlotte Hornets - CHA\nMiami Heat - MIA\nOrlando Magic - ORL\nWashington Wizards - WAS\nDenver Nuggets - DEN\nMinnesota Timberwolves - MIN\nPortland Trail Blazers - POR\nOklahoma City Thunder - OKC\nUtah Jazz - UTA\nGolden State Warriors - GSW\nLos Angeles Clippers - LAC\nLos Angeles Lakers - LAL\nPhoenix Suns - PHO\nSacramento Kings - SAC\nDallas Mavericks - DAL\nHouston Rockets - HOU\nMemphis Grizzlies - MEM\nNew Orleans Pelicans - NOH\nSan Antonio Spurs - SAS\n"
  abstract class JSON
  case class JSeq (elems: List[JSON]) extends JSON
  case class JObj (bindings: Map[String, JSON]) extends JSON
  case class JNum (num: Double) extends JSON
  case class JStr (str: String) extends JSON
  case class JBool(b: Boolean) extends JSON
  case object JNull extends JSON

  def show(json: JSON): String = json match {
    case JSeq(elems) =>
  "[" + (elems map show mkString ",")+"]"
    case JObj(bindings) =>
      val assocs = bindings map {
        case (key, value) => "\"" + key + "\": " + show(value)
      }
  "{" + (assocs mkString ", ") + "}"
    case JNum(num) => num.toString
    case JStr(str) => "\"" + str + "\""
    case JBool(b) => b.toString
    case JNull => "null"
  }

  def main(args: Array[String]) {

    menu()


    /*

    {
      "_id": ObjectId("52f29f91ddbd75540aba6dae"),
      "box": [
      {
        "players": [
        {
          "ast": 9,
          "blk": 2,
          "drb": 8,
          "fg": 8,
          "fg3": 0,
          "fg3_pct": "",
          "fg3a": 0,
          "fg_pct": ".533",
          "fga": 15,
          "ft": 3,
          "ft_pct": ".750",
          "fta": 4,
          "mp": "41:00",
          "orb": 6,
          "pf": 3,
          "player": "Jeff Ruland",
          "pts": 19,
          "stl": 1,
          "tov": 5,
          "trb": 14
        },
        {
          "ast": 2,
          "blk": 1,
          "drb": 7,
          "fg": 9,
          "fg3": 0,
          "fg3_pct": "",
          "fg3a": 0,
          "fg_pct": ".643",
          "fga": 14,
          "ft": 4,
          "ft_pct": ".667",
          "fta": 6,
          "mp": "36:00",
          "orb": 1,
          "pf": 1,
          "player": "Cliff Robinson",
          "pts": 22,
          "stl": 3,
          "tov": 5,
          "trb": 8
        },
        {
          "ast": 4,
          "blk": 0,
          "drb": 0,
          "fg": 8,
          "fg3": 0,
          "fg3_pct": ".000",
          "fg3a": 2,
          "fg_pct": ".571",
          "fga": 14,
          "ft": 5,
          "ft_pct": "1.000",
          "fta": 5,
          "mp": "30:00",
          "orb": 0,
          "pf": 0,
          "player": "Gus Williams",
          "pts": 21,
          "stl": 2,
          "tov": 1,
          "trb": 0
        },
        {
          "ast": 1,
          "blk": 0,
          "drb": 2,
          "fg": 8,
          "fg3": 0,
          "fg3_pct": "",
          "fg3a": 0,
          "fg_pct": ".533",
          "fga": 15,
          "ft": 2,
          "ft_pct": ".667",
          "fta": 3,
          "mp": "30:00",
          "orb": 0,
          "pf": 2,
          "player": "Jeff Malone",
          "pts": 18,
          "stl": 0,
          "tov": 2,
          "trb": 2
        },
        {
          "ast": 2,
          "blk": 4,
          "drb": 6,
          "fg": 1,
          "fg3": 0,
          "fg3_pct": "",
          "fg3a": 0,
          "fg_pct": ".250",
          "fga": 4,
          "ft": 2,
          "ft_pct": "1.000",
          "fta": 2,
          "mp": "25:00",
          "orb": 2,
          "pf": 2,
          "player": "Charles Jones",
          "pts": 4,
          "stl": 0,
          "tov": 1,
          "trb": 8
        },
        {
          "ast": 1,
          "blk": 0,
          "drb": 4,
          "fg": 0,
          "fg3": 0,
          "fg3_pct": "",
          "fg3a": 0,
          "fg_pct": ".000",
          "fga": 5,
          "ft": 2,
          "ft_pct": ".500",
          "fta": 4,
          "mp": "26:00",
          "orb": 2,
          "pf": 4,
          "player": "Dan Roundfield",
          "pts": 2,
          "stl": 1,
          "tov": 2,
          "trb": 6
        },
        {
          "ast": 1,
          "blk": 0,
          "drb": 0,
          "fg": 3,
          "fg3": 0,
          "fg3_pct": "",
          "fg3a": 0,
          "fg_pct": ".750",
          "fga": 4,
          "ft": 1,
          "ft_pct": "1.000",
          "fta": 1,
          "mp": "20:00",
          "orb": 0,
          "pf": 2,
          "player": "Perry Moss",
          "pts": 7,
          "stl": 2,
          "tov": 1,
          "trb": 0
        },
        {
          "ast": 1,
          "blk": 0,
          "drb": 0,
          "fg": 0,
          "fg3": 0,
          "fg3_pct": "",
          "fg3a": 0,
          "fg_pct": "",
          "fga": 0,
          "ft": 0,
          "ft_pct": "",
          "fta": 0,
          "mp": "15:00",
          "orb": 0,
          "pf": 0,
          "player": "Dudley Bradley",
          "pts": 0,
          "stl": 1,
          "tov": 0,
          "trb": 0
        },
        {
          "ast": 0,
          "blk": 0,
          "drb": 2,
          "fg": 2,
          "fg3": 0,
          "fg3_pct": "",
          "fg3a": 0,
          "fg_pct": ".500",
          "fga": 4,
          "ft": 1,
          "ft_pct": "1.000",
          "fta": 1,
          "mp": "11:00",
          "orb": 0,
          "pf": 2,
          "player": "Darren Daye",
          "pts": 5,
          "stl": 1,
          "tov": 0,
          "trb": 2
        },
        {
          "ast": 0,
          "blk": 0,
          "drb": 0,
          "fg": 1,
          "fg3": 0,
          "fg3_pct": "",
          "fg3a": 0,
          "fg_pct": ".500",
          "fga": 2,
          "ft": 0,
          "ft_pct": "",
          "fta": 0,
          "mp": "3:00",
          "orb": 0,
          "pf": 1,
          "player": "Tom McMillen",
          "pts": 2,
          "stl": 0,
          "tov": 0,
          "trb": 0
        },
        {
          "ast": 0,
          "blk": 0,
          "drb": 0,
          "fg": 0,
          "fg3": 0,
          "fg3_pct": "",
          "fg3a": 0,
          "fg_pct": ".000",
          "fga": 1,
          "ft": 0,
          "ft_pct": "",
          "fta": 0,
          "mp": "3:00",
          "orb": 0,
          "pf": 2,
          "player": "Manute Bol",
          "pts": 0,
          "stl": 0,
          "tov": 0,
          "trb": 0
        }
        ],
        "team": {
          "ast": 21,
          "blk": 7,
          "drb": 29,
          "fg": 40,
          "fg3": 0,
          "fg3_pct": ".000",
          "fg3a": 2,
          "fg_pct": ".513",
          "fga": 78,
          "ft": 20,
          "ft_pct": ".769",
          "fta": 26,
          "mp": 240,
          "orb": 11,
          "pf": 19,
          "pts": 100,
          "stl": 11,
          "tov": 17,
          "trb": 40
        },
        "won": 1
      },
      {
        "players": [
        {
          "ast": 4,
          "blk": 0,
          "drb": 1,
          "fg": 15,
          "fg3": 0,
          "fg3_pct": ".000",
          "fg3a": 2,
          "fg_pct": ".517",
          "fga": 29,
          "ft": 2,
          "ft_pct": ".400",
          "fta": 5,
          "mp": "42:00",
          "orb": 5,
          "pf": 2,
          "player": "Dominique Wilkins",
          "pts": 32,
          "stl": 3,
          "tov": 5,
          "trb": 6
        },
        {
          "ast": 0,
          "blk": 3,
          "drb": 8,
          "fg": 4,
          "fg3": 0,
          "fg3_pct": "",
          "fg3a": 0,
          "fg_pct": ".500",
          "fga": 8,
          "ft": 1,
          "ft_pct": ".500",
          "fta": 2,
          "mp": "34:00",
          "orb": 1,
          "pf": 2,
          "player": "Tree Rollins",
          "pts": 9,
          "stl": 0,
          "tov": 1,
          "trb": 9
        },
        {
          "ast": 4,
          "blk": 2,
          "drb": 4,
          "fg": 3,
          "fg3": 0,
          "fg3_pct": "",
          "fg3a": 0,
          "fg_pct": ".375",
          "fga": 8,
          "ft": 2,
          "ft_pct": ".500",
          "fta": 4,
          "mp": "31:00",
          "orb": 2,
          "pf": 4,
          "player": "Cliff Levingston",
          "pts": 8,
          "stl": 1,
          "tov": 2,
          "trb": 6
        },
        {
          "ast": 10,
          "blk": 0,
          "drb": 3,
          "fg": 5,
          "fg3": 0,
          "fg3_pct": "",
          "fg3a": 0,
          "fg_pct": ".714",
          "fga": 7,
          "ft": 2,
          "ft_pct": "1.000",
          "fta": 2,
          "mp": "29:00",
          "orb": 0,
          "pf": 4,
          "player": "Spud Webb",
          "pts": 12,
          "stl": 1,
          "tov": 3,
          "trb": 3
        },
        {
          "ast": 0,
          "blk": 0,
          "drb": 5,
          "fg": 6,
          "fg3": 0,
          "fg3_pct": "",
          "fg3a": 0,
          "fg_pct": ".316",
          "fga": 19,
          "ft": 1,
          "ft_pct": ".333",
          "fta": 3,
          "mp": "22:00",
          "orb": 3,
          "pf": 3,
          "player": "Kevin Willis",
          "pts": 13,
          "stl": 0,
          "tov": 1,
          "trb": 8
        },
        {
          "ast": 3,
          "blk": 0,
          "drb": 1,
          "fg": 3,
          "fg3": 0,
          "fg3_pct": ".000",
          "fg3a": 1,
          "fg_pct": ".500",
          "fga": 6,
          "ft": 0,
          "ft_pct": "",
          "fta": 0,
          "mp": "24:00",
          "orb": 2,
          "pf": 0,
          "player": "Randy Wittman",
          "pts": 6,
          "stl": 0,
          "tov": 0,
          "trb": 3
        },
        {
          "ast": 1,
          "blk": 0,
          "drb": 3,
          "fg": 2,
          "fg3": 0,
          "fg3_pct": "",
          "fg3a": 0,
          "fg_pct": ".286",
          "fga": 7,
          "ft": 0,
          "ft_pct": "",
          "fta": 0,
          "mp": "23:00",
          "orb": 2,
          "pf": 0,
          "player": "Scott Hastings",
          "pts": 4,
          "stl": 2,
          "tov": 1,
          "trb": 5
        },
        {
          "ast": 0,
          "blk": 0,
          "drb": 1,
          "fg": 2,
          "fg3": 0,
          "fg3_pct": "",
          "fg3a": 0,
          "fg_pct": ".667",
          "fga": 3,
          "ft": 0,
          "ft_pct": "",
          "fta": 0,
          "mp": "10:00",
          "orb": 0,
          "pf": 0,
          "player": "Lorenzo Charles",
          "pts": 4,
          "stl": 0,
          "tov": 0,
          "trb": 1
        },
        {
          "ast": 0,
          "blk": 0,
          "drb": 2,
          "fg": 0,
          "fg3": 0,
          "fg3_pct": "",
          "fg3a": 0,
          "fg_pct": ".000",
          "fga": 3,
          "ft": 0,
          "ft_pct": "",
          "fta": 0,
          "mp": "10:00",
          "orb": 0,
          "pf": 3,
          "player": "Sedric Toney",
          "pts": 0,
          "stl": 0,
          "tov": 3,
          "trb": 2
        },
        {
          "ast": 3,
          "blk": 0,
          "drb": 0,
          "fg": 0,
          "fg3": 0,
          "fg3_pct": "",
          "fg3a": 0,
          "fg_pct": "",
          "fga": 0,
          "ft": 0,
          "ft_pct": "",
          "fta": 0,
          "mp": "9:00",
          "orb": 0,
          "pf": 1,
          "player": "Ray Williams",
          "pts": 0,
          "stl": 1,
          "tov": 0,
          "trb": 0
        },
        {
          "ast": 0,
          "blk": 0,
          "drb": 0,
          "fg": 1,
          "fg3": 0,
          "fg3_pct": "",
          "fg3a": 0,
          "fg_pct": ".500",
          "fga": 2,
          "ft": 1,
          "ft_pct": ".500",
          "fta": 2,
          "mp": "6:00",
          "orb": 1,
          "pf": 4,
          "player": "Jon Koncak",
          "pts": 3,
          "stl": 0,
          "tov": 0,
          "trb": 1
        }
        ],
        "team": {
          "ast": 25,
          "blk": 5,
          "drb": 28,
          "fg": 41,
          "fg3": 0,
          "fg3_pct": ".000",
          "fg3a": 3,
          "fg_pct": ".446",
          "fga": 92,
          "ft": 9,
          "ft_pct": ".500",
          "fta": 18,
          "mp": 240,
          "orb": 16,
          "pf": 23,
          "pts": 91,
          "stl": 8,
          "tov": 16,
          "trb": 44
        },
        "won": 0
      }
      ],
      "date": new Date("1985-10-25T05:00:00+0100"),
      "teams": [
      {
        "name": "Washington Bullets",
        "abbreviation": "WSB",
        "score": 100,
        "home": false,
        "won": 1
      },
      {
        "name": "Atlanta Hawks",
        "abbreviation": "ATL",
        "score": 91,
        "home": true,
        "won": 0
      }
      ]
    }
*/
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
        output += "\n"+input
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
    crearPartido(fecha, abbrLtmp,localPlayers, abbrVtmp, visitPlayers)
  }
  def crearPartido(fecha: Array[String], abbrLtmp:String,localPlayers:Array[String], abbrVtmp:String, visitPlayers:Array[String]): Unit={
    var Json="{ 'box': [ "
  }

}
