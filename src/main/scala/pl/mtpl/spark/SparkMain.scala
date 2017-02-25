package pl.mtpl.spark

/**
  * Created by MarcinT.P on 2017-02-24.
  */
object SparkMain {
  var cmd : Option[String] = None
  var fname : Option[String] = None
  var cnt : Option[Int] = None

  def main(args: Array[String]) : Unit = {
    for(i <- args.indices) {
      i match {
        case 0 => cmd = Some(args(i))
        case 1 => fname = Some(args(i))
        case 2 => cnt = Some(args(i).toInt)
      }
    }

    if(cmd.isDefined) {
      println(s"Executing command ${cmd.get}")
      cmd.get match {
        case "gen" => new CSVGenerator().generate(fname.get, cnt.get)
        case "save-to-avro" => new AvroController().save(fname.get)
        case _ => println("gen|save-to-avro")
      }
    } else println("Gimme the command, fucker!")
  }
}
