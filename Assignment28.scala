import org.apache.spark.sql.SparkSession

object Assignment28 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local")
                .appName("Mlib Assignment")
                .config("spark.some.config.option", "some-value")
                .getOrCreate()
    val DelayedFlights = sparkSession.read.format("csv").option("header", "true").load("F:\\PDF Architect\\DelayedFlights.csv").toDF()
    println("Delayed_Flight Data->>" + DelayedFlights.count())

    DelayedFlights.createOrReplaceTempView("FlightDetails")
    DelayedFlights.show()

    val destination = sparkSession.sql("select Dest, count(Dest) from FlightDetails group by Dest order by count(Dest) desc").toDF()
    destination.show(5)

   val cancellations = sparkSession.sql("select Month , count(cancelled) from FlightDetails where CancellationCode = 'B' " +
                            "and Cancelled = 1 group by Month order by count(cancelled) desc").toDF()
     cancellations.show(1)

    val Route = sparkSession.sql("select Origin, Dest , count(Diverted) from FlightDetails where Diverted = 1 group by " +
                    "Origin, Dest order by count(Diverted) desc").toDF()
   Route.show(1)

  }
}