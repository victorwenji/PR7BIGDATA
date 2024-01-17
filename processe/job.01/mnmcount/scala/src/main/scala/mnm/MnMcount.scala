// scalastyle:off println

package main.scala.chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object MnMcount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("MnMCount")
      .getOrCreate()

    if (args.length < 1) {
      println("Usage: MnMcount <mnm_file_dataset>")
      sys.exit(1)
    }

    val mnmFile = args(0)

    // Read the M&M file into a Spark DataFrame
    val mnmDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFile)

    // Display the M&M DataFrame
    mnmDF.show(5, false)

    // Read the JSON file into a DataFrame
    val datajour = spark.read
      .option("multiLine", "true")
      .option("mode", "DROPMALFORMED")
      .option("header", "true")
      .option("inferSchema", "true")
      .json(mnmFile)

    // Display the schema of the DataFrame
    datajour.printSchema()

    // tout les dates
    val dateDF = datajour
      .select(explode(col("list")).as("list_element"))
      .select("list_element.dt_txt")
    dateDF.show(40)

    // temperature minimum temperature
    val tempDF = datajour
      .select(explode(col("list")).as("list_element"))
      .select("list_element.main.temp_min")

    // Convertir temperature  Kelvin enCelsius
    val temperature = datajour
      .select(explode(col("list")).as("list_element"))
      .select(
        expr("CAST(list_element.main.temp_min AS DOUBLE) - 273.15")
          .as("celsius")
      )
    temperature.show(40)

    // Add an index column for the join
    val dateDFIndex = dateDF.withColumn("index", monotonically_increasing_id())
    val temperatureIndex =
      temperature.withColumn("index", monotonically_increasing_id())

    // Jointure des deux table
    val temp_dateJoin =
      dateDFIndex.join(temperatureIndex, Seq("index"), "inner").drop("index")

    // le jour le plus froid
    val dateFroid =
      temp_dateJoin.orderBy(asc("celsius")).select("dt_txt", "celsius")
    dateFroid.show(40)

    // Other commented-out code...
  }
}

// scalastyle:on println
