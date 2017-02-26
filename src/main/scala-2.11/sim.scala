import java.net.URL
import java.nio.charset.Charset
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 2/24/17.
  */
object sim {

  case class Caserne(x: String, y: String, Name: Integer, Description: String)


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SIMDataAnalysis").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sc.setLogLevel("OFF")

    val caserneTxt = sc.parallelize(
      IOUtils.toString(
        new URL("http://donnees.ville.montreal.qc.ca/dataset/c69e78c6-e454-4bd9-9778-e4b0eaf8105b/resource/f6542ad1-31f5-458e-b33d-1a028fab3e98/download/casernessim.csv"),
        Charset.forName("utf8")).split("\n"))

    println("Fichier contient: " + caserneTxt.count() + " Lignes")
    println("Nombre de partitions: " + caserneTxt.partitions.size)

    import sqlContext.implicits._

    val header = caserneTxt.first()
    val no_header = caserneTxt.filter(_ (0) != header(0))
    val caserne = no_header.map(s => s.split(",")).map(
      s => Caserne(s(0),
        s(1),
        s(2).replaceAll("[^\\d]", "").toInt,
        s(3).replaceAll("""<(?!\/?a(?=>|\s.*>))\/?.*?>""", " ").trim()
      )
    ) toDF()

    // Create temporary table
    caserne.registerTempTable("caserneTemp")
    println("Table Temporaire")
    println("================")
    sqlContext.sql("Select * from caserneTemp").show()

    val df = caserne.select($"X", $"Y", $"Name", $"Description").cache()
    println("Save table Temporaire in the cache")
    df.show()

    // Save ORC file
    df.write.format("orc").mode(SaveMode.Overwrite).save("SIM.orc")

    // Load from ORC file
    val dfTest = sqlContext.read.format("orc").load("SIM.orc")
    println("Table loaded from ORC file")
    println("==========================")
    dfTest.show()

    // Compare DataFrame sizes (i.e. Original DataFrame with Loaded from HDFS)
    assert(dfTest.count() == df.count(), println("Assertion Failed"))

    // Save to a table
    df.write.format("orc").mode(SaveMode.Overwrite).saveAsTable("casernePerm")
    println("Table Permanente")
    println("================")
    sqlContext.table("casernePerm").show()

    // Show tables
    sqlContext.sql("show tables").show()
  }

}

