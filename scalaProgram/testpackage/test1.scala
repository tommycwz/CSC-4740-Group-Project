package testpackage

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkContext, SparkConf}


object test1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("textinputformat.record.delimiter", "[Event")
//    val getFile = sc.textFile("input.txt").filter(b => b.length > 0)
    val getFile = sc.textFile("lichess_db_standard_rated_2013-01.pgn").filter(b => b.length > 0)
//    val sptData = getFile.map(line => line.split("]")(0).replaceAll("[\\[\\]\"]","") + line.split("]")(5).replaceAll("UTCDate", "").replaceAll("[\\[\\]\"]","") + line.split("]")(12).replaceAll("Opening", "").replaceAll("[\\[\\]\"]",""))
    val mapData = getFile.map(line =>{
        if(line.split("]")(12).contains("Queen's Gambit")){
//          line.split("]")(12).replaceAll("Opening", "").replaceAll("[\\[\\]\"]","") + line.split("]")(0).replaceAll("[\\[\\]\"]","") + line.split("]")(5).replaceAll("UTCDate", "").replaceAll("[\\[\\]\"]","")
          if()
        }
    })

    mapData.collect().foreach(println)
  }
}
