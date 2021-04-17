package queensgambit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.{SparkContext, SparkConf}


object queensGambit {
  def main(args: Array[String]): Unit = {
    //Date
    val format = new java.text.SimpleDateFormat("yyyy.MM.dd")
    val strDate = "2013.10.23"
    val date = format.parse(strDate)

    //Default Settings
    val conf = new SparkConf().setAppName("Test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("textinputformat.record.delimiter", "[Event")

    //INPUT FILE
    //    val getFile = sc.textFile("input.txt").filter(b => b.length > 0)
    val getFile = sc.textFile("lichess_db_standard_rated_2013-01.pgn").filter(b => b.length > 0)

    val mapData = getFile.map(line =>{
      val opening = line.split("]")(12).replaceAll("Opening", "").replaceAll("[\\[\\]\"]","")
      val event = line.split("]")(0).replaceAll("[\\[\\]\"]","")
      val utc = line.split("]")(5).replaceAll("UTCDate", "").replaceAll("[\\[\\]\"\\s]","")

      //Target the Queen's Gambit Opening
      if(opening.contains("Queen's Gambit")){
        //Check if Date Before the show
        if(format.parse(utc).compareTo(date) < 0){
          ("Before", event)
        }
        //Else will be after the show
        else{
          ("After", event)
        }
      }
    })

//    mapData.collect().foreach(println)
    //      mapData.reduceByKey((v1,v2) => v1+v2)
          mapData.saveAsTextFile("output")
  }
}
