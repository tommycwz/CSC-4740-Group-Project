package testpackage

/*
 *  This program will show how the TV show "The Queen's Gambit" change the world of chess.
 *  This program will depicts how many people plays the Queen's Gambit Opening before and
 *  after the show first aired in all different Rated Match.
 */

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job

import java.text.SimpleDateFormat
import java.util.Date
import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

object test1 {
  def main(args: Array[String]): Unit = {
    //Date
    val format = new java.text.SimpleDateFormat("yyyy.MM.dd")
    val strDate = "2020.10.23"
    val targetDate = format.parse(strDate)

    //Default Settings
    val conf = new SparkConf().setAppName("Test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("textinputformat.record.delimiter", "[Event")

    //Delete File When Start
    FileUtils.deleteQuietly(new File("output_rated"))

    //INPUT FILE
    val getFile = sc.textFile("lichess_db_standard_rated_2020-10.pgn").filter(b => b.length > 0)


    try{
      //Filter all the blocks that contain Queens's Gambit Opening and split it.
      val mapData = getFile.filter(line => line.contains("Queen's Gambit")).map(line =>{
        val spt = line.split("]")
        val event = spt(0).replaceAll("[\\[\\]\"]","")
        val utc = spt(5).replaceAll("UTCDate", "").replaceAll("[\\[\\]\"\\s]","")

       spt + " " + event + " " + utc
      })

      mapData.saveAsTextFile("test_out")
    }
    catch{
      case ex: Exception =>{
        println("Error!")
      }
    }

  }

}
