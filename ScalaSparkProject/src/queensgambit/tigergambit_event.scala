package queensgambit

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
import org.apache.spark.{SparkContext, SparkConf}


object tigergambit_event {
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
    val getFile = sc.textFile("lichess_db_standard_rated_2013-01.pgn").filter(b => b.length > 0)

    //Filter all the blocks that contain Quess's Gambit Opening and split it.
    val mapData = getFile.filter(line => line.contains("Queen's Gambit")).map(line =>{
      val spt = line.split("]")
      val event = spt(0).replaceAll("[\\[\\]\"]","")
      val utc = spt(5).replaceAll("UTCDate", "").replaceAll("[\\[\\]\"\\s]","")

      //Check if Date Before the show
      if(format.parse(utc).compareTo(targetDate) < 0) {
        if(event.contains("Rated Blitz tournament")){
          ("Before: Rated Blitz tournament", 1)
        }
        else if(event.contains("Rated Bullet tournament")){
          ("Before: Rated Bullet tournament", 1)
        }
        else if(event.contains("Rated Rapid tournament")){
          ("Before: Rated Rapid tournament", 1)
        }
        else if(event.contains("Rated Classical tournament")){
          ("Before: Rated Classical tournament", 1)
        }
        else if(event.contains("Rated correspondence tournament")){
          ("Before: Rated correspondence tournament", 1)
        }
        else {
          ("Before:" + event, 1)
        }
      }
      //Else will be after the show
      else{
        if(event.contains("Rated Blitz tournament")){
          ("After: Rated Blitz tournament", 1)
        }
        else if(event.contains("Rated Bullet tournament")){
          ("After: Rated Bullet tournament", 1)
        }
        else if(event.contains("Rated Rapid tournament")){
          ("After: Rated Rapid tournament", 1)
        }
        else if(event.contains("Rated Classical tournament")){
          ("After: Rated Classical tournament", 1)
        }
        else if(event.contains("Rated correspondence tournament")){
          ("After: Rated correspondence tournament", 1)
        }
        else {
          ("After:" + event, 1)
        }
      }
    })

    val reduceData = mapData.reduceByKey((v1, v2) => v1 + v2).sortByKey(ascending=true)
    val finalData = reduceData.coalesce(1)
    finalData.saveAsTextFile("output_event")
  }

}
