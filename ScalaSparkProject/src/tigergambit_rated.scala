package queensgambit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.{SparkContext, SparkConf}


object tigergambit_rated {
  def main(args: Array[String]): Unit = {
    //Date
    val format = new java.text.SimpleDateFormat("yyyy.MM.dd")
    val strDate = "2013.1.15"
    val targetDate = format.parse(strDate)

    //Default Settings
    val conf = new SparkConf().setAppName("Test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("textinputformat.record.delimiter", "[Event")

    //INPUT FILE
    val getFile = sc.textFile("input.txt").filter(b => b.length > 0)
//    val getFile = sc.textFile("lichess_db_standard_rated_2013-01.pgn").filter(b => b.length > 0)

    //Filter all the blocks that contain Quess's Gambit Opening and split it.
    val mapData = getFile.map(line =>{
      val spt = line.split("]")
      val whiteElo = spt(7).replaceAll("[\\[\\]\"\\s]","").replaceAll("WhiteElo","").toInt
      val blackElo = spt(8).replaceAll("[\\[\\]\"\\s]","").replaceAll("BlackElo","").toInt
      val utc = spt(5).replaceAll("UTCDate", "").replaceAll("[\\[\\]\"\\s]","")

      //Check if Date Before the show
      if(format.parse(utc).compareTo(targetDate) < 0) {
        //White
        if(whiteElo < 1200 ){("Before -> White Range: <1200", 1)}
        else if(whiteElo >= 1201 && whiteElo <= 1500){("Before -> White Range: 1201 - 1500", 1)}
        else if(whiteElo >= 1501 && whiteElo <= 1800){("Before -> White Range: 1501 - 1800", 1)}
        else if(whiteElo >= 1801 && whiteElo <= 2000){("Before -> White Range: 1801 - 2000", 1)}
        else if(whiteElo >= 2001 && whiteElo <= 2200){("Before -> White Range: 2001 - 2200", 1)}
        else if(whiteElo >= 2201 && whiteElo <= 2400){("Before -> White Range: 2201 - 2400", 1)}
        else if(whiteElo >= 2401 && whiteElo <= 2600){("Before -> White Range: 2401 - 2600", 1)}
        else if(whiteElo >= 2601 && whiteElo <= 2800){("Before -> White Range: 2601 - 2800", 1)}
        else if(whiteElo >= 2801 && whiteElo <= 3000){("Before -> White Range: 2801 - 3000", 1)}
        else if(whiteElo > 3000){("Before -> White Range: >3000", 1)}

        //Black
        else if(blackElo < 1200 ){("Before -> Black Range: <1200", 1)}
        else if(blackElo >= 1201 && blackElo <= 1500){("Before -> Black Range: 1201 - 1500", 1)}
        else if(blackElo >= 1501 && blackElo <= 1800){("Before -> Black Range: 1501 - 1800", 1)}
        else if(blackElo >= 1801 && blackElo <= 2000){("Before -> Black Range: 1801 - 2000", 1)}
        else if(blackElo >= 2001 && blackElo <= 2200){("Before -> Black Range: 2001 - 2200", 1)}
        else if(blackElo >= 2201 && blackElo <= 2400){("Before -> Black Range: 2201 - 2400", 1)}
        else if(blackElo >= 2401 && blackElo <= 2600){("Before -> Black Range: 2401 - 2600", 1)}
        else if(blackElo >= 2601 && blackElo <= 2800){("Before -> Black Range: 2601 - 2800", 1)}
        else if(blackElo >= 2801 && blackElo <= 3000){("Before -> Black Range: 2801 - 3000", 1)}
        else if(blackElo > 3000){("Before -> Black Range: >3000", 1)}
        else{("Error on: Before", 1)}
      }

      //Else will be after the show
      else{
        //White
        if(whiteElo < 1200 ){("After -> White Range: <1200", 1)}
        else if(whiteElo >= 1201 && whiteElo <= 1500){("After -> White Range: 1201 - 1500", 1)}
        else if(whiteElo >= 1501 && whiteElo <= 1800){("After -> White Range: 1501 - 1800", 1)}
        else if(whiteElo >= 1801 && whiteElo <= 2000){("After -> White Range: 1801 - 2000", 1)}
        else if(whiteElo >= 2001 && whiteElo <= 2200){("After -> White Range: 2001 - 2200", 1)}
        else if(whiteElo >= 2201 && whiteElo <= 2400){("After -> White Range: 2201 - 2400", 1)}
        else if(whiteElo >= 2401 && whiteElo <= 2600){("After -> White Range: 2401 - 2600", 1)}
        else if(whiteElo >= 2601 && whiteElo <= 2800){("After -> White Range: 2601 - 2800", 1)}
        else if(whiteElo >= 2801 && whiteElo <= 3000){("After -> White Range: 2801 - 3000", 1)}
        else if(whiteElo > 3000){("After -> White Range: >3000", 1)}

        //Black
        else if(blackElo < 1200 ){("After -> Black Range: <1200", 1)}
        else if(blackElo >= 1201 && blackElo <= 1500){("After -> Black Range: 1201 - 1500", 1)}
        else if(blackElo >= 1501 && blackElo <= 1800){("After -> Black Range: 1501 - 1800", 1)}
        else if(blackElo >= 1801 && blackElo <= 2000){("After -> Black Range: 1801 - 2000", 1)}
        else if(blackElo >= 2001 && blackElo <= 2200){("After -> Black Range: 2001 - 2200", 1)}
        else if(blackElo >= 2201 && blackElo <= 2400){("After -> Black Range: 2201 - 2400", 1)}
        else if(blackElo >= 2401 && blackElo <= 2600){("After -> Black Range: 2401 - 2600", 1)}
        else if(blackElo >= 2601 && blackElo <= 2800){("After -> Black Range: 2601 - 2800", 1)}
        else if(blackElo >= 2801 && blackElo <= 3000){("After -> Black Range: 2801 - 3000", 1)}
        else if(blackElo > 3000){("After -> Black Range: >3000", 1)}
        else{("Error on: Before", 1)}
      }
    })

    mapData.collect().foreach(println)
//    val reduceData = mapData.reduceByKey((v1, v2) => v1 + v2).sortByKey(ascending=true)
//    val finalData = reduceData.coalesce(1)
//    finalData.saveAsTextFile("output_rated")
  }
}
