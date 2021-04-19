package queensgambit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import java.text.SimpleDateFormat
import java.util.Date
import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkContext, SparkConf}

object tigergambit_rated {
  def main(args: Array[String]): Unit = {
    //Date
    val format = new java.text.SimpleDateFormat("yyyy.MM.dd")
    val strDate = "2020.10.23"
    val targetDate = format.parse(strDate)

    //Default Settings
    val conf = new SparkConf().setAppName("TigerGambit_Rated").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("textinputformat.record.delimiter", "[Event")

    //Delete File When Start
    FileUtils.deleteQuietly(new File("output_rated_final"))

    //INPUT FILE
    val getFile = sc.textFile("lichess_db_standard_rated_2020-10.pgn").filter(b => b.length > 0)

    //Filter all the blocks that contain proper rating and split it.
    val mapData = getFile.filter(line => !line.contains("?")).map(line =>{
      val spt = line.split("]")
      val whiteElo = spt(9).replaceAll("[\\[\\]\"\\s]","").replaceAll("WhiteElo","").replaceAll("\\?","")
      val blackElo = spt(10).replaceAll("[\\[\\]\"\\s]","").replaceAll("BlackElo","").replaceAll("\\?","")
      val utc = spt(2).replaceAll("Date", "").replaceAll("[\\[\\]\"\\s]","")
      whiteElo + " " + blackElo + " " + utc
    })

    //Get White Data
    val whiteData = mapData.map(line =>{
      val whiteSpt = line.split(" ")
      val whiteElo = whiteSpt(0).toInt
      val utc = whiteSpt(2)

      //Get into their Range
      if(format.parse(utc).compareTo(targetDate) < 0) {
        if(whiteElo <= 1200 ){("Before -> White Range: <=1200", 1)}
        else if(whiteElo >= 1201 && whiteElo <= 1500){("Before -> White Range: 1201 - 1500", 1)}
        else if(whiteElo >= 1501 && whiteElo <= 1800){("Before -> White Range: 1501 - 1800", 1)}
        else if(whiteElo >= 1801 && whiteElo <= 2000){("Before -> White Range: 1801 - 2000", 1)}
        else if(whiteElo >= 2001 && whiteElo <= 2200){("Before -> White Range: 2001 - 2200", 1)}
        else if(whiteElo >= 2201 && whiteElo <= 2400){("Before -> White Range: 2201 - 2400", 1)}
        else if(whiteElo >= 2401 && whiteElo <= 2600){("Before -> White Range: 2401 - 2600", 1)}
        else if(whiteElo >= 2601 && whiteElo <= 2800){("Before -> White Range: 2601 - 2800", 1)}
        else if(whiteElo >= 2801 && whiteElo <= 3000){("Before -> White Range: 2801 - 3000", 1)}
        else if(whiteElo > 3000){("Before -> White Range: >3000", 1)}
        else{("Error on: Before White" + whiteElo , 1)}
      }
      else{
        if(whiteElo <= 1200 ){("After -> White Range: <=1200", 1)}
        else if(whiteElo >= 1201 && whiteElo <= 1500){("After -> White Range: 1201 - 1500", 1)}
        else if(whiteElo >= 1501 && whiteElo <= 1800){("After -> White Range: 1501 - 1800", 1)}
        else if(whiteElo >= 1801 && whiteElo <= 2000){("After -> White Range: 1801 - 2000", 1)}
        else if(whiteElo >= 2001 && whiteElo <= 2200){("After -> White Range: 2001 - 2200", 1)}
        else if(whiteElo >= 2201 && whiteElo <= 2400){("After -> White Range: 2201 - 2400", 1)}
        else if(whiteElo >= 2401 && whiteElo <= 2600){("After -> White Range: 2401 - 2600", 1)}
        else if(whiteElo >= 2601 && whiteElo <= 2800){("After -> White Range: 2601 - 2800", 1)}
        else if(whiteElo >= 2801 && whiteElo <= 3000){("After -> White Range: 2801 - 3000", 1)}
        else if(whiteElo > 3000){("After -> White Range: >3000", 1)}
        else{("Error on: After White" + whiteElo, 1)}
      }
    } )

    //Get Black Data
    val blackData = mapData.map(line =>{
      val blackSpt = line.split(" ")
      val blackElo = blackSpt(1).toInt
      val utc = blackSpt(2)

      //Get into their Range
      if(format.parse(utc).compareTo(targetDate) < 0) {
        if(blackElo <= 1200 ){("Before -> Black Range: <=1200", 1)}
        else if(blackElo >= 1201 && blackElo <= 1500){("Before -> Black Range: 1201 - 1500", 1)}
        else if(blackElo >= 1501 && blackElo <= 1800){("Before -> Black Range: 1501 - 1800", 1)}
        else if(blackElo >= 1801 && blackElo <= 2000){("Before -> Black Range: 1801 - 2000", 1)}
        else if(blackElo >= 2001 && blackElo <= 2200){("Before -> Black Range: 2001 - 2200", 1)}
        else if(blackElo >= 2201 && blackElo <= 2400){("Before -> Black Range: 2201 - 2400", 1)}
        else if(blackElo >= 2401 && blackElo <= 2600){("Before -> Black Range: 2401 - 2600", 1)}
        else if(blackElo >= 2601 && blackElo <= 2800){("Before -> Black Range: 2601 - 2800", 1)}
        else if(blackElo >= 2801 && blackElo <= 3000){("Before -> Black Range: 2801 - 3000", 1)}
        else if(blackElo > 3000){("Before -> Black Range: >3000", 1)}
        else{("Error on: Before Black" + blackElo, 1)}
      }
      else{
        if(blackElo <= 1200 ){("After -> Black Range: <=1200", 1)}
        else if(blackElo >= 1201 && blackElo <= 1500){("After -> Black Range: 1201 - 1500", 1)}
        else if(blackElo >= 1501 && blackElo <= 1800){("After -> Black Range: 1501 - 1800", 1)}
        else if(blackElo >= 1801 && blackElo <= 2000){("After -> Black Range: 1801 - 2000", 1)}
        else if(blackElo >= 2001 && blackElo <= 2200){("After -> Black Range: 2001 - 2200", 1)}
        else if(blackElo >= 2201 && blackElo <= 2400){("After -> Black Range: 2201 - 2400", 1)}
        else if(blackElo >= 2401 && blackElo <= 2600){("After -> Black Range: 2401 - 2600", 1)}
        else if(blackElo >= 2601 && blackElo <= 2800){("After -> Black Range: 2601 - 2800", 1)}
        else if(blackElo >= 2801 && blackElo <= 3000){("After -> Black Range: 2801 - 3000", 1)}
        else if(blackElo > 3000){("After -> Black Range: >3000", 1)}
        else{("Error on: After Black " + blackElo, 1)}
      }
    } )

    val mergeData = whiteData.union(blackData)
    val reduceData = mergeData.reduceByKey((v1, v2) => v1 + v2).sortByKey(ascending=true)
    val finalData = reduceData.coalesce(1)
    finalData.saveAsTextFile("output_rated_final")
  }
}
