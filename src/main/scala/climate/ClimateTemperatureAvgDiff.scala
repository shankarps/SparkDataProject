package climate

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark._

/**
 * Parse the dataset to identify the average of the difference between the maximum and minimum temperatures for each year.
 */

object ClimateTemperatureAvgDiff {
  
    def main(args: Array[String])={
      val conf = new SparkConf().setMaster("local[*]").setAppName("Temperature_Analysis")
      
      val sc = new SparkContext(conf)
      
      val filename = "data/climate/Madras_1901_2015_full.csv"
      val input = sc.textFile(filename)
      val header = input.first()
      
      //skip the header
      val data =  input.filter(x => (x != header))
      
      //retrieve the date, max temp and min temp      
      val dateTemp = data.map(x => {
        var tok = x.split(",");
        var date = tok(5);
        var tmax = tok(16)
        var tmin = tok(21);
        (date, tmax, tmin)
      })
      
      //some temperature data is not valid. They are shown as -9999 or 9999. Skip any records that have these.
       val filteredData = dateTemp.filter(x => (x._2.indexOf("9999") == -1)).filter(x => (x._3.indexOf("9999") == -1))
       
       
       var filteredDataNum = filteredData.map(x => (x._1, x._2.toFloat, x._3.toFloat))
       
       var yearTemp = filteredDataNum.map(x => (parseYear(x._1), x._2, x._3))
       
       var tempDiff = yearTemp.map(x => (x._1, (x._2 - x._3))).map(x => (parseYear(x._1), x._2))
       
       var avgTempDiffByYear = tempDiff.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map(x => (x._1, (x._2._1 / x._2._2)))
       
       var sorted = avgTempDiffByYear.sortBy(x => x._1, true)
       
       var finalresult = sorted.collect()
       
       finalresult.foreach(a => {
         println("%s, %.2f".format( a._1, a._2))
       })
       
      
    }
    

    def parseYear(year : String): String ={
      //println(year)
      year.substring(0, 4)
    }
}