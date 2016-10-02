package climate

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark._

object ClimateTemperatureMax {
  
    def main(args: Array[String])={
      val conf = new SparkConf().setMaster("local[*]").setAppName("Temperature_Analysis")
      
      val sc = new SparkContext(conf)
      
      val filename = "data/climate/Madras_1901_2015_full.csv"
      val input = sc.textFile(filename)
      val header = input.first()
      
      val data =  input.filter(x => (x != header))
      
     
      
      val dateTemp = data.map(x => {
        val tok = x.split(",");
        var date = tok(2);
        var tmax = tok(5)
        var tmin = tok(6);
        (date, tmax, tmin)
      })
      
       val filteredData = dateTemp.filter(x => (x._2.indexOf("9999") == -1))
       
       val filteredDataNum = filteredData.map(x => (x._1, x._2.toFloat, x._3.toFloat))
       
       val yearTemp = filteredDataNum.map(x => (parseYear(x._1), x._2, x._3))
       
       val tempMax = yearTemp.map(x => (x._1, x._2)).map(x => (parseYear(x._1), x._2))
       
       val avgTempMaxByYear = tempMax.reduceByKey(math.max(_, _))
       
      //Convert to Celsius if temperature was in Fahrenheit. 
       //val avgTempMaxByYear = tempMax.reduceByKey(math.max(_, _)).map(x => (x._1, ((x._2 - 32) * 5/9)))
       
       val sorted = avgTempMaxByYear.sortBy(x => x._1, true)
       
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