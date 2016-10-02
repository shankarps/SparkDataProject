package climate

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._



/**
 * Parse a dataset to identify the average rainfall for each year and print full list.
 */

object ClimateAnalyticsRainYearly {
    import org.apache.log4j.{Level, Logger}   
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)   
    Logger.getLogger("org").setLevel(Level.WARN)
    
    def main(args: Array[String])  {
      
      val conf = new SparkConf().setAppName("Climate1").setMaster("local[*]")
      
      val sc = new SparkContext(conf)
      
      val filename = "data/climate/Madras_1901_2015_full.csv"
      //val filename = "data/climate/Madras_1901_2015_rain.csv"
      //val filename = "data/climate/test_rain.csv"
      
      // Create a RDD of climate data
      val input = sc.textFile(filename)
      
      val header = input.first()
      
      var rawLines = input.filter(row => row != header)
      //rawLines.top(100).foreach(println)
      
      var lines = rawLines.map(x => {
                val tok = x.split(","); 
                var date = tok(5); 
                var prcp = tok(6).toFloat;
                if (prcp < 0) prcp = 0;
                (date, prcp)})
      
      //RDD[(String, Float)]
      var decades = lines.map(x => {(calcDecade(x._1), x._2)})
      
      //decades.top(100).foreach(println)
      
      //Calculate Averages
      var decadeVals = decades.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map(x => (x._1, (x._2._1 / x._2._2)))
      
      //Calculate total - not relevant
      //var decadeVals = decades.reduceByKey(_+_)
      
      //decadeVals.top(100).foreach(println)
      //decadeVals.saveAsTextFile("data/climate/output")
      
      var finalresult = decadeVals.sortBy(x => x._2, false).collect()
      
      finalresult.foreach(a => {
         println("%s, %.3f".format( a._1, a._2))
      })
       
    }
    
    def calcDecade(year : String): String ={
      //println(year)
      year.substring(0, 4)
    }
  
}