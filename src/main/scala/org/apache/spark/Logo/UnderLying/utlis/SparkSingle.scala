package org.apache.spark.Logo.UnderLying.utlis

import org.apache.spark.Logo.UnderLying.dataStructure._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.esotericsoftware.minlog.Log;
import com.esotericsoftware.minlog.Log.LEVEL_TRACE


/**
  * Simple Class which wrap around SparkContext, and SparkSession for easy testing
  */
object SparkSingle {



  private val conf = getConf()


  private def getConf(): Unit ={



    // ...
//    Log.set(LEVEL_TRACE)

    new SparkConf()
      .registerKryoClasses(Array(
        classOf[LogoSchema]
        ,classOf[CompositeLogoSchema]
        ,classOf[LogoMetaData]
        ,classOf[ConcretePatternLogoBlock]
        ,classOf[KeyValuePatternLogoBlock]
        ,classOf[CompositeTwoPatternLogoBlock]
        ,classOf[FilteringPatternLogoBlock[_]]
        ,classOf[PatternInstance]
        ,classOf[OneKeyPatternInstance]
        ,classOf[TwoKeyPatternInstance]
        ,classOf[ValuePatternInstance]
        ,classOf[KeyMapping]
      )
      )
  }

  private var spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("spark sql example")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  private var sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  var counter = 0

  def getSpark() = {
    counter += 1

    if (sc.isStopped){
      spark = SparkSession.builder().master("local[*]").appName("spark sql example").config("spark.some.config.option", "some-value")
        .getOrCreate()
    }
    (spark,sc)
  }

  def getSparkContext() = {
    getSpark()._2
  }

  def getSparkSession() = {
    getSpark()._1
  }

  def close(): Unit ={
//    counter -= 1
//    if(counter == 0){
//      spark.close()
//    }
  }

}
