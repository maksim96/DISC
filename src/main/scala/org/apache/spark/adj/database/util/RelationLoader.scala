package org.apache.spark.adj.database.util

import org.apache.spark.adj.database.RelationSchema
import org.apache.spark.adj.utlis.SparkSingle
import org.apache.spark.storage.StorageLevel

import scala.util.Random

class RelationLoader {

  lazy val (_, sc) = SparkSingle.getSpark()
  lazy val spark = SparkSingle.getSparkSession()
  var partitionSize = 4

  def csv(dataAddress:String, name:String, attrs:Seq[String]) = {
    val rawDataRDD = sc.textFile(dataAddress).repartition(partitionSize)

    val relationRDD = rawDataRDD.map {
      f =>
        var res: Array[Int] = null
        if (!f.startsWith("#") && !f.startsWith("%")) {
          val splittedString = f.split("\\s")
          res = splittedString.map(_.toInt)
        }
        res
    }.filter(f => f != null)

    relationRDD.cache()

    relationRDD
  }
}