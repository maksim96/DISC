package org.apache.spark.adj.execution.misc

import org.apache.spark.adj.utils.misc.{Conf, SparkSingle}

class DataLoader(partitionSize: Int = Conf.defaultConf().getTaskNum()) {

  lazy val (_, sc) = SparkSingle.getSpark()
  lazy val spark = SparkSingle.getSparkSession()
//  var partitionSize = 4

  def csv(dataAddress: String) = {
    val rawDataRDD = sc.textFile(dataAddress).repartition(partitionSize)

    val relationRDD = rawDataRDD
      .map { f =>
        var res: Array[Int] = null
        if (!f.startsWith("#") && !f.startsWith("%")) {
          val splittedString = f.split("\\s")
          res = splittedString.map(_.toInt)
        }
        res
      }
      .filter(f => f != null)
      .map(f => (f(0), f(1)))
      .flatMap(f => Iterator(f, f.swap))
      .distinct()
      .map(f => Array(f._1, f._2))

    relationRDD.cache()
    relationRDD.count()

    relationRDD
  }
}
