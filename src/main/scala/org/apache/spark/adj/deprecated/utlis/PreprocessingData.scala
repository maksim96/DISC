package org.apache.spark.adj.deprecated.utlis

import org.apache.spark.adj.utils.misc.SparkSingle

class PreprocessingData {

  val sc = SparkSingle.getSparkContext()

  def removeComment(address: String, output: String): Unit = {
    val rawRDD = sc
      .textFile(address)
      .map { f =>
        var res: (Int, Int) = null
        if (!f.startsWith("#")) {
          val splittedString = f.split("\\s")
          res = (splittedString(0).toInt, splittedString(1).toInt)
        }

        res
      }
      .filter(f => f != null)
      .map(f => f._1.toString + "\t" + f._2.toString)

    rawRDD.saveAsTextFile(output)
  }
}
