package org.apache.spark.disc.execution.hcube.pull

import org.apache.spark.disc.catlog.Catalog.DataType
import org.apache.spark.disc.catlog.Relation
import org.apache.spark.disc.execution.hcube.{
  HCubeBlock,
  HCubeHelper,
  TupleHCubeBlock
}
import org.apache.spark.disc.util.misc.SparkSingle

class RelationPartitioner(relation: Relation, helper: HCubeHelper) {

  val sc = SparkSingle.getSparkContext()
  val partitioner = helper.partitionerForRelation(relation.schema.id.get)

  def partitionRelation(): PartitionedRelation = {
    val schema = relation.schema
    val sentry = helper.genSentry(schema.id.get)
    val sentryRDD = sc.parallelize(sentry)
    val relationRDD = relation.rdd.map(f => (f, false))

//    println(s"relationRDD:${relationRDD.collect().toSeq.map(f => (f._1.toSeq, f._2))}")
//    println(s"sentryRDD:${sentryRDD.collect().toSeq.map(f => (f._1.toSeq, f._2))}")

    val partitionedRDD = relationRDD.union(sentryRDD).partitionBy(partitioner)

    val hcubeBlockRDD = partitionedRDD.mapPartitions { it =>
      var shareVector: Array[Int] = null
      val content = it.toArray
      val array = new Array[Array[DataType]](content.size - 1)

      var j = 0
      var i = 0
      val contentSize = content.size
      while (j < contentSize) {
        val (tuple, isSentry) = content(j)
        if (isSentry) {
          shareVector = tuple.map(_.toInt)
        } else {
          array(i) = tuple
          i += 1
        }
        j += 1
      }

      Iterator(
        TupleHCubeBlock(schema, shareVector, array).asInstanceOf[HCubeBlock]
      )
    }

    //cache the hcubeBlockRDD in memory
//    hcubeBlockRDD.cache()
//    hcubeBlockRDD.count()

    PartitionedRelation(hcubeBlockRDD, partitioner)
  }
}
