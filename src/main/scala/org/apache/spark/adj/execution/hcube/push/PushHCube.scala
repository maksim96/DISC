package org.apache.spark.adj.execution.hcube.push

import org.apache.spark.adj.database.Catalog.DataType
import org.apache.spark.adj.database.Relation
import org.apache.spark.adj.execution.hcube.{
  HCubeHelper,
  HCubePartitioner,
  TupleHCubeBlock
}
import org.apache.spark.adj.execution.hcube.pull.HCubePlan
import org.apache.spark.adj.execution.subtask.{SubTask, TaskInfo}
import org.apache.spark.adj.utils.misc.{Conf, SparkSingle}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer

class PushHCube(@transient query: HCubePlan, info: TaskInfo)
    extends Serializable {

  @transient val sc = SparkSingle.getSparkContext()
  @transient val shareMap = query.share
  val shareSeq = shareMap.toIndexedSeq
  val shareSpaceVector = shareSeq.map(_._2).toArray
  @transient val helper = new HCubeHelper(query)
  val partitioner = new HCubePartitioner(shareSpaceVector.toArray)
  @transient val relations = query.relations
  val contents = relations.map(_.rdd)
  val schemas = relations.map(_.schema)
  val keyToSchema = schemas.map(f => (f.id.get, f)).toMap

  def genCordinateRDD(relation: Relation) = {

    //init
    val rdd = relation.rdd
    val schema = relation.schema
    val attrIds = schema.attrIDs
    val tupleSize = attrIds.size
    val attrIdsLocalPosToGlobalPos = attrIds.map { idx =>
      shareSeq.map(_._1).indexOf(idx)
    }
    //here we need to make sure that the relationschema has an id
    val relationId = relation.schema.id.get
    val totalAttrsSize = shareSpaceVector.size
    val globalPosToAttrIdsLocalPosMap =
      attrIdsLocalPosToGlobalPos.zipWithIndex.toMap

//    println(
//      s"shareSeq:${shareSeq}, attrIdsLocalPosToGlobalPos:${attrIdsLocalPosToGlobalPos}, globalPosToAttrIdsLocalPosMap:${globalPosToAttrIdsLocalPosMap} "
//    )
//
//    println(s"all tuples:${relation.rdd.collect().map(_.toSeq).toSeq}")
    rdd.flatMap { tuple =>
      //find the hash values for each attribute of the tuple
      val hashValues = new Array[DataType](tupleSize)
      var i = 0
      while (i < tupleSize) {
        hashValues(i) = Utils.nonNegativeMod(
          tuple(i),
          shareSpaceVector(attrIdsLocalPosToGlobalPos(i))
        )
        i += 1
      }

      //fill the hash values for all attributes
      val locationForEachAttrs = Range(0, totalAttrsSize).map { idx =>
        if (globalPosToAttrIdsLocalPosMap.contains(idx)) {
          Array(hashValues(globalPosToAttrIdsLocalPosMap(idx)))
        } else {
          Range(0, shareSpaceVector(idx)).toArray
        }
      }.toArray

      //gen the location to be sent for the tuple
      var locations = locationForEachAttrs(0).map(Array(_))

      i = 1
      while (i < totalAttrsSize) {
        locations = locationForEachAttrs(i).flatMap { value =>
          locations.map(location => location :+ value)
        }
        i += 1
      }

      val locationForTuple =
        locations.map(location => partitioner.getPartition(location))

//      println(s"tuplesAndLocations:${locations
//        .map(locationId => (locationId.toSeq, (relationId, tuple.toSeq)))
//        .toSeq}")

      locationForTuple.map(locationId => (locationId, (relationId, tuple)))
    }
  }

//  def genSentryRDD(): RDD[(Int, (Int, Array[DataType]))] = {
//    val sentry = helper.genShareForAttrs(shareSeq.map(_._1))
//    val rdd = sc.parallelize(sentry)
//    rdd.map(
//      location => (partitioner.getPartition(location), (Int.MaxValue, null))
//    )
//  }

  def genHCubeRDD(): RDD[SubTask] = {

    val relationsWithLocation = relations.map(genCordinateRDD)
    val rdds = relationsWithLocation

    //generate RDD[SubTask]
    sc.union(rdds).groupByKey(Conf.defaultConf().taskNum).map {
      case (key, tuples) =>
        val localShare = partitioner.getShare(key)

//        val localShare = key.toArray

//        println(s"localShare:${localShare.toSeq}")

        val allSchemas = keyToSchema.values.toArray
        val receivedTupleHCubeBlocks = tuples
          .groupBy(_._1)
          .map {
            case (key, values) =>
              val schema = keyToSchema(key)
              val content = values.map(_._2).toArray
              val tupleHCubeBlock = TupleHCubeBlock(schema, localShare, content)
              tupleHCubeBlock
          }
          .toArray

        val tupleHCubeBlocks = allSchemas.map { schema =>
          val blocks = receivedTupleHCubeBlocks.filter(_.schema.id == schema.id)
          if (blocks.size == 1) {
            blocks(0)
          } else {
            TupleHCubeBlock(schema, localShare, new Array[Array[DataType]](0))
          }
        }

//        receivedTupleHCubeBlocks.foreach { block =>
//          println(
//            s"relation:${block.schema.id}, contents:${block.content.map(_.toSeq).toSeq}"
//          )
//        }

        val subTask = new SubTask(localShare, tupleHCubeBlocks, info)
        subTask
    }
  }
}