package org.apache.spark.Logo.Physical.Joiner

import java.io.{IOException, ObjectOutputStream}

import org.apache.spark.Logo.Physical.dataStructure.{LogoBlockRef, LogoSchema}
import org.apache.spark.Logo.Physical.utlis.ListGenerator
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

case class SubTask(rddPartitionMap:List[(Int,Int)], rdds:Seq[RDD[_]], newSchema:LogoSchema, oldSchemas:List[LogoSchema], keyMapping:List[List[Int]]) extends Serializable{


  def calculateNewIdex = {
    val keyList = rddPartitionMap.map(f => (f._1,oldSchemas(f._1).IndexToKey(f._2)))

    val newSpecificRow = keyList.
      foldRight(ListGenerator.fillList(0,newSchema.nodeSize))((updateList,targetList) => ListGenerator.fillListIntoTargetList(updateList._2,newSchema.nodeSize,keyMapping(updateList._1),targetList) )

    val index = newSchema.partitioner.getPartition(newSpecificRow)

    index
  }

  def calculatePreferedLocation = {
    val prefs = rddPartitionMap.map(f => rdds(f._1).preferredLocations(rdds(f._1).partitions(f._2)))
    val exactMatchLocations = prefs.reduce((x, y) => x.intersect(y))
    val locs = if (!exactMatchLocations.isEmpty) exactMatchLocations else prefs.flatten.distinct
    locs
  }

  def generateSubTaskPartition = {
    val idx = calculateNewIdex
    val preferredLocations = calculatePreferedLocation
    val subtaskPartition = new SubTaskPartition(idx, rddPartitionMap, rdds, preferredLocations)
    subtaskPartition
  }

//  def compute(rdds:RDD[_], context: TaskContext, handler:(RDD[_], List[SubTask], TaskContext) => LogoBlockRef): Unit ={
//    handler(rdds,List(this), context)
//  }

}

//case class SubTaskBatch(subtasks:List[SubTask]) extends Serializable{
//
//  //  def compute(rdds:RDD[_], context: TaskContext, handler:(RDD[_], List[SubTask], TaskContext) => LogoBlockRef): Unit ={
//  //    handler(rdds,subtasks, context)
//  //  }
//
//}

class SubTaskPartition(
                        idx: Int,
                        subPartitions: List[(Int,Int)],
                        @transient private val rdds: Seq[RDD[_]],
                        @transient val preferredLocations: Seq[String])
  extends Partition {
  override val index: Int = idx

  var partitionValues = subPartitions.map(f => (f._1,rdds(f._1).partitions(f._2)))
  def partitions: Seq[(Int,Partition)] = partitionValues

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    var partitionValues = subPartitions.map(f => (f._1,rdds(f._1).partitions(f._2)))
    oos.defaultWriteObject()
  }
}

