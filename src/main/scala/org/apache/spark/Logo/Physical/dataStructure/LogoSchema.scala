package org.apache.spark.Logo.Physical.dataStructure

import org.apache.spark.Logo.Physical.Maker.PartitionerMaker
import org.apache.spark.Logo.Physical.utlis.{ListGenerator, PointToNumConverter, TestUtil}
import org.apache.spark.Partitioner

import scala.collection.mutable.ArrayBuffer

sealed trait LogoColType
case object KeyType extends LogoColType
case object NonKeyType extends LogoColType
case object AttributeType extends LogoColType




/**
  * @note notice key must start from zero. and key cols are consecutive
  * @param edges edges of pattern
  * @param keySizeMap partition size for each key slot
  */
class LogoSchema (val edges:Seq[(Int,Int)], val keySizeMap:Map[Int,Int], val name:String = "") extends Serializable{

  assert(TestUtil.listEqual(edges.flatMap(f => Iterator(f._1,f._2)).toList.distinct.sorted,keySizeMap.keys.toList.sorted), "all nodes in edge are keys and must be specified an partition number")


  @transient lazy val nodeSize = (edges.flatMap(f => Iterable(f._1,f._2)).max)+1
  @transient lazy val keyCol = keySizeMap.keys.toList
  @transient lazy val slotSize = keySizeMap.values.toList
  @transient lazy val partitioner = PartitionerMaker()
    .setSlotMapping(keyCol)
    .setSlotSize(slotSize)
    .build()

  @transient lazy val baseList = slotSize
  @transient lazy val converter = new PointToNumConverter(baseList)

  //assume we sorted the keyCol and get their value as a list

  //we assume here that List(0,1,2,3) represent number 3,2,1,0
  def keyToIndex(key:Seq[Int]) = converter.convertToNum(key)

  def IndexToKey(num:Int) = converter.NumToList(num)

  def allPossiblePlan() = {
    ListGenerator.cartersianSizeList(slotSize);
  }

  override def clone(): AnyRef = {
    new LogoSchema(edges,keySizeMap)
  }

  override def toString: String = {
    val stringBuilder = new StringBuilder
    stringBuilder.append(s"\nname:${name}")
    stringBuilder.append("\nedge connection: \n")
    edges.foldLeft(stringBuilder)((stringBuilder,ele) => stringBuilder.append(ele+"\n"))
    stringBuilder.append("keySizeMap: \n")
    keySizeMap.foldLeft(stringBuilder)((stringBuilder,ele) => stringBuilder.append(ele+"\n"))
    stringBuilder.toString()
  }
}


/**
  *
  * @param schema new schema of this composite schema
  * @param oldSchemas old schema from which this new schema is dereived
  * @param keyMapping key mapping from old schemas to new schemas
  */
class CompositeLogoSchema(schema:LogoSchema,
                          oldSchemas:Seq[LogoSchema],
                          val keyMapping:Seq[Seq[Int]]) extends LogoSchema(schema.edges,schema.keySizeMap){

  @transient lazy val reverseKeyMapping = keyMapping.map(f => f.zipWithIndex.toMap)


  //TODO testing snapMapps,coreBlock,leafBlock,Joint

  /**
    * calculate each node in new pattern has how many old pattern join on it.
    */
  @transient lazy val snapMapps = keyMapping.flatten.groupBy(f => f).map(f => (f._1,f._2.size))


  /**
    * calculate the coreBlockId, core is the block which has the most joint.
    */
  @transient lazy val coreBlockId = {
    val JointSet = Joint.toSet
    val jointCountMap = keyMapping.map{
      f =>
        var counter = 0
        f.foldLeft(counter)((counter,ele) => JointSet.contains(ele) match {
          case true => counter+1
          case false => counter
        })
    }

    val coreBlockId = jointCountMap.zipWithIndex.maxBy(_._1)._2
    coreBlockId
  }

  /**
    * calculate the leafBlockIds, except from core, the other blocks are all leaf.
    */
  @transient lazy val leafBlockIds = Range(0,oldSchemas.size).filter(p => p != coreBlockId)

  /**
    * Joint are nodes in new pattern which have at two sub patterns join on it.
    */
  @transient lazy val Joint = snapMapps.filter(p => p._2 > 1).keys


  def oldKeysToNewKey(oldKey:Seq[Seq[Int]]) =
    IndexToKey(
    oldIndexToNewIndex(
      oldKey.zipWithIndex.map(f => oldSchemas(f._2).keyToIndex(f._1))))

  def oldIndexToNewIndex(oldIndex:Seq[Int]) = {
    val keyList = oldIndex.zipWithIndex.map(_.swap).map(f => (f._1,oldSchemas(f._1).IndexToKey(f._2)))

//    //TODO testing
//    println("keyList")
//    keyList.foreach(println)

    val newSpecificRow = keyList.
      foldRight(ListGenerator.fillList(0,schema.nodeSize))((updateList,targetList) => ListGenerator.fillListIntoTargetList(updateList._2,schema.nodeSize,keyMapping(updateList._1),targetList) )
    val index = schema.partitioner.getPartition(newSpecificRow)
    index
  }

  def newKeyToOldKey(newKey:Seq[Int]):Seq[Seq[Int]] = {
    keyMapping.map(f => f.map(newKey(_)))
  }

  def newKeyToOldIndex(newKey:Seq[Int]):Seq[Int] = {

    newKeyToOldKey(newKey).zipWithIndex.map(f => oldSchemas(f._2).keyToIndex(f._1))
  }

  def newIndexToOldIndex(newIndex:Int):Seq[Int] = {
    newKeyToOldIndex(IndexToKey(newIndex))
  }

  def allPossibleCombinePlan() ={
    val allPlan = allPossiblePlan()
    allPlan.map(f => newKeyToOldIndex(f))
  }

}

/**
  * generator for generate LogoSchema
  */
trait schemaGenerator{
  def generate():LogoSchema
}

/**
  *
  * @param edges edges of pattern, the direction of the edges is not important, so it will all be represented as (small_id,large_id),it only represent a connection
  * @param keySizeMap partition size for each key slot
  */
class PlainLogoSchemaGenerator(edges:Seq[(Int,Int)],keySizeMap:Map[Int,Int],name:String) extends schemaGenerator{
  override def generate() = {
    val sortedEdges = edges.map{
      f =>
        if (f._2 < f._1){
          f.swap
        }else{
          f
        }
    }

    new LogoSchema(sortedEdges,keySizeMap,name)
  }
}

/**
  *
  * @param oldSchemas old schema from which this new schema is dereived
  * @param partialKeyMappings key mapping from old schemas to new schemas
  */
abstract class CompositeLogoSchemaGenerator(val oldSchemas:Seq[LogoSchema], val partialKeyMappings:Seq[Map[Int,Int]], val name:String="") extends schemaGenerator{
  def keyMapGenerate():Seq[Seq[Int]]

  def integrityCheck(): Unit ={

  }

  def edgeGenerate(keyMapping: Seq[Seq[Int]]):Seq[(Int,Int)] = {
    val oldEdges = oldSchemas.map(_.edges)
    oldEdges.zipWithIndex.map{f =>
      val index = f._2
      val oldEdge = f._1
      oldEdge.map(x => (keyMapping(index)(x._1),(keyMapping(index)(x._2))))
    }.flatMap(f => f).map{
      f =>
        if (f._2 < f._1){
          f.swap
        }else{
          f
        }
    }.distinct
  }

  def keySizeMapGenerate(keyMapping: Seq[Seq[Int]]):Map[Int,Int] = {
    val oldKeySizes = oldSchemas.map(_.keySizeMap)
    oldKeySizes.zipWithIndex.map{ f=>
      val index = f._2
      val keySize = f._1
      keySize.map(x => (keyMapping(index)(x._1),x._2))
    }.flatMap(f => f.toList).distinct.sortBy(_._1).toMap
  }

  override def generate(): CompositeLogoSchema = {
    integrityCheck()

    val keyMapping  = keyMapGenerate()
    val edges = edgeGenerate(keyMapping)
    val keySizeMap = keySizeMapGenerate(keyMapping)
    val schema = LogoSchema(edges,keySizeMap,name)
    val compositeSchema = new CompositeLogoSchema(schema,oldSchemas,keyMapping)
    compositeSchema
  }
}



//must ensure that intersectionKey in IntersectionKeyMapping take low number new key first.
/**
  *
  * @param oldSchmeas old schema from which this new schema is dereived
  * @param intersectionKeyMappings key mapping from old schemas to new schemas
  */
class SimpleCompositeLogoSchemaGenerator(oldSchmeas:Seq[LogoSchema], intersectionKeyMappings:Seq[Map[Int,Int]],name:String="") extends CompositeLogoSchemaGenerator(oldSchmeas, intersectionKeyMappings){
  override def keyMapGenerate() = {
    val oldKeys = oldSchemas.map(_.keyCol)
    val keyMapBuffer = new Array[Array[Int]](oldSchemas.length).map(f => new Array[Int](0)).zipWithIndex.map { f =>
      val oldKey = oldKeys(f._2)
      f._1 ++ oldKey
    }


    //calculate how many intersection key here in new schema's key.
    var newIntersectionKeyNum = intersectionKeyMappings.map(f => f.toList.map(_._2)).flatMap(f => f).distinct.size

    //warning please twice check, because this operation will change the content of keyMapBuffer
    keyMapBuffer.zipWithIndex.foldLeft(newIntersectionKeyNum){(curSlot, key) =>
      val partialKeyMapping = intersectionKeyMappings(key._2)
      val keyMapping = key._1
      var _curSlot = curSlot

      for (i <- 0 until keyMapping.length){
        if (partialKeyMapping.contains(keyMapping(i))){
          keyMapping(i) = partialKeyMapping(keyMapping(i))
        } else{
          keyMapping(i) = _curSlot
          _curSlot += 1
        }
      }

      _curSlot}

    keyMapBuffer.toList.map(_.toList)
  }
}

object LogoSchema{
  def apply(edges:Seq[(Int,Int)],keySizeMap:Map[Int,Int],name:String=""): LogoSchema = {
    new PlainLogoSchemaGenerator(edges,keySizeMap,name).generate()
  }
}

object CompositeLogoSchema{
  def apply(oldSchmeas:Seq[LogoSchema], IntersectionKeyMappings:Seq[Map[Int,Int]],name:String="") = {
    new SimpleCompositeLogoSchemaGenerator(oldSchmeas,IntersectionKeyMappings).generate()
  }
}

