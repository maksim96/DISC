package org.apache.spark.Logo.Physical.Maker

import org.apache.spark.Logo.Physical.dataStructure._
import org.apache.spark.Logo.Physical.utlis.{ListGenerator, PointToNumConverter}
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}

import scala.reflect.ClassTag


//can only work with slotParititioner, or compositeParitioner with no more than 2 level.

abstract class RowLogoRDDMaker[A:ClassTag, B: ClassTag](val rdd: RDD[(A,B)]) extends Serializable{

  var _edges:Seq[(Int,Int)] = _
  var _keySizeMap:Map[Int,Int] = _



  lazy val _schema = LogoSchema(_edges,_keySizeMap)
  lazy val _nodeSize = _schema.nodeSize
  lazy val partitioner:CompositeParitioner = _schema.partitioner

  def setEdges(edges:Seq[(Int,Int)]) = {
    this._edges = edges
    this
  }

  def setKeySizeMap(keySizeMap:Map[Int,Int]) = {
    _keySizeMap = keySizeMap
    this
  }

  def getSchema = _schema


  def build():RDD[RowLogoBlock[(A,B)]]
}


/**
  * @param rdd The RDD used to make a logoRDD
  * @tparam A Attribute Type
  * List[Int] Key Type
  */
class SimpleRowLogoRDDMaker[A:ClassTag](rdd:RDD[(Seq[Int],A)]) extends RowLogoRDDMaker(rdd){

  @transient val sc = rdd.sparkContext
  lazy val keyCol = partitioner.partitioners.map(_.slotNum)

  def generateSentry() = {

    var sentryNode:Seq[Seq[Int]] = null
    var sentry:Seq[(Seq[Int],A)] = null
    var sentryRDD:RDD[(Seq[Int],A)] = null


    val slotNums = partitioner.partitioners.map(_.slotNum)
    val baseList = partitioner.partitioners.map(_.p1)
    sentryNode = ListGenerator.fillListListIntoSlots(ListGenerator.cartersianSizeList(baseList),_nodeSize,slotNums)
    sentry = sentryNode.map((_,null.asInstanceOf[A]))
    sentryRDD = sc.parallelize(sentry)


    sentryRDD
  }

  def build(): RDD[RowLogoBlock[(Seq[Int],A)]] ={

    require(_edges != null, "edge must be designated before build")
    require(_keySizeMap != null, "keySizeMap must be designated before build")

    val sentryRDD = generateSentry
    val baseList = partitioner.partitioners.map(_.p1)

    val sentriedRDD = rdd.union(sentryRDD)

    val schema = _schema.clone().asInstanceOf[LogoSchema]

    sentriedRDD.partitionBy(partitioner).mapPartitionsWithIndex[RowLogoBlock[(Seq[Int],A)]]({case (index,f) =>
      val blockGenerator = new rowBlockGenerator(schema,index,f)
      val block = blockGenerator.generate()
      Iterator(block)
    },true)
  }
}





//class TwoTupleOneHoleListLogoBlockMaker(sc:SparkContext,  partitioner: Partitioner)
//  extends LogoBlockMaker[VertexId,VertexId](sc, partitioner.numPartitions ,partitioner){
//
//  override def makeBlocks(rdd: RDD[(VertexId, VertexId)]) = {
//    val sentriedRDD = generateSentry(rdd)
//    sentriedRDD.partitionBy(partitioner).mapPartitionsWithIndex[LogoBlock](
//      { (x,y) =>
//        Iterator(new TwoTupleOneHoleListLogoBlock(x,y.filter(_ != (x,-x)).toList))
//      },true
//    )
//  }
//
//  override def generateSentry(rdd: RDD[(VertexId, VertexId)]) = {
//    val sentries = sc.parallelize(Range(0,colorNums)).map(f => (f.toLong,(-f).toLong))
//    rdd ++ sentries
//  }
//}
//
//class TwoTupleOneHoleListLogoBlockHashMaker(sc:SparkContext, colorNums:Int)
//  extends TwoTupleOneHoleListLogoBlockMaker(sc,new HashPartitioner(colorNums))
//
//
//
//class TwoTupleTwoHoleListLogoBlockMaker(sc:SparkContext,  partitioner: HashPartitioner2)
//  extends LogoBlockMaker[(VertexId,VertexId),Boolean](sc, partitioner.numPartitions ,partitioner){
//
//  override def makeBlocks(rdd: RDD[((VertexId, VertexId),Boolean)]) = {
//    val sentriedRDD = generateSentry(rdd)
//    val row = partitioner.p1
//    val col = partitioner.p2
//    sentriedRDD.partitionBy(partitioner).mapPartitionsWithIndex[LogoBlock](
//      { (x,y) =>
//        Iterator(new TwoTupleTwoHoleListLogoBlock((x/row,x%col),y.filter(_._2 != false).map(_._1).toList))
//      },true
//    )
//  }
//
//  override def generateSentry(rdd: RDD[((VertexId, VertexId),Boolean)]) = {
//
//    implicit class Crossable[X](xs: Traversable[X]) {
//      def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
//    }
//
//    val row = partitioner.p1
//    val col = partitioner.p2
//
//    val sentries = sc.parallelize((Range(0,row) cross Range(0,col)).toList).map(f => ((f._1.toLong,f._2.toLong),false))
//    rdd ++ sentries
//  }
//}
//
//class TwoTupleTwoHoleListLogoBlockHashMaker(sc:SparkContext, color1:Int, color2:Int)
//  extends TwoTupleTwoHoleListLogoBlockMaker(sc,new HashPartitioner2(color1,color2))
//
//
//
//
//
//
//



