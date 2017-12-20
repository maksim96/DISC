package org.apache.spark.Logo.Logical

import org.apache.spark.Logo.Physical.Builder.{LogoBuildPhyiscalStep, LogoBuildScriptStep, SnapPoint}
import org.apache.spark.Logo.Physical.dataStructure._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class LogoLogicalBuildScript {

}

//TODO implement this class
abstract class LogoPatternBuildLogicalStep(logoRDDRefs:List[LogoPatternBuildLogicalStep], snapPoints:List[SnapPoint], name:String="") extends LogoBuildScriptStep{

  lazy val compositeSchema = new IntersectionCompositeLogoSchemaGenerator(logoRDDRefs.map(_.getSchema()), snapPoints) generate()

  @transient lazy val sc = SparkContext.getOrCreate()
  var coreId = 0

  lazy val corePhysical = generateCorePhyiscal()
  lazy val leafPhysical = generateLeafPhyiscal()

  //method used by planner to set which LogoRDDReference is the core.
  def setCoreID(coreId:Int): Unit ={
    this.coreId = coreId
  }

  //preprocessing the leaf RDD, if the leaf is not in J-state, then it will be in J-state
  def generateLeafPhyiscal():PatternLogoRDD

  //preprocessing the core RDD
  def generateCorePhyiscal():PatternLogoRDD

  //generate the new Pattern and add it to catalog, after generate the pattern is in F state
  def generateNewPattern():PatternLogoRDD
  def getSchema():LogoSchema

}

// the generator for generating the handler for converting blocks into a planned2CompositeBlock.
class Planned2HandlerGenerator(coreId:Int){
  def generate():(Seq[LogoBlockRef],CompositeLogoSchema) => LogoBlockRef = {
    (blocks,schema) =>

      val planned2CompositeSchema = schema.toPlan2CompositeSchema(coreId)
      val subBlocks = blocks.asInstanceOf[Seq[PatternLogoBlock[_]]]

      //this place needs to implement later, although currently it has no use.
      val metaData = LogoMetaData(Seq(2,1,2),10)

      val planned2CompositeLogoBlock = new CompositeTwoPatternLogoBlock(planned2CompositeSchema,metaData, subBlocks)

      planned2CompositeLogoBlock
  }
}

class LogoComposite2PatternBuildLogicalStep(logoRDDRefs:List[LogoPatternBuildLogicalStep], snapPoints:List[SnapPoint]) extends LogoPatternBuildLogicalStep(logoRDDRefs,snapPoints) {

  lazy val coreLogoRef = logoRDDRefs(coreId)
  lazy val leafLogoRef = coreId match {
    case 0 => logoRDDRefs(1)
    case 1 => logoRDDRefs(0)
  }

  lazy val logoRDDs = coreId match {
    case 0 => List(corePhysical,leafPhysical)
    case 1 => List(leafPhysical, corePhysical)
  }


  lazy val handler = {
    new Planned2HandlerGenerator(coreId) generate()
  }

  lazy val logoStep = LogoBuildPhyiscalStep(logoRDDs, compositeSchema, snapPoints,handler)

  override def generateLeafPhyiscal(): PatternLogoRDD = {
    leafLogoRef.generateNewPattern()
  }

  override def generateCorePhyiscal(): PatternLogoRDD = {
    coreLogoRef.generateNewPattern()
  }

  override def generateNewPattern(): PatternLogoRDD = {
    new PatternLogoRDD(logoStep.performFetchJoin(sc), getSchema())
  }

  override def getSchema(): LogoSchema = compositeSchema.toPlan2CompositeSchema(coreId)
}


class LogoEdgePatternBuildLogicalStep(edgeLogoRDD:PatternLogoRDD) extends LogoPatternBuildLogicalStep(List(),List()){

  override def generateNewPattern(): PatternLogoRDD = {
    edgeLogoRDD
  }

  override def getSchema(): LogoSchema = edgeLogoRDD.patternSchema

  override def generateLeafPhyiscal():PatternLogoRDD = {
    edgeLogoRDD
  }

  override def generateCorePhyiscal():PatternLogoRDD = {
    edgeLogoRDD
  }
}


