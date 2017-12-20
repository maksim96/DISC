package org.apache.spark.Logo.Logical

import org.apache.spark.Logo.Physical.Builder.LogoBuildScriptStep
import org.apache.spark.Logo.Physical.dataStructure.{KeyMapping, LogoSchema, PatternLogoRDD}


//TODO logical
case class LogoRDDReference(schema: LogoSchema, buildScriptStep: LogoBuildScriptStep){
  def generate() = ???
}

case class PatternLogoRDDReference(patternSchema: LogoSchema, buildScript:LogoPatternBuildLogicalStep) extends LogoRDDReference(patternSchema,buildScript){

  //prepare the Pattern Logo RDD for build operation.
  def toSubPattern(keyMapping: KeyMapping):SubPatternLogoRDDReference = ???

  //actually generate the patternRDD
  override def generate() = ???
}

case class SubPatternLogoRDDReference(patternLogoRDDReference:PatternLogoRDD, keyMapping:KeyMapping){
  def generate() = ???
}
