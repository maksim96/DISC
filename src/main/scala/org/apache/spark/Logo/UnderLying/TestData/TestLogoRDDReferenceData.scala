package org.apache.spark.Logo.UnderLying.TestData

import org.apache.spark.Logo.Plan.LogoEdgePatternPhysicalPlan

object TestLogoRDDReferenceData {
  lazy val edgeLogoRDDReference = new LogoEdgePatternPhysicalPlan(TestLogoRDDData.debugEdgePatternLogoRDD) toLogoRDDReference()
}
