package hzhang.test.exp.utils

import gnu.trove.map.hash.{TIntIntHashMap, TLongIntHashMap}
import org.apache.spark.adj.deprecated.execution.rdd.{CompositeTwoPatternLogoBlock, TwoKeyPatternInstance}
import org.apache.spark.adj.deprecated.execution.rdd.loader.{DataLoader, Edge3PatternLoader, EdgeLoader}
import org.apache.spark.adj.deprecated.plan.deprecated.PhysicalPlan.FilteringCondition

import scala.collection.mutable

class ADJTestPattern(data: String, h1:Int=3, h2:Int=3)  {

  //  var h1 = 13
  //  var h2 = 13
  val filterCoefficient = 1

  lazy val rawEdge = {
    //        new CompactEdgeLoader(data) rawEdgeRDD
    new DataLoader(data) rawEdgeRDD
  }

  lazy val edge = {

    getEdge(h1,h2)
  }

  def getEdge(hNumber: (Int, Int)) = {
    //        new CompactEdgePatternLoader(rawEdge,Seq(hNumber._1,hNumber._2)) edgeLogoRDDReference
    rawEdge.count()
    new EdgeLoader(rawEdge, Seq(hNumber._1, hNumber._2)) edgeLogoRDDReference
  }


  def get(name:String)  ={
    name match {
      case "lazyNear5Clique" => lazyNear5Clique

      case "eagerHouse" => eagerHouse
      case "eagerThreeTriangle" => eagerThreeTriangle
      case "eagerNear5Clique" => eagerNear5Clique


      case "houseMul" => houseMul
      case "near5CliqueMul" => near5CliqueMul

      case "triangleCom" => triangleCom
      case "fourCliqueCom" => fourCliqueCom

      case "test" => threeAttributesRelation
      case "debug" => threeTriangleGSync
      case "houseGJ" => houseIntersectionGJ
      case "houseGHD" => houseIntersectionGHD
      case "houseF" => houseIntersectionF
      case "threeTriangleNoAD" => threeTriangleNoAdaptive
      case "threeTriangleNoPNoA" => threeTriangleNoPNoA
      case "threeTriangleF" => threeTriangleF
      case "trianglePlusOneEdge" => trianglePlusOneEdge
      case "trianglePlusTwoEdgeF" => trianglePlusTwoEdgeF
      case "trianglePlusWedge" => trianglePlusWedge
      case "squarePlusOneEdgeF" => squarePlusOneEdge
      case "chordalRoof" => chordalRoof
      case _ => null
    }
  }

  def aggregatePattern(name:String) = {
    name match {
      case "triangleAgg" => triangleAgg
      case "squareAgg" => squareAgg
      case "chordalSquareAgg" => chordalSquareAgg
      case "fourCliqueAgg" => fourCliqueAgg
      case "houseAgg" => houseAgg
      case "threeTriangleAgg" => threeTriangleAgg
      case "near5CliqueAgg" => near5CliqueAgg
      case _ => null
    }
  }


  lazy val wedge = {
    val edge = getEdge(h1,h2)
    edge.build(edge.to(0,2))
  }


  lazy val triangle = {
    val edge = getEdge(h1,h2)
    val triangle =  edge.build(edge.to(1,2),edge.to(0,2))
    triangle
  }

  lazy val triangleEdge = {
    val edge = getEdge(h1,h2)

    val triangle =  edge.build(edge.to(1,2),edge.to(0,2))

    val triangleEdge = triangle.build(edge.to(0,3))

    triangleEdge
  }

  lazy val threePath = {
    val edge = getEdge(h1,h2)
    val wedge = edge.build(edge.to(0,2))
    val threePath = wedge.build(edge.to(1, 3))

    threePath
  }

  lazy val square = {
    val edge4_1 = getEdge(h1, h2)
    val edge4_4 = getEdge(h1, h2)

    val wedge = edge4_4
      .build(edge4_4.to(0,2))

    val square = wedge.build(edge4_1.to(1,3), edge4_1.to(2,3))
    square
  }

  lazy val squareEdge = {
    val edge = getEdge(h1, h2)

    val wedge = edge.build(edge.to(0,2))

    val square = wedge.build(edge.to(1,3), edge.to(2,3))

    val squareEdge = square.build(edge.to(0,4))

    squareEdge
  }

  lazy val eagerchordalSquare = {

    val edge3_1 = getEdge(h1, 1)
    val edge3_3 = getEdge(h1, h2)
    val edge1_4 = getEdge(h1, 1)


    val wedge = edge1_4.build(edge1_4.to(0,2)).toConcrete()
    val triangle = edge3_3.filter(p => p(0) < p(1)).build(edge3_1.to(1,2), edge3_1.to(0,2))

    val chordalSquare = wedge.build(triangle.to(1,2,3)).filter(p => p(1) < p(2))

    chordalSquare
  }

  lazy val chordalSquare = {

    val edge3_1 = getEdge(h1, 1)
    val edge3_3 = getEdge(h1, h2)

    val triangle = edge3_3
      .build(edge3_1.to(1,2), edge3_1.to(0,2))

    val chordalSquare = triangle.build(triangle.to(0,1,3))

    chordalSquare
  }

  lazy val fourClique = {
    val filteredEdge = getEdge(h1, h2).filter(p => p(0) < p(1))
    val triangle =  filteredEdge.build(filteredEdge.to(1,2),filteredEdge.to(0,2))

    val filteredEdge4_1 = getEdge(h1,h2).filter(p => p(0) < p(1))

    val fourClique = triangle.build(filteredEdge4_1.to(0,3),filteredEdge4_1.to(1,3),filteredEdge4_1.to(2,3))
    fourClique
  }

  lazy val house = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val leftEdge = edge4_4.filter(p => p(0) < p(1))
    val wedge = leftEdge.build(edge4_4.to(1,2))

    val squareTemp = wedge.build(edge4_1.to(2,3), edge4_1.to(0,3))
      .filter(p => p(0) != p(2) && p(1) != p(3))

    val indexTriangle = leftEdge.build(edge4_1.to(1,2), edge4_1.to(0,2))

    val house = squareTemp.build(indexTriangle.to(0,1,4))
      .filter(p => p(3) != p(4) && p(2) != p(4))

    house
  }

  lazy val eagerHouse = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)
    val edge1_4 = getEdge(1, h1)

    val leftEdge = edge4_4.filter(p => p(0) < p(1))
    val wedge = leftEdge.build(edge4_4.to(1,2)).toConcrete()

    val wedge1 = edge1_4.build(edge1_4.to(0,2))

    val squareTemp = wedge.build(wedge1.to(3,1,2))
      .filter(p => p(0) != p(2) && p(1) != p(3))

    val indexTriangle = leftEdge.build(edge4_1.to(1,2), edge4_1.to(0,2))

    val house = squareTemp.build(indexTriangle.to(0,1,4))
      .filter(p => p(3) != p(4) && p(2) != p(4))

    house
  }


  lazy val houseMul = {
    val edge4_1 = getEdge(11, 1)
    val edge4_4 = getEdge(11, 11)
    val edge4_m = getEdge(11, h1)

    val leftEdge = edge4_4.filter(p => p(0) < p(1))
    val wedge = leftEdge.build(edge4_4.to(1,2))

    val squareTemp = wedge.build(edge4_1.to(2,3), edge4_1.to(0,3))
      .filter(p => p(0) != p(2) && p(1) != p(3))

    val indexTriangle = leftEdge.build(edge4_m.to(1,2), edge4_m.to(0,2))

    val house = squareTemp.build(indexTriangle.to(0,1,4))
      .filter(p => p(3) != p(4) && p(2) != p(4))

    house
  }

  lazy val threeTriangleOld = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val triangle = edge4_4.build(edge4_4.to(0,2), edge4_4.to(1,2)).filter(p => p(1) < p(2))

    val indexTriangle = edge4_4.build(edge4_1.to(0,2), edge4_1.to(1,2))

    val chordalSquareTemp = triangle.build(indexTriangle.to(0,1,3))

    val threeTriangle = chordalSquareTemp.build(indexTriangle.to(0,2,4))
      .filter(p => p(3) != p(2) && p(4) != p(1) && p(3) != p(4))

    threeTriangle
  }

  lazy val threeTriangleGSync = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val triangle = edge4_4.build(edge4_4.to(0,2), edge4_4.to(1,2)).filter(p => p(1) < p(2))


    val chordalSquareTemp = triangle.build(edge4_1.to(0,3),edge4_1.to(2,3))

    val threeTriangle = chordalSquareTemp.gSyncbuild(edge4_1.to(0,4),edge4_1.to(1,4))
      .filter(p => p(3) != p(1) && p(4) != p(2) && p(3) != p(4))

    threeTriangle
  }



  lazy val threeCenterTriangle = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val triangle = edge4_4.build(edge4_4.to(0,2), edge4_4.to(1,2))

    val indexTriangle = edge4_4.build(edge4_1.to(0,2), edge4_1.to(1,2))

    val chordalSquareTemp = triangle.build(edge4_1.to(0,3),edge4_1.to(1,3))

    val threeTriangle = chordalSquareTemp.build(indexTriangle.to(0,1,4))

    threeTriangle

  }

  lazy val twoHeadTriangle = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val triangle = edge4_4.build(edge4_1.to(0,2), edge4_1.to(1,2))
      .filter(p => p(0) < p(2))
    //

    val trianglePlusOneEdge= triangle.build(edge4_4.to(1,3))

    val indexTriangle = edge4_4.build(edge4_1.to(0,2), edge4_1.filter(p => p(0) < p(1)).to(1,2))

    val twoHeadTriangle = trianglePlusOneEdge.build(indexTriangle.to(1,3,4))

    twoHeadTriangle
  }

  lazy val threeTriangle = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val triangle = edge4_4.build(edge4_4.to(0,2), edge4_4.to(1,2))
    //      .filter(p => p(1) < p(2))

    val indexTriangle = edge4_4.build(edge4_1.to(0,2), edge4_1.to(1,2))

    val chordalSquareTemp = triangle.build(edge4_1.to(0,3),edge4_1.to(1,3))

    val threeTriangle = chordalSquareTemp.build(indexTriangle.to(0,2,4))
    //      .filter(p => p(3) != p(2) && p(4) != p(1) && p(3) != p(4))

    threeTriangle
  }

  lazy val eagerThreeTriangle = {
    val edge4_1 = getEdge(h1, 1)
    val edge1_4 = getEdge(1, h1)
    val edge4_4 = getEdge(h1, h2)

    val triangle = edge4_4.build(edge4_4.to(0,2), edge4_4.to(1,2))
    //      .filter(p => p(1) < p(2))

    val wedge = edge1_4.build(edge1_4.to(0,2))

    val indexTriangle = edge4_4.build(edge4_1.to(0,2), edge4_1.to(1,2))

    val chordalSquareTemp = triangle.build(wedge.to(3, 0, 1))

    val threeTriangle = chordalSquareTemp.build(indexTriangle.to(0,2,4))
    //      .filter(p => p(3) != p(2) && p(4) != p(1) && p(3) != p(4))

    threeTriangle
  }


  lazy val fourCliqueEdge = {
    val filteredEdge = edge
    val triangle =  filteredEdge.build(filteredEdge.to(1,2),filteredEdge.to(0,2)).filter(p => p(0) < p(1))

    val filteredEdge4_1 = getEdge(h1,1)

    val fourClique = triangle.build(filteredEdge4_1.to(0,3),filteredEdge4_1.to(1,3),filteredEdge4_1.filter(p => p(0) < p(1)).to(2,3))

    val indexTriangle = filteredEdge.build(filteredEdge4_1.to(1,2),filteredEdge4_1.to(0,2)).filter(p => p(0) < p(1))

    val fourCliqueEdge = fourClique.build(filteredEdge4_1.to(0,4)).filter(p => p(4) != p(2) && p(4) != p(3))


    fourCliqueEdge
  }


  lazy val near5Clique = {
    val filteredEdge = edge
    val triangle =  filteredEdge.build(filteredEdge.to(1,2),filteredEdge.to(0,2))
    //      .filter(p => p(0) < p(1))

    val filteredEdge4_1 = getEdge(h1,1)

    val fourClique = triangle.build(filteredEdge4_1.to(0,3),filteredEdge4_1.to(1,3),filteredEdge4_1.to(2,3))

    val indexTriangle = filteredEdge.build(filteredEdge4_1.to(1,2),filteredEdge4_1.to(0,2))
    //      .filter(p => p(0) < p(1))

    val near5Clique = fourClique.build(indexTriangle.to(0,1,4))
    //      .filter(p => p(4) != p(2) && p(4) != p(3))


    near5Clique
  }

  lazy val lazyNear5Clique = {
    val filteredEdge = edge
    val triangle =  filteredEdge.build(filteredEdge.to(1,2),filteredEdge.to(0,2))
    //      .filter(p => p(0) < p(1))

    val filteredEdge4_1 = getEdge(h1,1)

    val fourClique = triangle.build(filteredEdge4_1.to(0,3),filteredEdge4_1.to(1,3),filteredEdge4_1.to(2,3))

    val indexTriangle = filteredEdge.build(filteredEdge4_1.to(1,2),filteredEdge4_1.to(0,2))
    //      .filter(p => p(0) < p(1))

    val near5Clique = fourClique.build(filteredEdge4_1.to(1,4),filteredEdge4_1.to(0,4))
    //      .filter(p => p(4) != p(2) && p(4) != p(3))


    near5Clique
  }


  lazy val eagerNear5Clique = {
    val filteredEdge = edge
    val triangle =  filteredEdge.build(filteredEdge.to(1,2),filteredEdge.to(0,2))
    //      .filter(p => p(0) < p(1))

    val filteredEdge4_1 = getEdge(h1,1)

    //

    val fourClique = triangle.build(filteredEdge4_1.to(0,3),filteredEdge4_1.to(1,3),filteredEdge4_1.to(2,3)).toConcrete()

    val indexTriangle = filteredEdge.build(filteredEdge4_1.to(1,2),filteredEdge4_1.to(0,2))
    //      .filter(p => p(0) < p(1))

    val near5Clique = fourClique.build(indexTriangle.to(0,1,4))
    //      .filter(p => p(4) != p(2) && p(4) != p(3))


    near5Clique
  }


  lazy val near5CliqueMul = {
    val filteredEdge = getEdge(11,11)
    val triangle =  filteredEdge.build(filteredEdge.to(1,2),filteredEdge.to(0,2)).filter(p => p(0) < p(1))

    val filteredEdge4_1 = getEdge(11,1)
    val filteredEdge4_m = getEdge(11,h1)



    val fourClique = triangle.build(filteredEdge4_1.to(0,3),filteredEdge4_1.to(1,3),filteredEdge4_1.filter(p => p(0) < p(1)).to(2,3))

    val indexTriangle = filteredEdge.build(filteredEdge4_m.to(1,2),filteredEdge4_m.to(0,2)).filter(p => p(0) < p(1))

    val near5Clique = fourClique.build(indexTriangle.to(0,1,4)).filter(p => p(4) != p(2) && p(4) != p(3))


    near5Clique
  }

  lazy val triangleIntersectionVersion = {
    val filterCondition = FilteringCondition({
      pattern =>
        pattern.getValue(0) < pattern.getValue(1)
    }, false)

    val filteredEdge = edge
      .filter(filterCondition)

    val leftEdge = filteredEdge.toIdentitySubPattern()
    val rightEdge = filteredEdge.toSubPattern((0, 1), (1, 2))
    val middleEdge = filteredEdge.toSubPattern((0, 0), (1, 2))

    val triangle = leftEdge.build(rightEdge, middleEdge)
    triangle
  }






  lazy val threeLine = {
    val edge = this.edge

    val filterCondition = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(1)
    }, true)

    val leftEdge = edge.toIdentitySubPattern()
    val middleEdge = edge.filter(filterCondition).toSubPattern((0, 1), (1, 2))

    val wedge = leftEdge.build(middleEdge).toIdentitySubPattern()
    val rightEdge = edge.toSubPattern((0, 2), (1, 3))

    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.pattern(0) != pattern.pattern(3)
    }, false)

    val threeLine = wedge.build(rightEdge).filter(filterCondition1)

    threeLine
  }

  lazy val trianglePlusOneEdge = {
    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(1)
    }, true)

    val lastEdge = getEdge(h1, 1)

    val edge = getEdge(h1, h2)
    val leftEdge = edge.filter(filterCondition1).toIdentitySubPattern()
    val rightEdge = edge.toSubPattern((0, 1), (1, 2))

    val wedge = leftEdge.build(rightEdge).toIdentitySubPattern()

    val middleEdge = edge.filter(filterCondition1).toSubPattern((0, 0), (1, 2))

    val triangle = wedge.build(middleEdge)

    val filterCondition2 = FilteringCondition({
      pattern =>
        val array = pattern.pattern
        array(3) != array(1) && array(3) != array(2)
    }, false)

    val triangleWithOneEdge = triangle.toIdentitySubPattern().build(lastEdge.toSubPattern((0, 0), (1, 3))).filter(filterCondition2)

    triangleWithOneEdge
  }

  lazy val trianglePlusTwoEdgeF = {

    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(1)
    }, true)

    val edge4_1 = getEdge(h1, 1)

    val edge = getEdge(h1, h2)
    val leftEdge = edge.filter(filterCondition1).toIdentitySubPattern()
    val rightEdge = edge.toSubPattern((0, 1), (1, 2))

    val wedge = leftEdge.build(rightEdge).toIdentitySubPattern()

    val middleEdge = edge.filter(filterCondition1).toSubPattern((0, 0), (1, 2))

    val filterCondition2 = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        (p(0)%2+ p(1)%2 + p(2)%2) < 2
    },false)

    val triangle = wedge.build(middleEdge).filter(filterCondition2)

    val triangleWithOneEdge = triangle.toIdentitySubPattern().build(edge4_1.toSubPattern((0, 0), (1, 3)))

    val filterCondition3 = FilteringCondition({
      pattern =>
        val array = pattern.pattern
        array(3) != array(1) && array(3) != array(2) && array(4) != array(0) && array(4) != array(2)
    }, false)

    val trianglePlusTwoEdge = triangleWithOneEdge.toIdentitySubPattern().build(edge4_1.toSubPattern((0, 1), (1, 4)))
      .filter(filterCondition3)

    trianglePlusTwoEdge
  }

  lazy val trianglePlusWedge = {
    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(1)
    }, true)

    val edge = this.edge
    val leftEdge = edge.filter(filterCondition1).toIdentitySubPattern()
    val rightEdge = edge.toSubPattern((0, 1), (1, 2))

    val wedge = leftEdge.build(rightEdge).toIdentitySubPattern()

    val middleEdge = edge.filter(filterCondition1).toSubPattern((0, 0), (1, 2))

    val triangle = wedge.build(middleEdge)

    val triangleWithOneEdge = triangle.toIdentitySubPattern().build(edge.toSubPattern((0, 0), (1, 3)))

    val filterCondition2 = FilteringCondition({
      pattern =>
        pattern.pattern(3) != pattern.pattern(0) && pattern.pattern(3) != pattern.pattern(1) && pattern.pattern(4) != pattern.pattern(0) && pattern.pattern(4) != pattern.pattern(1) && pattern.pattern(4) != pattern.pattern(2)
    }, false)

    val triangleWithWedge = triangleWithOneEdge.toIdentitySubPattern().build(edge.toSubPattern((0, 3), (1, 4))).filter(filterCondition2)

    triangleWithWedge
  }

  lazy val squareFast = {


    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)
    val filterCondition = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(1)
    }, true)

    val leftEdge = edge4_4
      .filter(filterCondition)
      .toIdentitySubPattern()

    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.pattern(1) < pattern.pattern(2)
    }, false)

    val wedge = leftEdge.build(edge4_4
      .filter(filterCondition)
      .toSubPattern((0, 0), (1, 2)))
      .filter(filterCondition1)
      .toIdentitySubPattern()

    val filterCondition2 = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(3)
    }, false)
    val threeLineTemp = wedge.build(edge4_1.toSubPattern((0, 1), (1, 3)))
      .filter(filterCondition2)
      .toIdentitySubPattern()
    val square = threeLineTemp.build(edge4_1.toSubPattern((0, 2), (1, 3)))
    square
  }


  lazy val squareBush = {

    val edge1_4 = getEdge(h1, h2).filter(p => p(0) < p(1),true)
    val edge4_4 = getEdge(h1, h2)

    val indexWedge = edge1_4.build(edge1_4.to(0,2)).filter(p =>  p(1) < p(2))
    val wedge = edge4_4.build(edge4_4.to(0,2)).filter(p =>  p(1) < p(2))

    val square = wedge.build(indexWedge.to(3,1,2)).filter(p => p(3) < p(0))
    square
  }

  lazy val squareIntersectionVerificationFast = {


    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)
    val filterCondition = FilteringCondition({
      pattern =>
        pattern.getValue(0) < pattern.getValue(1)
    }, false)

    val leftEdge = edge4_4
      .filter(filterCondition)


    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.getValue(1) < pattern.getValue(2)
    }, false)

    val wedge = leftEdge.build(edge4_4
      .filter(filterCondition)
      .toSubPattern((0, 0), (1, 2)))
      .filter(filterCondition1)



    val filterCondition2 = FilteringCondition({
      pattern =>
        pattern.getValue(0) < pattern.getValue(3)
    }, false)
    val square = wedge.build(edge4_1.toSubPattern((0, 1), (1, 3)), edge4_1.toSubPattern((0, 2), (1, 3)))
      .filter(filterCondition2)
    //    val square = threeLineTemp.build(edge4_1.toSubPattern((0,2),(1,3)))
    square
  }








  lazy val chordalSquareFast = {

    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.getValue(0) < pattern.getValue(1)
    }, false)

    val edge3_1 = getEdge(h1, 1)
    val edge3_3 = getEdge(h1, h2)
    val leftEdge = edge3_3.filter(filterCondition1).toIdentitySubPattern()

    val triangle = leftEdge.build(edge3_1.toSubPattern((0, 1), (1, 2)), edge3_1.toSubPattern((0, 0), (1, 2)))


    val filterCondition2 = FilteringCondition({
      pattern =>
        pattern.getValue(2) < pattern.getValue(3)
    }, false)

    val chordalSquare = triangle
      .toIdentitySubPattern()
      .build(triangle.toSubPattern((0, 0), (1, 1), (2, 3)))
      .filter(filterCondition2)

    chordalSquare
  }





  lazy val squarePlusOneEdge = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_2 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val filterCondition = FilteringCondition({ pattern =>
      val p = pattern.pattern
      p(1) < p(3) && (p(0)%2+ p(1)%2 + p(2)%2 + p(3) %2) < 2
    }, false)


    val leftEdge = edge4_4.toIdentitySubPattern()
    val wedge = leftEdge.build(edge4_4.toSubPattern((0, 1), (1, 2))).toIdentitySubPattern()



    val squareTemp = wedge.build(edge4_2.toSubPattern((0, 2), (1, 3)), edge4_2.toSubPattern((0, 0), (1, 3))).filter(filterCondition).toIdentitySubPattern()

    val filterCondition2 = FilteringCondition({ pattern =>
      val p = pattern.pattern
      p(0) != p(2) && p(4) != p(1) && p(4) != p(2) && p(4) != p(3)
    }, false)

    val squarePlusOneEdge = squareTemp.build(edge4_1.toSubPattern((0, 0), (1, 4))).filter(filterCondition2)

    squarePlusOneEdge
  }


  lazy val houseFast = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val filterCondition = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(1)
    }, false)

    val leftEdge = edge4_4.filter(filterCondition).toIdentitySubPattern()
    val wedge = leftEdge.build(edge4_4.toSubPattern((0, 1), (1, 2))).toIdentitySubPattern()
    val threeLine = wedge.build(edge4_1.toSubPattern((0, 2), (1, 3))).toIdentitySubPattern()
    val squareTemp = threeLine.build(edge4_1.toSubPattern((0, 0), (1, 3))).toIdentitySubPattern()


    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.pattern(0) != pattern.pattern(2) && pattern.pattern(1) != pattern.pattern(3) && pattern.pattern(3) != pattern.pattern(4) && pattern.pattern(2) != pattern.pattern(4)
    }, false)

    val triangleLeftEdge = leftEdge
    val triangleWedge = leftEdge.build(edge4_1.toSubPattern((0, 1), (1, 2))).toIdentitySubPattern()
    val indexTriangle = triangleWedge.build(edge4_1.toSubPattern((0, 0), (1, 2)))

    val house = squareTemp.build(indexTriangle.toSubPattern((0, 0), (1, 1), (2, 4))).filter(filterCondition1)

    house
  }

  lazy val houseIntersectionFast = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val filterCondition = FilteringCondition({ p =>
      p.getValue(0) < p.getValue(1)
    }, false)

    val leftEdge = edge4_4.filter(filterCondition).toIdentitySubPattern()
    val wedge = leftEdge.build(edge4_4.toSubPattern((0, 1), (1, 2))).toIdentitySubPattern()

    val filterCondition2 = FilteringCondition({ pattern =>
      val p = pattern.pattern
      val node0 = p(0)
      val node1 = p(1)
      val node2 = p(2)
      val node3 = p(3)

      (node0 != node2 && node1 != node3 )
    }
      , false)

    val squareTemp = wedge.build(edge4_1.toSubPattern((0, 2), (1, 3)), edge4_1.toSubPattern((0, 0), (1, 3)))
      .filter(filterCondition2)
      .toIdentitySubPattern()

    val filterCondition3 = FilteringCondition(
      {
        pattern =>
          val p = pattern.pattern
          val node2 = p(2)
          val node3 = p(3)
          val node4 = p(4)

          (node3 != node4 && node2 != node4)
      }
      , false)

    val indexTriangle = leftEdge.build(edge4_1.toSubPattern((0, 1), (1, 2)), edge4_1.toSubPattern((0, 0), (1, 2)))

    val house = squareTemp.build(indexTriangle.toSubPattern((0, 0), (1, 1), (2, 4)))
      .filter(filterCondition3)

    house
  }





  lazy val houseIntersectionGJ = {
    val edge4_1 = getEdge(h1, h2)
    val edge4_4 = getEdge(h1, h2)

    val leftEdge = edge4_4.filter(p => p(0) < p(1),true)
    val wedge = leftEdge.build(edge4_4.toSubPattern((0, 1), (1, 2)))

    val squareTemp = wedge.build(edge4_1.toSubPattern((0, 2), (1, 3)), edge4_1.toSubPattern((0, 0), (1, 3)))
      .filter(p => p(0) != p(2) && p(1) != p(3))

    val indexTriangle = leftEdge.build(edge4_1.toSubPattern((0, 1), (1, 2)), edge4_1.toSubPattern((0, 0), (1, 2)))

    val house = squareTemp.build(edge4_1.to(0,4),edge4_1.to(1,4))
      .filter(p => p(3) != p(4) && p(2) != p(4))
    house
  }

  lazy val houseIntersectionGJ_AssignOptimized = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val leftEdge = edge4_4.filter(p => p(0) < p(1),true)
    val wedge = leftEdge.build(edge4_4.toSubPattern((0, 1), (1, 2)))

    val squareTemp = wedge.build(edge4_1.toSubPattern((0, 2), (1, 3)), edge4_1.toSubPattern((0, 0), (1, 3)))
      .filter(p => p(0) != p(2) && p(1) != p(3))

    val indexTriangle = leftEdge.build(edge4_1.toSubPattern((0, 1), (1, 2)), edge4_1.toSubPattern((0, 0), (1, 2)))

    val house = squareTemp.build(edge4_1.to(0,4),edge4_1.to(1,4))
      .filter(p => p(3) != p(4) && p(2) != p(4))
    house
  }

  lazy val houseIntersectionGHD = {
    val edge4_1 = getEdge(h1, h2)
    val edge4_4 = getEdge(h1, h2)

    val leftEdge = edge4_4.filter(p => p(0) < p(1),true)
    val wedge = leftEdge.build(edge4_4.toSubPattern((0, 1), (1, 2)))

    val squareTemp = wedge.build(edge4_1.toSubPattern((0, 2), (1, 3)), edge4_1.toSubPattern((0, 0), (1, 3)))
      .filter(p => p(0) != p(2) && p(1) != p(3))

    val indexTriangle = leftEdge.build(edge4_1.toSubPattern((0, 1), (1, 2)), edge4_1.toSubPattern((0, 0), (1, 2)))

    val house = squareTemp.build(indexTriangle.toSubPattern((0, 0), (1, 1), (2, 4)))
      .filter(p => p(3) != p(4) && p(2) != p(4))

    house
  }


  lazy val houseIntersectionFastNoSymmetryNew = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val leftEdge = edge4_4
    val wedge = leftEdge.build(edge4_4.toSubPattern((0, 1), (1, 2)))

    val squareTemp = wedge.build(edge4_1.toSubPattern((0, 2), (1, 3)), edge4_1.toSubPattern((0, 0), (1, 3)))
      .filter(p => p(0) != p(2) && p(1) != p(3))

    val indexTriangle = leftEdge.build(edge4_1.toSubPattern((0, 1), (1, 2)), edge4_1.toSubPattern((0, 0), (1, 2)))

    val house = squareTemp.build(indexTriangle.toSubPattern((0, 0), (1, 1), (2, 4)))
      .filter(p => p(3) != p(4) && p(2) != p(4))

    house
  }



  lazy val houseHand = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_2 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val filterCondition = FilteringCondition({ p =>
      p.getValue(0) < p.getValue(1)
    }, true)

    val leftEdge = edge4_4.filter(filterCondition).toIdentitySubPattern()
    val wedge = leftEdge.build(edge4_4.toSubPattern((0, 1), (1, 2))).toIdentitySubPattern()

    val filterCondition2 = FilteringCondition({ pattern =>
      val p = pattern.pattern
      val node0 = p(0)
      val node1 = p(1)
      val node2 = p(2)
      val node3 = p(3)

      (node0 != node2 && node1 != node3 )
    }
      , false)

    val squareTemp = wedge.build(edge4_2.toSubPattern((0, 2), (1, 3)), edge4_2.toSubPattern((0, 0), (1, 3)))
      .filter(filterCondition2)
      .toIdentitySubPattern()

    val filterCondition3 = FilteringCondition(
      {
        pattern =>
          val p = pattern.pattern
          val node2 = p(2)
          val node3 = p(3)
          val node4 = p(4)

          (node3 != node4 && node2 != node4)
      }
      , false)

    val indexTriangle = leftEdge.build(edge4_1.toSubPattern((0, 1), (1, 2)), edge4_1.toSubPattern((0, 0), (1, 2)))

    val house = squareTemp.build(indexTriangle.toSubPattern((0, 0), (1, 1), (2, 4)))


    val size = house.generateF().logoRDD.map {
      f =>
        var size = 0L
        val block = f.asInstanceOf[CompositeTwoPatternLogoBlock]

        val leftBlock = block.coreBlock
        val rightBlock = block.leafsBlock

        val leftBlockIt = leftBlock.enumerateIterator()


        var p2 = 0
        var p3 = 0
        val keyPattern = new TwoKeyPatternInstance(0,0)
        while(leftBlockIt.hasNext){
          val p = leftBlockIt.next()
          p2 = p.getValue(2)
          p3 = p.getValue(3)
          keyPattern.setNode(p.getValue(0),p.getValue(1))
          val leafs = rightBlock.getValue(keyPattern)


          if (leafs != null){
            val row = leafs.getRaw()
            val length = row.length

            //            size += length
            //
            //
            //            if (util.Arrays.binarySearch(row,p2) >= 0){
            //              size -= 1
            //            }
            //
            //            if (util.Arrays.binarySearch(row,p3) >= 0){
            //              size -= 1
            //            }

            var i = 0
            while (i < length){
              val temp = row(i)
              i += 1
              if(temp != p2 && temp != p3) {
                size += 1
              }
            }
          }
        }
        size
    }.sum().toLong

    println("size is" + size)

    house
  }

  //  lazy val houseIntersectionFast2 = {
  //    val edge4_1 = getEdge(h1, 1)
  //    val edge1_4 = getEdge(1,h1)
  //    val edge4_2 = getEdge(h1, 1)
  //    val edge4_4 = getEdge(h1, h2)
  //
  //    val filterCondition = FilteringCondition({ p =>
  //      p.pattern(0) < p.pattern(1)
  //    }, false)
  //
  //
  //    val leftEdge = edge4_1.toIdentitySubPattern()
  //    val wedge = leftEdge.build(edge1_4.toSubPattern((0, 1), (1, 2))).toIdentitySubPattern()
  //
  //    val squareTemp = wedge.build(edge4_4.toSubPattern((0, 2), (1, 3)), edge4_4.filter(filterCondition).toSubPattern((0, 0), (1, 3))).toIdentitySubPattern()
  //
  //
  //    val filterCondition2 = FilteringCondition({ pattern =>
  //      val p = pattern.pattern
  //      p(0) != p(2) && p(1) != p(3) && p(1) != p(4) && p(2) != p(4)
  //    }
  //      , false)
  //
  //
  //    val indexTriangle = leftEdge.build(edge4_1.toSubPattern((0, 1), (1, 2)), edge4_1.toSubPattern((0, 0), (1, 2)))
  //
  //    val house = squareTemp.build(indexTriangle.toSubPattern((0, 0), (1, 3), (2, 4)))
  //      .filter(filterCondition2)
  //
  //    house
  //  }

  lazy val houseIntersectionF = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_2 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val filterC = this.filterCoefficient

    val filterCondition = FilteringCondition({ p =>
      p.pattern(0) < p.pattern(1)
    }, true)

    val leftEdge = edge4_4.filter(filterCondition).toIdentitySubPattern()
    val wedge = leftEdge.build(edge4_4.toSubPattern((0, 1), (1, 2))).toIdentitySubPattern()

    val filterCondition1 = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        (((p(0)*31+p(1))*31+p(2))*31+p(3)) % 10 < filterC
    },false)

    val squareTemp = wedge.build(edge4_2.toSubPattern((0, 2), (1, 3)), edge4_2.toSubPattern((0, 0), (1, 3))).filter(filterCondition1)
      .toIdentitySubPattern()

    val filterCondition2 = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        ((p(0)*31+p(1))*31+p(2)) % 10 < filterC
    },false)

    val filterCondition3 = FilteringCondition({ pattern =>
      val p = pattern.pattern
      p(3) != p(4) && p(2) != p(4) && p(0) != p(2) && p(1) != p(3)
    }, false)

    val indexTriangle = leftEdge.build(edge4_1.toSubPattern((0, 1), (1, 2)), edge4_1.toSubPattern((0, 0), (1, 2))).filter(filterCondition2)

    val house = squareTemp.build(indexTriangle.toSubPattern((0, 0), (1, 1), (2, 4)))
      .filter(filterCondition3)

    house
  }



  lazy val threeTriangleF = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val leftEdge = edge4_4.toIdentitySubPattern()
    val filterC = this.filterCoefficient



    val filterCondition = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        ((p(0)*31+p(1))*31+p(2)) % 10 < filterC && p(1) < p(2)
    }, false)

    //    ((p(0)*31+p(1))*31+p(2) % 10) < 2
    //    (p(0)%2+ p(1)%2 + p(2)%2) < 2

    val triangle = leftEdge.build(edge4_4.toSubPattern((0, 0), (1, 2)), edge4_4.toSubPattern((0, 1), (1, 2))).filter(filterCondition)

    val triangleFilterCondition = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        ((p(0)*31+p(1))*31+p(2)) % 10 < filterC
      //              (p(0)%2+ p(1)%2 + p(2)%2) < 2
    },false)

    val indexTriangle = leftEdge.build(edge4_1.toSubPattern((0, 0), (1, 2)), edge4_1.toSubPattern((0, 1), (1, 2))).filter(triangleFilterCondition)

    val chordalSquareTemp = triangle.toIdentitySubPattern().build(indexTriangle.toSubPattern((0, 0), (1, 1), (2, 3)))

    val filterCondition1 = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        p(3) != p(2) && p(4) != p(1) && p(3) != p(4)
    }, false)

    val threeTriangle = chordalSquareTemp.toIdentitySubPattern().build(indexTriangle.toSubPattern((0, 0), (1, 2), (2, 4)))
      .filter(filterCondition1)

    threeTriangle
  }


  lazy val threeTriangleConCur = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)
    val edge4_1A = getEdge(h1, 1)
    val edge4_4A = getEdge(h1, h2)



    val leftEdge = edge4_4.toIdentitySubPattern()

    val filterCondition = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        p(1) < p(2)
    }, false)

    val triangle = leftEdge.build(edge4_4.toSubPattern((0, 0), (1, 2)), edge4_4.toSubPattern((0, 1), (1, 2))).filter(filterCondition)



    //second round materialziation job


    val indexTriangle = leftEdge.build(edge4_1.toSubPattern((0, 0), (1, 2)), edge4_1.toSubPattern((0, 1), (1, 2)))
    val indexTriangle2 = edge4_4A.build(edge4_1A.toSubPattern((0, 0), (1, 2)), edge4_1A.toSubPattern((0, 1), (1, 2)))

    val secondRoundJob = Seq(indexTriangle,indexTriangle2).toParArray
    val ps = secondRoundJob.map(f => f.toKeyValue(Set(0,1)))


    //third round job

    val chordalSquareTemp = triangle.toIdentitySubPattern().build(ps(0).toSubPattern((0, 0), (1, 1), (2, 3)))

    val filterCondition1 = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        p(3) != p(2) && p(4) != p(1) && p(3) != p(4)
    }, false)

    val threeTriangle = chordalSquareTemp.toIdentitySubPattern().build(ps(1).toSubPattern((0, 0), (1, 2), (2, 4)))
      .filter(filterCondition1)

    threeTriangle
  }




  lazy val threeTriangleFast = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)



    val leftEdge = edge4_4.toIdentitySubPattern()

    val filterCondition = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        p(1) < p(2)
    }, false)

    val triangle = leftEdge.build(edge4_4.toSubPattern((0, 0), (1, 2)), edge4_4.toSubPattern((0, 1), (1, 2))).filter(filterCondition)

    val indexTriangle = leftEdge.build(edge4_1.toSubPattern((0, 0), (1, 2)), edge4_1.toSubPattern((0, 1), (1, 2)))

    val chordalSquareTemp = triangle.toIdentitySubPattern().build(indexTriangle.toSubPattern((0, 0), (1, 1), (2, 3)))

    val filterCondition1 = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        p(3) != p(2) && p(4) != p(1) && p(3) != p(4)
    }, false)

    val threeTriangle = chordalSquareTemp.toIdentitySubPattern().build(indexTriangle.toSubPattern((0, 0), (1, 2), (2, 4)))
      .filter(filterCondition1)

    threeTriangle
  }

  lazy val threeTriangleNoPNoA = {
    val edge4_1 = getEdge(h1, h2)
    val edge4_4 = getEdge(h1, h2)

    val leftEdge = edge4_4.toIdentitySubPattern()

    val filterCondition = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        p(1) < p(2)
    }, false)

    val triangle = leftEdge.build(edge4_4.toSubPattern((0, 0), (1, 2)), edge4_4.toSubPattern((0, 1), (1, 2))).filter(filterCondition)

    //    val indexTriangle = leftEdge.build(edge4_1.toSubPattern((0, 0), (1, 2)), edge4_1.toSubPattern((0, 1), (1, 2)))

    val chordalSquareTemp = triangle.toIdentitySubPattern().build(edge4_1.toSubPattern((0, 0), (1, 3)), edge4_1.toSubPattern((0, 1), (1, 3)))

    val filterCondition1 = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        p(3) != p(2) && p(4) != p(1) && p(3) != p(4)
    }, false)

    val threeTriangle = chordalSquareTemp.toIdentitySubPattern().build(edge4_1.toSubPattern((0, 0), (1, 4)), edge4_1.toSubPattern((0, 2), (1, 4)))
      .filter(filterCondition1)

    threeTriangle
  }

  lazy val threeTriangleNoAdaptive = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val leftEdge = edge4_4.toIdentitySubPattern()

    val filterCondition = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        p(1) < p(2)
    }, false)

    val triangle = leftEdge.build(edge4_4.toSubPattern((0, 0), (1, 2)), edge4_4.toSubPattern((0, 1), (1, 2))).filter(filterCondition)

    //    val indexTriangle = leftEdge.build(edge4_1.toSubPattern((0, 0), (1, 2)), edge4_1.toSubPattern((0, 1), (1, 2)))

    val chordalSquareTemp = triangle.toIdentitySubPattern().build(edge4_1.toSubPattern((0, 0), (1, 3)), edge4_1.toSubPattern((0, 1), (1, 3)))

    val filterCondition1 = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        p(3) != p(2) && p(4) != p(1) && p(3) != p(4)
    }, false)

    val threeTriangle = chordalSquareTemp.toIdentitySubPattern().build(edge4_1.toSubPattern((0, 0), (1, 4)), edge4_1.toSubPattern((0, 2), (1, 4)))
      .filter(filterCondition1)

    threeTriangle
  }




  lazy val near5CliqueOld = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val leftEdge = edge4_4.toIdentitySubPattern()

    val filterCondition = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        p(1) < p(2)
    }, false)

    val triangle = leftEdge.build(edge4_1.toSubPattern((0, 0), (1, 2)), edge4_1.toSubPattern((0, 1), (1, 2))).filter(filterCondition)

    val indexTriangle = leftEdge.build(edge4_1.toSubPattern((0, 0), (1, 2)), edge4_1.toSubPattern((0, 1), (1, 2)))

    val filterCondition1 = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        p(0) < p(3)
    }, false)

    val chordalSquareTemp = triangle.toIdentitySubPattern().build(indexTriangle.toSubPattern((1, 1), (2, 2), (0, 3))).filter(filterCondition1)

    val filterCondition2 = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        p(4) != p(1) && p(4) != p(2)
    }, false)

    val near5Clique = chordalSquareTemp.toIdentitySubPattern().build(indexTriangle.toSubPattern((0, 0), (1, 3), (2, 4)))
      .filter(filterCondition2)

    near5Clique
  }




  lazy val chordalRoof = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val leftEdge = edge4_4.toIdentitySubPattern()

    val filterCondition = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        p(1) < p(2)
    }, false)

    val triangle = leftEdge.build(edge4_1.toSubPattern((0, 0), (1, 2)), edge4_1.toSubPattern((0, 1), (1, 2))).filter(filterCondition)

    val indexTriangle = leftEdge.build(edge4_1.toSubPattern((0, 0), (1, 2)), edge4_1.toSubPattern((0, 1), (1, 2)))

    val filterCondition1 = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        p(0) < p(3)
    }, false)

    val chordalSquareTemp = triangle.toIdentitySubPattern().build(indexTriangle.toSubPattern((1, 1), (2, 2), (0, 3))).filter(filterCondition1)

    val filterCondition2 = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        p(4) != p(1) && p(4) != p(2)
    }, false)

    val chordalRoof = chordalSquareTemp.toIdentitySubPattern().build(edge4_1.toSubPattern((0, 0), (1, 4)), edge4_1.toSubPattern((0, 3), (1, 4)))
      .filter(filterCondition2)

    chordalRoof
  }

  //  lazy val solarSquareGSync = {
  //    val edge4_1 = getEdge(h1, 1)
  //    val edge4_4 = getEdge(h1, h2)
  //
  //
  //    val wedge = edge4_4.build(edge4_4.to(0,2))
  //
  //    val chordalSquare = wedge.build(edge4_1.to(0,3), edge4_1.to(1,3), edge4_1.to(2,3))
  //
  //
  //    val solarSquare = chordalSquare.gSyncbuild(edge4_1.to(0,4),edge4_1.to(1,4),edge4_1.to(2,4))
  //      .filter(p => p(3) != p(1) && p(4) != p(2) && p(3) != p(4))
  //
  //    threeTriangle
  //  }

  lazy val twoSquare = {

    val filterC = this.filterCoefficient

    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)


    val leftEdge = edge4_4




    val wedge = leftEdge.build(edge4_4

      .toSubPattern((0, 0), (1, 2)))

      .toIdentitySubPattern()


    val filterCondition3 = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        (((p(0)*31+p(1))*31+p(2))*31+p(3)) % 1000 < 1
    },false)

    val square = wedge.build(edge4_1.toSubPattern((0, 1), (1, 3)), edge4_1.toSubPattern((0, 2), (1, 3)))
      .filter(filterCondition3)

    val twoSquare = square.toIdentitySubPattern().build(square.toSubPattern((0, 0),(1, 1),(2,4),(3,5)))

    //    val square = threeLineTemp.build(edge4_1.toSubPattern((0,2),(1,3)))
    twoSquare
  }



  lazy val triangleAgg = {
    val filteredEdge = edge.filter(p => p(0) < p(1),true)
    val triangle =  filteredEdge.build(edge.to(1,2),edge.to(0,2))

    triangle.rdd().mapPartitions({
      f =>

        val intMap = new TIntIntHashMap()
        while (f.hasNext){
          val p = f.next()
          val key = p(0)
          intMap.adjustOrPutValue(key,1,1)
        }

        intMap.keys().zip(intMap.values()).iterator
    },true).reduceByKey(_ + _)
  }

  lazy val squareAgg = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val wedge = edge4_4.build(edge4_4.to(0,2)).filter(p => p(1) < p(2))
    val square = wedge.build(edge4_1.to(1,3), edge4_1.to(2,3)).filter(p => p(0) != p(3))

    square.rdd().mapPartitions({
      f =>
        val intMap = new TIntIntHashMap()
        while (f.hasNext){
          val p = f.next()
          val key = p(0)
          intMap.adjustOrPutValue(key,1,1)
        }

        intMap.keys().zip(intMap.values()).iterator

    },true).reduceByKey(_ + _)
  }

  lazy val chordalSquareAgg = {
    val edge3_1 = getEdge(h1, 1)
    val edge3_3 = getEdge(h1, h2)

    val triangle = edge3_3.build(edge3_1.to(1,2), edge3_1.to(0,2))
    val chordalSquare = triangle
      .build(triangle.to(0,1,3))
      .filter(p => p(2) < p(3))

    chordalSquare.rdd().mapPartitions({
      f =>

        val intMap = new TIntIntHashMap()
        while (f.hasNext){
          val p = f.next()
          val key = p(0)
          intMap.adjustOrPutValue(key,1,1)
        }

        intMap.keys().zip(intMap.values()).iterator

    },true).reduceByKey(_ + _)
  }

  lazy val fourCliqueAgg = {


    //    val edge3_1 = getEdge(h1, 1).filter(p => p(0) < p(1), true)
    //    val edge3_3 = getEdge(h1, h2).filter(p => p(0) < p(1), true)
    //
    //    val triangle = edge3_3.build(edge3_1.to(0,2), edge3_1.to(1,2))
    //    val chordalSquare = triangle
    //      .build(triangle.toSubPattern((1, 1), (2, 2), (0, 3)))
    //
    //    val fourClique = chordalSquare.build(edge3_3.to(0,3))
    //    fourClique



    val edge3_1 = getEdge(h1, 1).filter(p => p(0) < p(1), true)
    val edge3_3 = getEdge(h1, h2).filter(p => p(0) < p(1), true)
    val edge3_3u = getEdge(h1, h2)

    val triangle = edge3_3u.build(edge3_1.to(0,2), edge3_1.to(1,2))
    val chordalSquare = triangle
      .build(triangle.toSubPattern((1, 1), (2, 2), (0, 3)))

    val fourClique = chordalSquare.build(edge3_3.to(0,3))

    fourClique.rdd().mapPartitions({
      f =>

        val intMap = new TIntIntHashMap()
        while (f.hasNext){
          val p = f.next()
          val key = p(1)
          intMap.adjustOrPutValue(key,1,1)
        }

        intMap.keys().zip(intMap.values()).iterator

    },true).reduceByKey(_ + _)
  }

  lazy val houseAgg = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val leftEdge = edge4_4.filter(p => p(0) < p(1),true)
    val wedge = leftEdge.build(edge4_4.to(1,2))

    val squareTemp = wedge.build(edge4_1.toSubPattern((0, 2), (1, 3)), edge4_1.toSubPattern((0, 0), (1, 3)))
      .filter(p => p(2) < p(3) && p(0) != p(2) && p(1) != p(3))

    val indexTriangle = leftEdge.build(edge4_1.toSubPattern((0, 1), (1, 2)), edge4_1.toSubPattern((0, 0), (1, 2)))

    val house = squareTemp.build(indexTriangle.toSubPattern((0, 0), (1, 1), (2, 4)))
      .filter(p => p(3) != p(4) && p(2) != p(4))

    house.rdd().mapPartitions({
      f =>

        val longMap = new TLongIntHashMap()
        while (f.hasNext){
          val p = f.next()
          val key = (p(2).toLong << 32) | (p(3) & 0xffffffffL)
          longMap.adjustOrPutValue(key,1,1)

        }

        longMap.keys().zip(longMap.values()).iterator

    },true).reduceByKey(_ + _)
  }

  lazy val threeTriangleAgg = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val triangle = edge4_4.build(edge4_4.to(0,2), edge4_4.to(1,2)).filter(p => p(1) < p(2))

    val indexTriangle = edge4_4.build(edge4_1.to(0,2), edge4_1.to(1,2))

    val chordalSquareTemp = triangle.build(indexTriangle.to(0,1,3))

    val threeTriangle = chordalSquareTemp.build(indexTriangle.to(0,2,4))
      .filter(p => p(3) < p(4) && p(3) != p(2) && p(4) != p(1) )

    threeTriangle.rdd().mapPartitions({
      f =>

        val longMap = new TLongIntHashMap()
        while (f.hasNext){
          val p = f.next()
          val key = (p(1).toLong << 32) | (p(2) & 0xffffffffL)

          if (longMap.containsKey(key)){
            longMap.adjustValue(key,1)
          } else{
            longMap.put(key,1)
          }

        }

        longMap.keys().zip(longMap.values()).iterator

    },true).reduceByKey(_ + _)
  }

  lazy val near5CliqueAgg = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val leftEdge = edge4_4.toIdentitySubPattern()

    val filterCondition = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        p(1) < p(2)
    }, false)

    val triangle = edge4_4.build(edge4_1.to(0,2), edge4_1.to(1,2)).filter(p => p(0) < p(1))

    val indexTriangle = edge4_4.build(edge4_1.to(0,2), edge4_1.to(1,2))

    val filterCondition1 = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        p(0) < p(3)
    }, false)

    val chordalSquareTemp = triangle.toIdentitySubPattern().build(indexTriangle.toSubPattern((1, 1), (2, 2), (0, 3))).filter(filterCondition1)

    val filterCondition2 = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        p(4) != p(1) && p(4) != p(2)
    }, false)

    val near5Clique = chordalSquareTemp.toIdentitySubPattern().build(indexTriangle.toSubPattern((0, 0), (1, 3), (2, 4)))
      .filter(filterCondition2)

    near5Clique.rdd().mapPartitions({
      f =>

        val longMap = new TLongIntHashMap()
        while (f.hasNext){
          val p = f.next()
          val key = (p(0).toLong << 32) | (p(1) & 0xffffffffL)
          longMap.adjustOrPutValue(key,1,1)

        }

        mutable.LongMap


        longMap.keys().zip(longMap.values()).iterator



    },true).reduceByKey(_ + _)
  }


  lazy val triangleCom = {
    val filteredEdge = edge
    val triangle =  filteredEdge.build(filteredEdge.to(1,2),filteredEdge.to(0,2))
    triangle
  }

  lazy val fourCliqueCom = {
    val filteredEdge = edge
    val triangle =  filteredEdge.build(filteredEdge.to(1,2),filteredEdge.to(0,2))

    val filteredEdge4_1 = filteredEdge

    val fourClique = triangle.build(filteredEdge4_1.to(0,3),filteredEdge4_1.to(1,3),filteredEdge4_1.to(2,3))
    fourClique
  }

  lazy val threeAttributesRelation = {
    val edge3_1 = getEdge(h1, h1)
    val edge3_3 = getEdge(h1, h2)

    val triangle = edge3_3
      .filter(p => p(0) < p(1))
      .build(edge3_1.to(1,2), edge3_1.to(0,2))


    val threeAttributesRelation = triangle.rdd().map(f => (f.pattern.clone(),1))

    //    println(threeAttributesRelation.map(f => (f._1(0),f._1(1),f._1(2))).distinct().count())

    val sizes = Seq(3,3,1)

    val patternLoader = new Edge3PatternLoader(threeAttributesRelation,sizes)
    val logo = patternLoader.edgeLogoRDDReference


    val out = logo.build(triangle.to(0,1,3))
      .filter(p => p(2) < p(3))

    //    val out = logo.build(edge3_1.to(0,3),edge3_1.to(1,3))
    out
  }

}