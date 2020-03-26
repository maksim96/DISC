package dsce

import org.apache.spark.adj.database.RelationSchema
import org.apache.spark.adj.optimization.costBased.decomposition.relationGraph.RelationDecomposer
import org.apache.spark.adj.utils.exp.ExpQueryHelper
import org.apache.spark.dsce.testing.{
  ExpQuery,
  ExtraExpEntry,
  UniqueQueryComputer
}
import org.scalatest.FunSuite

class GHDTest extends FunSuite {

  test("distGenGHDs") {
    ExtraExpEntry.main(Array("5"))
  }

  test("genGHD") {

    val queries = Seq(
//      "wedge"
//      "debug"
      "quadTriangle"
//      "triangleCore",
//      "twinCSquare",
//      "twinClique4"
//      "starofDavidPlus"
    )

    queries.foreach { query =>
      val data = "debug"
      val expQuery = new ExpQuery(data)
      val schemas = expQuery.getSchema(query)
      val decomposer = new RelationDecomposer(schemas)
      val ghds = decomposer.decomposeTree()
      val optimalGHD = ghds.head

      println(s"num of GHD:${ghds.size}, optimal:${optimalGHD}")
//      ghds.foreach { g =>
//        println(g)
//      }
    }
  }

  val numNode = 7
  val queryComputer = new UniqueQueryComputer(numNode)

  test("genGHDs") {
    val patterns = queryComputer.genValidPattern()
    val dmls = patterns.map { f =>
      val V = f.V
      val E = f.E
      val dictionary = Seq("A", "B", "C", "D", "E", "F", "G", "H", "I")
      val Attrs = Range(0, V.size).map(dictionary)
      val IdToAttrsMap =
        V.zip(Attrs).toMap
      val EdgesOfAttrs =
        E.map(f => (IdToAttrsMap(f._1), IdToAttrsMap(f._2)))
          .map { f =>
            if (f._1 > f._2) {
              f.swap
            } else {
              f
            }
          }
          .sorted
      EdgesOfAttrs.map(f => s"${f._1}-${f._2};").reduce(_ + _)
    }

    val ghds = dmls.map { dml =>
      val schemas = ExpQueryHelper.dmlToSchemas(dml)

      val time1 = System.currentTimeMillis()
      val decomposer = new RelationDecomposer(schemas)
      val optimalGHD =
        decomposer.decomposeTree().head
      val time2 =System.currentTimeMillis()

      print(s"dml:${dml}-time:${time2-time1}ms")
      println(optimalGHD)
      optimalGHD
    }

    println(s"numPattern:${patterns.size}")
    println(s"maximum fhtw for ${numNode}-node pattern:${ghds.maxBy(_.fhtw)}")
  }
}
