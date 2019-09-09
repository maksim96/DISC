package org.apache.spark.adj.optimization.decomposition.relationGraph

import org.apache.spark.adj.database.Catalog.{AttributeID, RelationID}
import org.apache.spark.adj.database.RelationSchema

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

//TODO: few more test needed
class RelationDecomposer(schemas: Seq[RelationSchema]) {
  def decomposeTree(): IndexedSeq[RelationGHDTree] = {

    //filter the schemas that are contained inside another schema
    val containedSchemas = schemas.filter { s1 =>
      schemas.diff(Seq(s1)).exists(s2 => s1.attrIDs.diff(s2.attrIDs).isEmpty)
    }
    val notContainedSchemas = schemas.diff(containedSchemas)

    //find the GHD Decomposition for the notContainedSchemas
    val E = notContainedSchemas.map(f => RelationEdge(f.attrIDs.toSet))
    val V = E.flatMap(_.attrs).distinct
    val graph = RelationGraph(V, E)
    val ghds = HyperTreeDecomposer.allGHDs(graph)

    //construct RelationGHD
    ghds
      .map { t =>
        val edgeToSchema =
          notContainedSchemas.map(f => (f.attrIDs.toSet, f)).toMap
        val bags =
          t.V
            .map(f => (f.id, f.g.E().map(edge => edgeToSchema(edge.attrs))))
            .map {
              case (idx, bag) =>
                //add previously filtered schemas to the bags that contained it.
                val fullBag = bag ++ bag
                  .flatMap(
                    schema1 =>
                      containedSchemas.filter(
                        schema2 => schema2.attrIDs.diff(schema1.attrIDs).isEmpty
                    )
                  )
                  .distinct

                (idx, fullBag)
            }
        val connections = t.E.map(e => (e.u.id, e.v.id))

        RelationGHDTree(bags, connections, t.fractionalHyperNodeWidth())
      }
      .sortBy(relationGHD => (relationGHD.fhtw, -relationGHD.E.size))
  }

  //TODO: test
  def decomposeStar(
    isSingleAttrFactorization: Boolean = true
  ): IndexedSeq[RelationGHDStar] = {
    //filter the schemas that are contained inside another schema
    val containedSchemas = schemas.filter { s1 =>
      schemas.diff(Seq(s1)).exists(s2 => s1.attrIDs.diff(s2.attrIDs).isEmpty)
    }
    val notContainedSchemas = schemas.diff(containedSchemas)

    //find the GHD Decomposition for the notContainedSchemas
    val E = notContainedSchemas.map(f => RelationEdge(f.attrIDs.toSet))
    val V = E.flatMap(_.attrs).distinct
    val graph = RelationGraph(V, E)
    val ghds = HyperTreeDecomposer.allGHDs(graph)

    //filter out the HyperStar and construct RelationGHD
    val stars = ghds
      .filter { t =>
        val adjList = t.E
          .map(edge => (edge.u.id, edge.v.id))
          .flatMap(edge => Iterator(edge, edge.swap))
          .groupBy(_._1)
          .map(g => (g._1, g._2.map(_._2)))
          .toMap

        val numOfNodes = t.V.size

        if (numOfNodes == 1) {
          false
        } else {
          t.V.exists { n =>
            adjList(n.id).size == (numOfNodes - 1)
          }
        }
      }
      .map { t =>
        val edgeToSchema =
          notContainedSchemas.map(f => (f.attrIDs.toSet, f)).toMap
        val bagMaps =
          t.V
            .map(f => (f.id, f.g.E().map(edge => edgeToSchema(edge.attrs))))
            .map {
              case (idx, bag) =>
                //add previously filtered schemas to the bagMaps that contained it.
                val fullBag = bag ++ bag
                  .flatMap(
                    schema1 =>
                      containedSchemas.filter(
                        schema2 => schema2.attrIDs.diff(schema1.attrIDs).isEmpty
                    )
                  )
                  .distinct

                (idx, fullBag)
            }
            .toMap

        val adjList = t.E
          .map(edge => (edge.u.id, edge.v.id))
          .flatMap(edge => Iterator(edge, edge.swap))
          .groupBy(_._1)
          .map(g => (g._1, g._2.map(_._2)))
          .toMap

        val numOfNodes = t.V.size
        val rootId = t.V
          .filter { n =>
            adjList(n.id).size == (numOfNodes - 1)
          }
          .head
          .id
        val root = bagMaps(rootId)
        val leaves = bagMaps.keys.toSeq.diff(Seq(rootId)).map(bagMaps)

        RelationGHDStar(root, leaves, t.fractionHyperStarWidth(rootId))
      }

    if (isSingleAttrFactorization) {
      stars
        .filter(_.isSingleAttrLeafStar())
        .sortBy(
          relationGHDStar =>
            (relationGHDStar.fhsw, -relationGHDStar.leaves.size)
        )
    } else {
      stars.sortBy(
        relationGHDStar => (relationGHDStar.fhsw, -relationGHDStar.leaves.size)
      )
    }

  }

}

case class RelationGHDTree(V: Seq[(Int, Seq[RelationSchema])],
                           E: Seq[(Int, Int)],
                           fhtw: Double) {

  val idToGHDNode = V.toMap

  //enumerate all the possible traversal orders of the ghd
  def allTraversalOrder: Seq[Seq[Int]] = {

    val fullDirE = E.flatMap(f => Seq(f, f.swap))
    val ids = V.map(_._1)

    //all traversalOrders
    var traversalOrders = ids.permutations.toSeq

    //only retain connected traversalOrders
    traversalOrders = traversalOrders.filter { p =>
      var valid = true
      var i = 1
      while (i < p.size) {
        val subPath = p.slice(0, i)
        if (!subPath.exists(j => fullDirE.contains(j, i))) {
          valid = false
        }
      }
      valid
    }

    traversalOrders
  }

  //find the compatible attribute orders for an given traversal order
  def compatibleAttrOrder(traversalOrder: Seq[Int]): Seq[Array[AttributeID]] = {
    val firstAttrs =
      idToGHDNode(traversalOrder.head).flatMap(_.attrIDs).distinct.toSeq
    var attrOrders = firstAttrs.permutations.toSeq
    var remainingTraversalOrder = traversalOrder.drop(1)
    val assignedAttrIds = ArrayBuffer[Int]()
    assignedAttrIds ++= firstAttrs

    while (remainingTraversalOrder.nonEmpty) {
      val nextAttrIds = idToGHDNode(traversalOrder.head)
        .flatMap(_.attrIDs)
        .diff(assignedAttrIds)
      val nextAttrOrders = nextAttrIds.permutations.toSeq
      attrOrders = attrOrders.flatMap { attrOrder =>
        nextAttrOrders.map(nextOrder => attrOrder ++ nextOrder)
      }
    }

    attrOrders.map(_.toArray)
  }

}

case class RelationGHDStar(core: Seq[RelationSchema],
                           leaves: Seq[Seq[RelationSchema]],
                           fhsw: Double) {

  val coreAttrIds = core.flatMap(_.attrIDs).distinct

  def isSingleAttrLeafStar(): Boolean = {
    leaves.forall { schemas =>
      val leafAttrIds = schemas.flatMap(_.attrIDs).distinct
      leafAttrIds.diff(coreAttrIds).size == 1
    }
  }

  def factorizeSingleAttrOrder(): (Seq[AttributeID], Int) = {
    if (isSingleAttrLeafStar()) {
      val leaveIds = leaves.map { schemas =>
        val leafAttrIds = schemas.flatMap(_.attrIDs).distinct
        leafAttrIds.diff(coreAttrIds)(0)
      }

      (coreAttrIds ++ leaveIds, coreAttrIds.size - 1)
    } else {
      throw new Exception("Star does not exists factorizeSingleAttrOrder")
    }
  }

  override def toString: String = {
    s"""
       |core:${core}
       |
       |leaves:${leaves}
       |
       |fhsw:${fhsw}
       |""".stripMargin
  }

}