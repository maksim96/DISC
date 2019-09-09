package org.apache.spark.adj.execution.hcube.utils

import org.apache.spark.adj.database.Catalog.{AttributeID, DataType}
import org.apache.spark.adj.database.RelationSchema
import org.apache.spark.adj.execution.subtask.{ArrayTrie, Trie}

class TriePreConstructor(attrOrders: Array[AttributeID],
                         schema: RelationSchema,
                         _content: Array[Array[DataType]])
    extends Serializable {

  private val content = _content.map(f => f.clone())

  def reorder() = {
    //The func that mapping idx-th value of each tuple to j-th pos, where j-th position is the reletive order of idx-th attribute determined via attribute order
    val tupleMappingFunc =
      attrOrders
        .filter(schema.containAttribute)
        .map(schema.attrIDs.indexOf(_))
    //    .zipWithIndex.reverse.sortBy(_._1).map(_._2).toArray
    val contentArity = schema.arity
    val contentSize = content.size
    val tempArray = new Array[DataType](contentArity)

    var i = 0
    while (i < contentSize) {
      val tuple = content(i)
      var j = 0

      while (j < contentArity) {
        tempArray(j) = tuple(j)
        j += 1
      }

      j = 0
      while (j < contentArity) {
        tuple(j) = tempArray(tupleMappingFunc(j))
        j += 1
      }

      i += 1
    }
  }

  def construct(): Trie = {
    reorder()
    ArrayTrie(content, schema.arity)
  }
}