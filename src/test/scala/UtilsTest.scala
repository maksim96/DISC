import org.apache.spark.Logo.Physical.utlis.{ListGenerator, PointToNumConverter}
import org.scalatest.FunSuite

class UtilsTest extends FunSuite{

  test("listGenerator"){
    //test list cross product
    val list1 = List.range(0,3).map(f => List(f))
    val list2 = List.range(1,4)
    val list3 = ListGenerator.crossProduct(list1, list2)

    val list4 = List(
      List(0,1),List(0,2),List(0,3),
      List(1,1),List(1,2),List(1,3),
      List(2,1),List(2,2),List(2,3)
      )

    list4.foreach(println)
    list3.foreach(println)

    assert(list3.zip(list4).forall(p => (p._1.diff(p._2).length == 0)))

    //test fill list
    val emptyList = ListGenerator.fillList(0,10)
    assert(emptyList.size == 10)


    // test generate catersian list
    val sizeList = List(3,3)
    val cartersianList = ListGenerator.cartersianSizeList(sizeList)
    val cartersianList1 = List(
      List(0,0),List(0,1),List(0,2),
      List(1,0),List(1,1),List(1,2),
      List(2,0),List(2,1),List(2,2)
    )

    assert(cartersianList.zip(cartersianList1).forall(p => (p._1.diff(p._2).length == 0)))

    //test fill cartersian list
    val slotMapping = List(1,2)
    val resultList = ListGenerator.fillListListIntoSlots(cartersianList,3,slotMapping)

    val resultList1 = List(
      List(0,0,0),List(0,0,1),List(0,0,2),
      List(0,1,0),List(0,1,1),List(0,1,2),
      List(0,2,0),List(0,2,1),List(0,2,2)
    )

    resultList.foreach(println)


    assert(resultList.zip(resultList1).forall(p => (p._1.diff(p._2).length == 0)))


    System.err.print("Error Test")
  }


  test("PointToNumConverter"){
    val baseList = List(3,4,5,6)
    val numList = List(1,2,3,4)
    val converter = new PointToNumConverter(baseList)
    val index = converter.convertToNum(numList)

    assert(index == 1*4*5*6+2*5*6+3*6+4)

    val numList1 = converter.NumToList(index)

    assert(numList.diff(numList).length == 0)







  }

}
