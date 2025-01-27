package disc.integration

import disc.util.{ExpData, ExpQuery, SparkFunSuite}
import org.apache.spark.disc.optimization.rule_based.aggregate.CountTableCache
import org.apache.spark.disc.util.misc.{Conf, QueryType}
import org.apache.spark.disc.{BatchSubgraphCounting, SubgraphCounting}

class DISCMainTest extends SparkFunSuite {

//  ("facebook", "facebook.txt"),
//  ("reactcome", "reactcome.txt"),
//  ("as-caida", "as-caida.txt"),
//  ("to", "topology.txt"),

//  val dataset = "reactcome"
  val dataset = "eu"

  def getPhyiscalPlan(dataset: String, query: String) = {
    val conf = Conf.defaultConf()
    val data = ExpData.getDataAddress(dataset)
//    val dmlString = new ExpQuery(data) getQuery (query)
    SubgraphCounting.phyiscalPlan(data, query, conf.orbit, conf.queryType)
  }

  def getLogicalPlan(dataset: String, query: String) = {
    val conf = Conf.defaultConf()
    val data = ExpData.getDataAddress(dataset)
//    val dmlString = new ExpQuery(data) getQuery (query)
    SubgraphCounting.logicalPlan(data, query, conf.orbit, conf.queryType)
  }

  test("expEntry") {
    val data = ExpData.getDataAddress(dataset)
    val outputPath = "./examples/out.csv"
    val executeMode = "Result"
//    val executeMode = "Count"
    //    val executeMode = "ShowPlan"
    val queryType = "HOM"

//    val platform = "Single"
    val platform = "Local"

    val expQuery = new ExpQuery(data)

    val queries = Seq("t1").map(f => expQuery.getDml(f))

    queries.foreach { query =>
//      CountTableCache.reset()
      val command1 =
        s"-q $query -d ${data} -o ${outputPath} -e $executeMode -u ${queryType} -c A -p $platform"

      SubgraphCounting.main(command1.split("\\s"))
    }
  }

  test("BatchSubgraphCounting"){
    val data = ExpData.getDataAddress(dataset)
    val outputPrefix = "./examples/"
    val executeMode = "Result"
    val queryType = "HOM"
    val platform = "disc_local.properties"
    val queryFile = "./examples/query.txt"
    val orbit = "A"

    val command1 =
      s"-b ${queryFile} -d ${data} -o ${outputPrefix} -e $executeMode -u ${queryType} -c ${orbit} -p $platform"

    BatchSubgraphCounting.main(command1.split("\\s"))
  }

  //check if whole pipeline can be compiled
  test("triangle") {
    val query = "triangle"
    val plan = getPhyiscalPlan(dataset, query)
    println(s"PhyiscalPlan:\n${plan.prettyString()}")

    val output = plan.count()
    println(s"Output Size:${output}")
  }

  test("3-node") {
    //check if whole pipeline can basically work

    val queries = Seq("wedge", "triangle")
    val discConf = Conf.defaultConf()
    discConf.queryType = QueryType.InducedISO

    queries.foreach { query =>
      val plan = getPhyiscalPlan(dataset, query)
      //      println(s"plan:\n${plan.prettyString()}")
      val outString = s"""
                           |----------------$query-------------------
                           |plan:\n${plan.prettyString()}
                           |patternSize:${plan.count()}
                           |""".stripMargin
      print(outString)
    }
  }

  test("4-node") {
    //check if whole pipeline can basically work

    val queries = Seq(
      "threePath",
      "threeStar",
      "triangleEdge",
      "square",
      "chordalSquare",
      "fourClique"
    )
    val discConf = Conf.defaultConf()
    discConf.queryType = QueryType.InducedISO

    queries.foreach { query =>
      val plan = getPhyiscalPlan(dataset, query)
      //      println(s"plan:\n${plan.prettyString()}")
      val outString = s"""
                         |----------------$query-------------------
                         |plan:\n${plan.prettyString()}
                         |""".stripMargin
      print(outString)
    }
  }

  //check if whole pipeline can basically work
  test("5-node") {
    //    val queries = Seq("wedge", "triangle")
    val discConf = Conf.defaultConf()
    discConf.queryType = QueryType.ISO

    val queries = Seq("house", "threeTriangle", "solarSquare", "near5Clique")
//    val queries = Seq("solarSquare")
//    val queries = Seq("house")
    queries.foreach { query =>
      CountTableCache.reset()
      val plan = getPhyiscalPlan(dataset, query)
//      println(s"plan:\n${plan.prettyString()}")
      val outString = s"""
                         |----------------$query-------------------
                         |plan:\n${plan.prettyString()}
                         |patternSize:${plan.count()}
                         |""".stripMargin
      print(outString)
    }
  }

  //check if whole pipeline can basically work
  test("6-node") {
//    val queries = Seq("wedge", "triangle")
    val discConf = Conf.defaultConf()
    discConf.queryType = QueryType.ISO

    val queries = Seq(
      "quadTriangle",
      "triangleCore",
      "twinCSquare",
      "twinClique4",
      "starofDavidPlus"
    )
    queries.foreach { query =>
      CountTableCache.reset()
      val plan = getPhyiscalPlan(dataset, query)
      println(s"plan:\n${plan.prettyString()}")
//      val outString = s"""
//                         |----------------$query-------------------
//                         |plan:\n${plan.prettyString()}
//                         |patternSize:${plan.count()}
//                         |""".stripMargin
//      print(outString)
    }
  }

}
