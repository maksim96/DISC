package org.apache.spark.disc

import org.apache.spark.disc.SubgraphCounting.countOnePatternOneGraph
import org.apache.spark.disc.optimization.rule_based.aggregate.CountTableCache
import org.apache.spark.disc.util.misc.{ExecutionMode, QueryType}

import java.io.{BufferedWriter, File, FileWriter}
import scala.io.Source

object BatchBatchSubgraphCounting{
  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }


  def main(args: Array[String]): Unit = {
    import scopt.OParser

    case class InputConfig(queryFile: String = "",
                           dataDirectory: String = "",
                           outputPrefix: String = "",
                           executionMode: String = "CountReturn",
                           queryType: String = "ISO",
                           core: String = "A",
                           platform: String = "")

    //parser for args
    val builder = OParser.builder[InputConfig]
    val parser1 = {
      import builder._
      OParser.sequence(
        programName("DISC"),
        head("DISC", "0.1"),
        opt[String]('b', "queryFile")
          .action((x, c) => c.copy(queryFile = x))
          .text("path to file that store pattern graph"),
        opt[String]('t', "dataDirectory")
          .action((x, c) => c.copy(dataDirectory = x))
          .text("input path of data graph"),
        opt[String]('o', "output")
          .action((x, c) => c.copy(outputPrefix = x))
          .text("prefix of output path"),
        opt[String]('e', "executionMode")
          .action((x, c) => c.copy(executionMode = x))
          .text(
            "[ShowPlan|Count|Result] (ShowPlan: show the query plan, Count: show sum of count, Result: execute and output counts of query)"
          ),
        opt[String]('u', "queryType")
          .action((x, c) => c.copy(queryType = x))
          .text(
            "[InducedISO|ISO|HOM] (InducedISO: Induced Isomorphism Count, ISO: Isomorphism Count, HOM: Homomorphism Count)"
          ),
        opt[String]('c', "orbit")
          .action((x, c) => c.copy(core = x))
          .text("orbit, i.e., A"),
        opt[String]('p', "environment")
          .action((x, c) => c.copy(platform = x))
          .text("path to environment configuration")
      )
    }

    OParser.parse(parser1, args, InputConfig()) match {
      case Some(config) =>
        val dataDirectory = config.dataDirectory
        val queryType = QueryType.withName(config.queryType)
        val executionMode = ExecutionMode.withName(config.executionMode)
        val orbit = config.core
        val queryFile = config.queryFile
        val platform = config.platform
        val outputPrefix = config.outputPrefix

        var dataGraphFiles = getListOfFiles(dataDirectory)



        val queries =
          Source
            .fromFile(queryFile)
            .getLines()
            .toArray
            .map(f => f.split("\\s"))
            .map(f => (f(0), f(1)))


        for (dataGraphID <- 0 to dataGraphFiles.size - 1) {
          val outputPath = outputPrefix + dataGraphFiles(dataGraphID).getName().split("\\.")(0) + "_embedding.txt"
          var counts = ""

          for (patternGraphID <- 0 to queries.size - 1) {

            CountTableCache.reset()

            val command1 =
              s"-q ${queries(patternGraphID)} -d ${dataGraphFiles(dataGraphID)} -o ${outputPath} -e $executionMode -u ${queryType} -c ${orbit} -p $platform"

            val count = countOnePatternOneGraph(command1.split("\\s"))
            counts = counts + count.toString() + "\n"


          }

          val file = new File(outputPath)
          val bw = new BufferedWriter(new FileWriter(file))
          bw.write(counts)
          bw.close()
        }
    }
  }
}
