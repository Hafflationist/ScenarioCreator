package id

import chase._
import org.utils.Correspondence
import scenarioCreator.data.Schema
import scenarioCreator.data.column.nesting.Column
import scenarioCreator.data.table.Table

import scala.jdk.CollectionConverters._;

object Id2KörnerkissenKorr {

    def qualifiedName(pair: (Table, Column)): String =
        pair._1.name().toString + pair._2.name().toString

    def isCorresponding(left: Column, right: Column): Boolean = {
        val leftIdSet = Extractor.flatten(left.id())
        val rightIdSet = Extractor.flatten(right.id())
        leftIdSet.intersect(rightIdSet).nonEmpty
    }

    // Die folgende Methode war einst angedacht, als noch die Illusion bestand, dass man 1:n und n:m-Korrs zurückgeben muss.
    // Zum Glück ist das nicht der Fall, weil die mir vorschwebende algorithmische Lösung aufwendig scheint.
    def aggregateRawCorrs(corrs: List[(Set[String], Set[String])]): List[(Set[String], Set[String])] = {
        // grouping over the left side:
        val allNamesLeft = corrs.flatMap(corr => corr._1)
        val allNamesRight = corrs.flatMap(corr => corr._2)

        val hugo2 = allNamesLeft
            .map(leftName => corrs
                .filter(pair => pair._1.contains(leftName))
                .map(pair => pair._1)
                .fold(Set())((a: Set[String], b: Set[String]) => a.union(b))
            )


        val hugo = corrs
            .foldLeft(List[(Set[String], Set[String])]())((acc: List[(Set[String], Set[String])], curr: (Set[String], Set[String])) => {
                if (acc.isEmpty)
                    List(curr)
                else {
                    val head :: tail = acc
                    if (head._1.intersect(curr._1).nonEmpty || head._2.intersect(curr._2).nonEmpty) {
                        val newHead = (head._1.union(curr._1), head._2.union(curr._2))
                        newHead :: tail
                    } else
                        curr :: head :: tail
                }
            }
            )
        throw new NotImplementedError("Lies den Kommentar über aggregateRawCorrs!")
    }

    def convert(schema: Schema): List[Correspondence[String]] = {
        val columnList = schema.tableSet().asScala.toList
            .flatMap(t => t.columnList.asScala.toList.map(column => (t, column)))

        // calculcating raw correspondences (all corrs are represented as an accumulation of 1:1 corrs)
        val rawCorrs = columnList
            .flatMap(pair => columnList.map(p => (pair, p)))
            .filter(nestedPair => isCorresponding(nestedPair._1._2, nestedPair._2._2))
            .map(nestedPair => (qualifiedName(nestedPair._1), qualifiedName(nestedPair._2)))

        // aggregating raw corrs to get corrs with lists

        // SKIP

        // convert to type of Körnerkissen
        rawCorrs.map(pair => new Correspondence[String](pair._1, pair._2, Double.NaN))
    }

}
