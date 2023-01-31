package id

import scenarioCreator.data.column.nesting.{Column, ColumnCollection, ColumnLeaf, ColumnNode}
import scenarioCreator.data.identification.{Id, IdMerge, IdPart, IdSimple}
import scenarioCreator.data.table.Table

import scala.jdk.CollectionConverters.ListHasAsScala

object Extractor {

    def extract(table: Table): List[Id] = {
        val extractedIds: List[Id] = table.columnList().asScala.toList
            .flatMap(column => extract(column))
        table.id() :: extractedIds
    }

    private def extract(column: Column): List[Id] = {
        column match {
            case leaf: ColumnLeaf => leaf.id() :: Nil
            case node: ColumnNode => node.id() :: extract(node)
            case col: ColumnCollection => col.id() :: extract(col)
        }
    }

    def intersect(id1: Id, id2: Id) : Boolean = {
        flatten(id1).intersect(flatten(id2)).nonEmpty
    }

    private def flatten(id: Id) : List[IdSimple] = {
        id match {
            case ids: IdSimple => ids :: Nil
            case idp: IdPart => flatten(idp.predecessorId())
            case idm: IdMerge => flatten(idm.predecessorId1()) ++ flatten(idm.predecessorId2())
        }
    }
}
