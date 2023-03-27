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

    private def extract(column: Column): List[Id] =
        column match {
            case leaf: ColumnLeaf => leaf.id() :: Nil
            case node: ColumnNode => node.id() :: extract(node)
            case col: ColumnCollection => col.id() :: extract(col)
        }

    def intersect(id1: Id, id2: Id): Boolean =
        flatten(id1).intersect(flatten(id2)).nonEmpty

    private def flatten(id: Id): List[IdSimple] =
        id match {
            case ids: IdSimple => ids :: Nil
            case idp: IdPart => flatten(idp.predecessorId())
            case idm: IdMerge => flatten(idm.predecessorId1()) ++ flatten(idm.predecessorId2())
        }

    def removeMerges(id: Id, rootId: Id): Id =
        id match {
            case ids: IdSimple => ids
            case idp: IdPart => new IdPart(removeMerges(idp.predecessorId(), rootId), idp.extensionNumber(), idp.splitType())
            case idm: IdMerge =>
                if (intersect(idm.predecessorId1(), rootId))
                    removeMerges(idm.predecessorId1(), rootId)
                else
                    removeMerges(idm.predecessorId2(), rootId)
        }

    def removePartsAndXorMerges(id: Id, rootId: Id): Id =
        id match {
            case ids: IdSimple => ids
            case idp: IdPart => removePartsAndXorMerges(idp.predecessorId(), rootId)
            case idm: IdMerge =>
                if (intersect(idm.predecessorId1(), rootId))
                    removePartsAndXorMerges(idm.predecessorId1(), rootId)
                else
                    removePartsAndXorMerges(idm.predecessorId2(), rootId)
        }

    def idToExtensionNumbers(id: Id): List[Id] =
        id match {
            case _: IdSimple => Nil
            case idp: IdPart => idp :: idToExtensionNumbers(idp.predecessorId())
            case _: IdMerge => throw new NotImplementedError()
        }
}
