package correspondence

import id.Extractor
import scenarioCreator.data.Schema
import scenarioCreator.data.identification.Id
import scenarioCreator.data.table.Table

import scala.jdk.CollectionConverters.SetHasAsScala

object Conversion {

    private def getCorrespondingTables(rootTable: Table, targetSchema: Schema): CorrespondenceRaw = {
        val rootIds = Extractor.extract(rootTable)
        CorrespondenceRaw(
            rootTable,
            targetSchema.tableSet().asScala.toList
            .map(targetTable => (targetTable, areIntersecting(rootIds, targetTable)))
            .filter(pair => pair._2.nonEmpty)
        )
    }

    private def areIntersecting(rootIds: List[Id], targetTable: Table) = {
        val targetIds = Extractor.extract(targetTable)
        rootIds.flatMap(rid => targetIds.find(tid => Extractor.intersect(rid, tid)).map(tid => (rid, tid)))
    }

    def convert(rootSchema: Schema, targetSchema: Schema): List[Correspondence] = {
        rootSchema.tableSet().asScala.toList
            .map(table => getCorrespondingTables(table, targetSchema))
            .map(Correspondence.of)
    }
}
