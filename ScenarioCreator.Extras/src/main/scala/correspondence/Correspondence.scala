package correspondence

import scenarioCreator.data.identification.Id
import scenarioCreator.data.table.Table
import scenarioCreator.extra.correspondence.ExistsExpression


case class CorrespondenceRaw(
    rootTable: Table,
    targetTables: List[(Table, List[(Id, Id)])]
)

case class Correspondence(
    forall: Table,
    exists: ExistsExpression[Table]
)

object Correspondence {
    def prettyPrint(correspondence: Correspondence): String = {
        throw new NotImplementedError()
    }

    def of(corrRaw: CorrespondenceRaw): Correspondence = {
        throw new NotImplementedError()
    }
}