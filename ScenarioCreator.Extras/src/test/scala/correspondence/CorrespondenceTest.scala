package correspondence

import org.scalatest.funsuite.AnyFunSuite
import scenarioCreator.data.identification._
import scenarioCreator.data.primitives.StringPlusNaked
import scenarioCreator.data.table.Table
import scenarioCreator.data.{Context, Language}
import scenarioCreator.utils.SSet

import scala.jdk.CollectionConverters._

class CorrespondenceTest extends AnyFunSuite {


    private def idToTable(id: Id): Table = new Table(
        id, new StringPlusNaked("name", Language.Technical), List().asJava, Context.getDefault, SSet.of(), SSet.of()
    )

    test("should create exist expressions") {
        // --- Arrange
        val rootId = new IdSimple(0)
        val id1 = new IdPart(new IdPart(rootId, 21, MergeOrSplitType.And), 12, MergeOrSplitType.Xor)
        val id2 = new IdPart(new IdPart(rootId, 21, MergeOrSplitType.And), 11, MergeOrSplitType.Xor)
        val id3 = new IdPart(rootId, 22, MergeOrSplitType.And)
        val table1 = idToTable(id1)
        val table2 = idToTable(id2)
        val table3 = idToTable(id3)
        val tables: List[Table] = List(table1, table2, table3)

        // --- Act
        val expression = Correspondence.convertIdsToExistExpression(tables, rootId)

        // --- Assert
        val expectedExpression = Some(
            ExistsConjunction[Table](Set(
                ExistsTerminal[Table](table3, Correspondence.tablePrinter),
                ExistsDisjunction[Table](Set(
                    ExistsTerminal[Table](table2, Correspondence.tablePrinter),
                    ExistsTerminal[Table](table1, Correspondence.tablePrinter)
                )
                )
            )
            )
        )
        assert(expectedExpression == expression)
    }
}
