package correspondence

import id.Extractor
import scenarioCreator.data.identification._
import scenarioCreator.data.table.Table


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

    def of(corrRaw: CorrespondenceRaw): Option[Correspondence ]= {
        // Dieser Teil wird hoch-IQ:
        // Ich hab ein bisschen nachgedacht, was man hier überhaupt genau machen muss und bin zu folgenden
        // vereinfachenden Schlüssen gekommen:
        // 1.  Grundsätzlich kann jedes IdMerge ignoriert werden, dass es nur dafür sorgt, dass Datensätze aus anderen
        //    Quellen ebenfalls dort es ich befinden. Das ist für den Existenzquantor irrelevant.
        // 2. Die wahre Magie spielt sich also in den IdPart-Teilen ab. Da IdPart-Teile jeweils die gleichen Vorgänger
        //    haben und sich nur durch die Zahl unterscheiden, kann man davon ausgehen, dass in der Liste alle möglichen
        //    Teile enthalten sind.
        // 3. Die IdPart-Teile bauen eine rekursive Struktur auf, die 1:1 in logische Formeln umgewandelt werden kann.
        //    Der entsprechende logische Operator ist bereits in die Art des IdParts integriert (splitType (mal wieder
        //    hoch-IQ von mir)).
        // 4. 1 bis 3 lassen den Algo entspannt von den Fingern gleiten.
        // 4.1. Man könnte eine Funktion bauen, die die Id-Struktur vereinfacht, indem IdMerge entfernt wird.
        // 4.2. Der Rest wird von einer rekursiven Funktion, die alles nach 3 umformt.
        convertIdsToExistExpression(corrRaw.targetTables.map(_._1), corrRaw.rootTable.id())
            .map(exists =>       Correspondence(corrRaw.rootTable, exists))
    }

    def convertIdsToExistExpression(tables: List[Table], rootId: Id): Option[ExistsExpression[Table]] = {
        def convertIdsToExistExpressionInner(t2n: List[(Table, List[Id])]): Option[ExistsExpression[Table]] = {
            def reduction(pair: (Table, List[Id])): (Table, List[Id]) = (pair._1, pair._2.tail)

            val optExpressionParts: List[Option[ExistsExpression[Table]]] = t2n
                .groupBy(pair => pair._2.head).values
                .map(idGroup => {
                    if (idGroup.length > 1)
                        convertIdsToExistExpressionInner(idGroup.map(reduction))
                    else if (idGroup.nonEmpty)
                        Some(ExistsTerminal[Table](idGroup.head._1))
                    else
                        Some(ExistsNowhere[Table]())
                }
                )
                .toList

            val validEvaluation: Boolean = optExpressionParts.forall(opt => opt.nonEmpty)
            assert(validEvaluation)
            val expressionParts: Set[ExistsExpression[Table]] = optExpressionParts
                .flatMap(opt => opt.toList)
                .toSet
            val splitType = Extractor.idToExtensionNumbers(t2n.head._1.id())
                .filter(id => id.isInstanceOf[IdPart])
                .map(id => id.asInstanceOf[IdPart])
                .last
                .splitType()
            if (splitType == MergeOrSplitType.And)
                Some(ExistsConjunction(expressionParts))
            else if (splitType == MergeOrSplitType.Xor)
                Some(ExistsConjunction(expressionParts))
            else
                Option.empty[ExistsExpression[Table]]
        }

        val t2n = tables
            .map(t => (t, Extractor.idToExtensionNumbers(Extractor.removeMerges(t.id(), rootId)).reverse))
        convertIdsToExistExpressionInner(t2n)
    }
}