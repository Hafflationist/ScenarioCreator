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
        throw new NotImplementedError()
    }
}