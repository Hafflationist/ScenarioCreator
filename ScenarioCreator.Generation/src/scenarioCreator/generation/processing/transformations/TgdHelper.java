package scenarioCreator.generation.processing.transformations;

import scenarioCreator.data.Schema;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.table.Table;
import scenarioCreator.data.tgds.ReducedRelation;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.StreamExtensions;

import java.util.List;
import java.util.Optional;

public class TgdHelper {
    private TgdHelper() {
    }

    public static List<TupleGeneratingDependency> calculateTgdsForUnchangedTables(Schema schema1, Schema schema2) {
        return schema1.tableSet().stream()
                .map(t1 -> schema2.tableSet().stream()
                        .filter(t2 -> t2.id().equals(t1.id()))
                        .findFirst()
                        .map(t2 -> new Pair<>(t1, t2)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(pair -> areTablesEquals(pair.first(), pair.second()))
                .map(Pair::first)
                .map(TgdHelper::trivialTgds)
                .toList();
    }

    private static boolean areTablesEquals(Table t1, Table t2) {
        final var sameId = t1.id().equals(t2.id());
        return sameId && StreamExtensions
                .zip(t1.columnList().stream(), t2.columnList().stream(), Pair::new)
                .allMatch(pair -> areColumnsEquals(pair.first(), pair.second()));
    }

    private static boolean areColumnsEquals(Column column1, Column column2) {
        return column1.id().equals(column2.id());
    }

    private static TupleGeneratingDependency trivialTgds(Table table) {
        final var reducedRelation = ReducedRelation.fromTable(table);
        return new TupleGeneratingDependency(
                List.of(reducedRelation),
                List.of(reducedRelation),
                List.of()
        );
    }
}