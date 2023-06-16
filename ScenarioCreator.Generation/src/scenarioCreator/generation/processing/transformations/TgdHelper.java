package scenarioCreator.generation.processing.transformations;

import scenarioCreator.data.Schema;
import scenarioCreator.data.table.Table;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.utils.Pair;

import java.util.List;
import java.util.Optional;

public class TgdHelper {
    private TgdHelper() {
    }

    public static List<TupleGeneratingDependency> calculateTrivialTgds(Schema schema1, Schema schema2) {
        final var hugo = schema1.tableSet().stream()
                .map(t1 -> schema2.tableSet().stream()
                        .filter(t2 -> t2.id().equals(t1.id()))
                        .findFirst()
                        .map(t2 -> new Pair<>(t1,t2)))
                .filter(Optional::isPresent)
                .map(Optional::get);
        //throw new RuntimeException("Implement me!");
        // TODO: Implement me!
        return List.of();
    }

    private static boolean areTableEquals(Table t1, Table t2) {
        throw new RuntimeException("Implement me!");
        //t1.c
    }
}
