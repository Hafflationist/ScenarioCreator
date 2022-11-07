package processing.transformations.structural;

import scenarioCreator.data.Context;
import scenarioCreator.data.Language;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.identification.IdSimple;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.data.table.Table;
import scenarioCreator.generation.processing.transformations.constraintBased.ConstraintBasedTestingUtils;
import scenarioCreator.generation.processing.transformations.constraintBased.base.FunctionalDependencyManager;
import scenarioCreator.utils.SSet;

import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;

public final class StructuralTestingUtils {
    private StructuralTestingUtils() {
    }

    public static Function<Integer, Id[]> getIdGenerator(int start) {
        return n -> Stream
                .iterate(start + 1, id -> id + 1)
                .limit(n)
                .map(IdSimple::new)
                .toArray(IdSimple[]::new);
    }

    public static Table createTable(int idNum, List<Column> columnList) {
        return createTable(idNum, columnList, new Random());
    }

    public static Table createTable(int idNum, List<Column> columnList, Random random) {
        final var name = new StringPlusNaked("Tabelle", Language.Mixed);
        final var fdSet = FunctionalDependencyManager.transClosure(
                ConstraintBasedTestingUtils.generateFd(columnList, random)
        );
        return new Table(
                new IdSimple(idNum), name, columnList, Context.getDefault(), SSet.of(), fdSet
        );
    }
}