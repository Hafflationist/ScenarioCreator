package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.transformations.constraintBased.ConstraintBasedTestingUtils;
import de.mrobohm.utils.SSet;

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
        var name = new StringPlusNaked("Tabelle", Language.Mixed);
        var fdSet = ConstraintBasedTestingUtils.generateFd(columnList, random);
        return new Table(new IdSimple(idNum), name, columnList,
                Context.getDefault(), SSet.of(), fdSet);
    }
}