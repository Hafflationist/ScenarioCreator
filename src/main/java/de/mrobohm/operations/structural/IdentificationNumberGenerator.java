package de.mrobohm.operations.structural;

import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.table.Table;

import java.util.Comparator;
import java.util.Set;
import java.util.stream.Stream;

public final class IdentificationNumberGenerator {
    private IdentificationNumberGenerator() {
    }

    public static int[] generate(Set<Table> tableSet, int n) {
        var tableIdStream = tableSet.stream().map(Table::id);
        var columnIdStream = tableSet.stream()
                .flatMap(t -> t.columnList().stream().flatMap(IdentificationNumberGenerator::columnToIdStream));
        var idStream = Stream.concat(tableIdStream, columnIdStream);
        var maxId = idStream.max(Comparator.naturalOrder()).orElse(1);
        return Stream.iterate(maxId + 1, id -> id + 1).limit(n).mapToInt(Integer::intValue).toArray();
    }

    private static Stream<Integer> columnToIdStream(Column column) {
        return switch (column) {
            case ColumnCollection collection -> Stream.concat(
                    Stream.of(collection.id()),
                    collection.columnList().stream().flatMap(IdentificationNumberGenerator::columnToIdStream));
            case ColumnLeaf leaf -> Stream.of(leaf.id());
            case ColumnNode node -> Stream.concat(
                    Stream.of(node.id()),
                    node.columnList().stream().flatMap(IdentificationNumberGenerator::columnToIdStream));
        };
    }
}
