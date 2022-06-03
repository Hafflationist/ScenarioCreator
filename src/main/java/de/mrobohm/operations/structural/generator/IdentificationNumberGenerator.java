package de.mrobohm.operations.structural.generator;

import de.mrobohm.data.column.constraint.ColumnConstraint;
import de.mrobohm.data.column.constraint.ColumnConstraintUnique;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.table.Table;

import java.util.Comparator;
import java.util.Set;
import java.util.function.Function;
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
        var constraintIdStream = constraintsToIdStream(column.constraintSet());
        return switch (column) {
            case ColumnCollection collection -> Stream.of(
                            Stream.of(collection.id()),
                            constraintIdStream,
                            collection.columnList().stream().flatMap(IdentificationNumberGenerator::columnToIdStream))
                    .flatMap(Function.identity());
            case ColumnLeaf leaf -> Stream.concat(Stream.of(leaf.id()), constraintIdStream);
            case ColumnNode node -> Stream.of(
                            Stream.of(node.id()),
                            constraintIdStream,
                            node.columnList().stream().flatMap(IdentificationNumberGenerator::columnToIdStream))
                    .flatMap(Function.identity());
        };
    }

    private static Stream<Integer> constraintsToIdStream(Set<ColumnConstraint> constraintSet) {
        return constraintSet.stream()
                .filter(c -> c instanceof ColumnConstraintUnique)
                .map(c -> (ColumnConstraintUnique) c)
                .map(ColumnConstraintUnique::getUniqueGroupId);
    }
}