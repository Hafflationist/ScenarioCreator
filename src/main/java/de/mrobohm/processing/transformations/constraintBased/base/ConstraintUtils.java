package de.mrobohm.processing.transformations.constraintBased.base;

import de.mrobohm.data.column.constraint.ColumnConstraint;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.table.Table;

import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ConstraintUtils {
    private ConstraintUtils() {
    }

    public static Table replaceConstraints(Table t, Function<ColumnConstraint, Stream<ColumnConstraint>> switcher) {
        final var newColumnList = t.columnList().stream()
                .map(column -> replaceConstraints(column, switcher))
                .toList();
        return t.withColumnList(newColumnList);
    }

    private static Column replaceConstraints(Column column, Function<ColumnConstraint, Stream<ColumnConstraint>> switcher) {
        final var newConstraintSet = column.constraintSet().stream()
                .flatMap(switcher)
                .collect(Collectors.toCollection(TreeSet::new));
        return switch (column) {
            case ColumnLeaf leaf -> leaf.withConstraintSet(newConstraintSet);
            case ColumnNode node -> {
                final var newColumnList = node.columnList().stream()
                        .map(columnInner -> replaceConstraints(columnInner, switcher))
                        .toList();
                yield node
                        .withConstraintSet(newConstraintSet)
                        .withColumnList(newColumnList);
            }
            case ColumnCollection col -> {
                final var newColumnList = col.columnList().stream()
                        .map(columnInner -> replaceConstraints(columnInner, switcher))
                        .toList();
                yield col
                        .withConstraintSet(newConstraintSet)
                        .withColumnList(newColumnList);
            }
        };
    }
}
