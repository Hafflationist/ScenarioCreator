package de.mrobohm.operations.structural.base;

import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.operations.linguistic.helpers.LinguisticUtils;
import de.mrobohm.utils.StreamExtensions;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public class GroupColumnLeafsToNodeBase {

    public static Set<Column> findGroupableColumns(List<Column> columnList, Random random) {
        var validColumnList = columnList.stream().filter(GroupColumnLeafsToNodeBase::areConstraintsFine).toList();
        var validColumnCount = validColumnList.size();
        var groupSize = random.nextInt(1 + validColumnCount);
        var ex = new RuntimeException("This should never happen.");
        return StreamExtensions
                .pickRandomOrThrowMultiple(validColumnList.stream(), groupSize, ex)
                .collect(Collectors.toSet());
    }

    public static ColumnNode createNewColumnNode(int newId, Set<Column> columnSet, Random random) {
        assert columnSet.size() > 0 : "createNewColumnNode wurde mit 0 Spalten aufgerufen!";

        // TODO: Vllt könnte man hier ein besseren neuen Namen finden...
        var allNames = columnSet.stream().map(Column::name).toList();
        var newName = allNames.stream()
                .reduce((a, b) -> LinguisticUtils.merge(a, b, random))
                .orElse(allNames.get(0));
        return new ColumnNode(newId, newName, columnSet.stream().toList(), Set.of());
    }

    public static boolean areConstraintsFine(Column column) {
        return column.constraintSet().stream().noneMatch(constraint -> constraint instanceof ColumnConstraintPrimaryKey);
    }
}