package de.mrobohm.operations.structural.base;

import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.operations.linguistic.helpers.LinguisticUtils;
import de.mrobohm.utils.StreamExtensions;

import java.util.List;
import java.util.Random;
import java.util.Set;

public class GroupColumnLeafsToNodeBase {

    public static List<Column> findGroupableColumns(List<Column> columnList, Random random) {
        var validColumnList = columnList.stream().filter(GroupColumnLeafsToNodeBase::areConstraintsFine).toList();
        var validColumnCount = validColumnList.size();
        var groupSize = random.nextInt(1 + validColumnCount);
        var ex = new RuntimeException("This should never happen.");
        return StreamExtensions
                .pickRandomOrThrowMultiple(validColumnList.stream(), groupSize, ex)
                .toList();
    }

    public static ColumnNode createNewColumnNode(int newId, List<Column> columnList, Random random) {
        assert columnList.size() > 0 : "createNewColumnNode wurde mit 0 Spalten aufgerufen!";

        // TODO: Vllt könnte man hier ein besseren neuen Namen finden...
        var allNames = columnList.stream().map(Column::name).toList();
        var newName = allNames.stream()
                .reduce((a, b) -> LinguisticUtils.merge(a, b, random))
                .orElse(allNames.get(0));
        return new ColumnNode(newId, newName, columnList, Set.of());
    }

    public static boolean areConstraintsFine(Column column) {
        return column.constraintSet().stream().noneMatch(constraint -> constraint instanceof ColumnConstraintPrimaryKey);
    }
}