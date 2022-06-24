package de.mrobohm.operations.structural.base;

import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.table.Table;
import de.mrobohm.operations.linguistic.helpers.LinguisticUtils;
import de.mrobohm.utils.StreamExtensions;

import java.util.List;
import java.util.Random;
import java.util.Set;

public class GroupingColumnsBase {

    public static List<Column> findGroupableColumns(List<Column> columnList, Random random) {
        var validColumnList = columnList.stream().filter(GroupingColumnsBase::areConstraintsFine).toList();
        var validColumnCount = validColumnList.size();
        assert validColumnCount > 0;
        var groupSize = random.nextInt(validColumnCount) + 1;
        var ex = new RuntimeException("This should never happen.");
        return StreamExtensions
                .pickRandomOrThrowMultiple(validColumnList.stream(), groupSize, ex, random)
                .toList();
    }

    public static ColumnNode createNewColumnNode(Id newId, List<Column> columnList, Random random) {
        assert columnList.size() > 0 : "createNewColumnNode wurde mit 0 Spalten aufgerufen!";

        var newName = mergeNames(columnList, random);
        return new ColumnNode(newId, newName, columnList, Set.of(), false);
    }

    public static StringPlus mergeNames(List<Column> columnList, Random random){
        // TODO: Vllt könnte man hier ein besseren neuen Namen finden...
        var allNames = columnList.stream().map(Column::name).toList();
        return allNames.stream()
                .reduce((a, b) -> LinguisticUtils.merge(a, b, random))
                .orElse(allNames.get(0));
    }

    private static boolean areConstraintsFine(Column column) {
        return column.constraintSet().stream().noneMatch(constraint -> constraint instanceof ColumnConstraintPrimaryKey);
    }

    public static boolean containsGroupableColumns(List<Column> columnList) {
        return columnList.stream().anyMatch(GroupingColumnsBase::areConstraintsFine);
    }
}