package scenarioCreator.generation.processing.transformations.structural.base;

import scenarioCreator.data.column.constraint.ColumnConstraintPrimaryKey;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnNode;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.primitives.StringPlus;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.LinguisticUtils;
import scenarioCreator.utils.SSet;
import scenarioCreator.utils.StreamExtensions;

import java.util.List;
import java.util.Random;

public class GroupingColumnsBase {

    public static List<Column> findGroupableColumns(List<Column> columnList, Random random) {
        final var validColumnList = columnList.stream().filter(GroupingColumnsBase::areConstraintsFine).toList();
        final var validColumnCount = validColumnList.size();
        assert validColumnCount > 0;
        final var groupSize = random.nextInt(validColumnCount) + 1;
        final var ex = new RuntimeException("This should never happen.");
        return StreamExtensions
                .pickRandomOrThrowMultiple(validColumnList.stream(), groupSize, ex, random)
                .toList();
    }

    public static ColumnNode createNewColumnNode(Id newId, List<Column> columnList, Random random) {
        assert columnList.size() > 0 : "createNewColumnNode wurde mit 0 Spalten aufgerufen!";

        final var newName = mergeNames(columnList, random);
        return new ColumnNode(newId, newName, columnList, SSet.of(), false);
    }

    public static StringPlus mergeNames(List<Column> columnList, Random random) {
        final var allNames = columnList.stream().map(Column::name).toList();
        return allNames.stream()
                .reduce((a, b) -> LinguisticUtils.merge(a, b, random))
                .orElse(allNames.get(0));
    }

    private static boolean areConstraintsFine(Column column) {
        return !column.containsConstraint(ColumnConstraintPrimaryKey.class);
    }

    public static boolean containsGroupableColumns(List<Column> columnList) {
        return columnList.stream().anyMatch(GroupingColumnsBase::areConstraintsFine);
    }
}
