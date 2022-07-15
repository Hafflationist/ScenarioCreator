package de.mrobohm.processing.transformations.structural.base;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.constraint.ColumnConstraint;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.identification.Id;

import java.util.Map;
import java.util.stream.Collectors;

public class IdTranslation {
    public static Schema translateConstraints(Schema schema, Map<Id, Id> idTranslationMap) {
        var newTableSet = schema.tableSet().stream().map(t -> {
            var newColumnList = t.columnList().stream().map(column -> {
                var newConstraintSet = column.constraintSet().stream()
                        .map(c -> IdTranslation.translateConstraint(c, idTranslationMap))
                        .collect(Collectors.toSet());
                if (column.constraintSet().equals(newConstraintSet)) {
                    return column;
                }
                return switch (column) {
                    case ColumnLeaf leaf -> leaf.withConstraintSet(newConstraintSet);
                    case ColumnNode node -> node.withConstraintSet(newConstraintSet);
                    case ColumnCollection col -> col.withConstraintSet(newConstraintSet);
                };
            }).toList();
            if (t.columnList().equals(newColumnList)) {
                return t;
            }
            return t.withColumnList(newColumnList);
        }).collect(Collectors.toSet());
        return schema.withTables(newTableSet);
    }

    private static ColumnConstraint translateConstraint(ColumnConstraint constraint, Map<Id, Id> idTranslationMap) {
        var containsChangedId = switch (constraint) {
            case ColumnConstraintForeignKey ccfk -> idTranslationMap.containsKey(ccfk.foreignColumnId());
            case ColumnConstraintForeignKeyInverse ccfki -> idTranslationMap.containsKey(ccfki.foreignColumnId());
            default -> false;
        };
        if (containsChangedId) {
            return switch (constraint) {
                case ColumnConstraintForeignKey ccfk ->
                        ccfk.withForeignColumnId(idTranslationMap.get(ccfk.foreignColumnId()));
                case ColumnConstraintForeignKeyInverse ccfki ->
                        ccfki.withForeignColumnId(idTranslationMap.get(ccfki.foreignColumnId()));
                default -> throw new RuntimeException();
            };
        }
        return constraint;
    }
}
