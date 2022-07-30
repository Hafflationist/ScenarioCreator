package de.mrobohm.processing.transformations.structural.base;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.constraint.ColumnConstraint;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.table.FunctionalDependency;
import de.mrobohm.processing.integrity.IdentificationNumberCalculator;
import de.mrobohm.utils.SSet;

import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class IdTranslation {
    public static Schema translateConstraints(Schema schema, Map<Id, SortedSet<Id>> idTranslationMap) {
        var newTableSet = schema.tableSet().stream().map(t -> {
            var newColumnList = t.columnList().stream().map(column -> {
                var newConstraintSet = column.constraintSet().stream()
                        .flatMap(c -> IdTranslation.translateConstraint(c, idTranslationMap).stream())
                        .collect(Collectors.toCollection(TreeSet::new));
                if (column.constraintSet().equals(newConstraintSet)) {
                    return column;
                }
                return switch (column) {
                    case ColumnLeaf leaf -> leaf.withConstraintSet(newConstraintSet);
                    case ColumnNode node -> node.withConstraintSet(newConstraintSet);
                    case ColumnCollection col -> col.withConstraintSet(newConstraintSet);
                };
            }).toList();
            var newFunctionalDependencySet = translateFunctionalDependencySet(
                    t.functionalDependencySet(), idTranslationMap, t.columnList()
            );
            if (t.columnList().equals(newColumnList)
                    && t.functionalDependencySet().equals(newFunctionalDependencySet)) {
                return t;
            }
            return t
                    .withColumnList(newColumnList)
                    .withFunctionalDepencySet(newFunctionalDependencySet);
        }).collect(Collectors.toCollection(TreeSet::new));
        return schema.withTables(newTableSet);
    }

    private static SortedSet<ColumnConstraint> translateConstraint(ColumnConstraint constraint, Map<Id, SortedSet<Id>> idTranslationMap) {
        var containsChangedId = switch (constraint) {
            case ColumnConstraintForeignKey ccfk -> idTranslationMap.containsKey(ccfk.foreignColumnId());
            case ColumnConstraintForeignKeyInverse ccfki -> idTranslationMap.containsKey(ccfki.foreignColumnId());
            default -> false;
        };
        if (containsChangedId) {
            return switch (constraint) {
                case ColumnConstraintForeignKey ccfk -> idTranslationMap.get(ccfk.foreignColumnId()).stream()
                        .map(ccfk::withForeignColumnId)
                        .collect(Collectors.toCollection(TreeSet::new));
                case ColumnConstraintForeignKeyInverse ccfki -> idTranslationMap.get(ccfki.foreignColumnId()).stream()
                        .map(ccfki::withForeignColumnId)
                        .collect(Collectors.toCollection(TreeSet::new));
                default -> throw new RuntimeException();
            };
        }
        return SSet.of(constraint);
    }

    private static SortedSet<FunctionalDependency> translateFunctionalDependencySet(
            SortedSet<FunctionalDependency> fdSet,
            Map<Id, SortedSet<Id>> idTranslationMap,
            List<Column> columnList
    ) {
        var newFdSet = fdSet.stream().map(fd -> {
            var newLeft = translateSideOfFunctionalDependeny(fd.left(), idTranslationMap, columnList);
            var newRight = translateSideOfFunctionalDependeny(fd.right(), idTranslationMap, columnList);
            var newFd = new FunctionalDependency(newLeft, newRight);
            return fd.equals(newFd) ? fd : newFd;
        }).collect(Collectors.toCollection(TreeSet::new));
        return fdSet.equals(newFdSet) ? fdSet : newFdSet;
    }

    private static SortedSet<Id> translateSideOfFunctionalDependeny(
            SortedSet<Id> idSet,
            Map<Id, SortedSet<Id>> idTranslationMap,
            List<Column> columnList
    ) {
        var columnIdSet = columnList.stream()
                .flatMap(column -> IdentificationNumberCalculator.columnToIdStream(column, false))
                .collect(Collectors.toSet());

        var newIdSet = idSet.stream().map(id -> {
            if(!idTranslationMap.containsKey(id))  {
                return id;
            }
            var newIdList = idTranslationMap.get(id).stream()
                    .filter(columnIdSet::contains)
                    .toList();

            assert !newIdList.isEmpty() : "Beim Übersetzen einer ID, wurde der Ursprung aus der Tabelle wegeskaliert. Soll das wirklich so?";
            assert newIdList.size() == 1: "Es liegen mehrere neue Ids in der Tabelle vor, dass kann nicht sein, da das Spalten von Spalten nicht unterstützt wird!";
            return newIdList.get(0);
        }).collect(Collectors.toCollection(TreeSet::new));
        return idSet.equals(newIdSet) ? idSet : newIdSet;
    }
}
