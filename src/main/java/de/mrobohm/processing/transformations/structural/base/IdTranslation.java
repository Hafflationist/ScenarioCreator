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

import java.util.*;
import java.util.stream.Collectors;

public class IdTranslation {
    public static Schema translateConstraints(Schema schema, Map<Id, SortedSet<Id>> idTranslationMap, Set<Id> idPurgeSet) {
        var newTableSet = schema.tableSet().stream().map(t -> {
            var newColumnList = t.columnList().stream().map(column -> {
                var newConstraintSet = column.constraintSet().stream()
                        .flatMap(c -> IdTranslation.translateConstraint(c, idTranslationMap, idPurgeSet).stream())
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
                    t.functionalDependencySet(), idTranslationMap, idPurgeSet, t.columnList()
            );
            if (t.columnList().equals(newColumnList)
                    && t.functionalDependencySet().equals(newFunctionalDependencySet)) {
                return t;
            }
            return t
                    .withColumnList(newColumnList)
                    .withFunctionalDependencySet(newFunctionalDependencySet);
        }).collect(Collectors.toCollection(TreeSet::new));
        return schema.withTableSet(newTableSet);
    }

    private static SortedSet<ColumnConstraint> translateConstraint(
            ColumnConstraint constraint, Map<Id, SortedSet<Id>> idTranslationMap, Set<Id> idPurgeSet
    ) {
        var containsPurgedId = switch (constraint) {
            case ColumnConstraintForeignKey ccfk -> idPurgeSet.contains(ccfk.foreignColumnId());
            case ColumnConstraintForeignKeyInverse ccfki -> idPurgeSet.contains(ccfki.foreignColumnId());
            default -> false;
        };
        if (containsPurgedId) {
            return SSet.of();
        }
        var containsChangedId = switch (constraint) {
            case ColumnConstraintForeignKey ccfk -> idTranslationMap.containsKey(ccfk.foreignColumnId());
            case ColumnConstraintForeignKeyInverse ccfki -> idTranslationMap.containsKey(ccfki.foreignColumnId());
            default -> false;
        };
        if (!containsChangedId) {
            return SSet.of(constraint);
        }
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

    private static SortedSet<FunctionalDependency> translateFunctionalDependencySet(
            SortedSet<FunctionalDependency> fdSet,
            Map<Id, SortedSet<Id>> idTranslationMap,
            Set<Id> idPurgeSet,
            List<Column> columnList
    ) {
        var newFdSet = fdSet.stream()
                .filter(fd -> fd.left().stream().noneMatch(idPurgeSet::contains))
                .map(fd -> {
                    var newLeft = translateSideOfFunctionalDependency(
                            fd.left(), idTranslationMap, idPurgeSet, columnList
                    );
                    var newRight = translateSideOfFunctionalDependency(
                            fd.right(), idTranslationMap, idPurgeSet, columnList
                    );
                    var newFd = new FunctionalDependency(newLeft, newRight);
                    return fd.equals(newFd) ? fd : newFd;
                })
                .filter(fd -> !fd.left().isEmpty() && !fd.right().isEmpty())
                .collect(Collectors.toCollection(TreeSet::new));
        return fdSet.equals(newFdSet) ? fdSet : newFdSet;
    }

    private static SortedSet<Id> translateSideOfFunctionalDependency(
            SortedSet<Id> idSet,
            Map<Id, SortedSet<Id>> idTranslationMap,
            Set<Id> idPurgeSet,
            List<Column> columnList
    ) {
        var columnIdSet = columnList.stream()
                .flatMap(column -> IdentificationNumberCalculator.columnToIdStream(column, false))
                .collect(Collectors.toSet());

        var newIdSet = idSet.stream()
                .filter(id -> !idPurgeSet.contains(id))
                .map(id -> {
                    if (!idTranslationMap.containsKey(id)) {
                        return id;
                    }
                    var newIdList = idTranslationMap.get(id).stream()
                            .filter(columnIdSet::contains)
                            .toList();

                    assert !newIdList.isEmpty() : "Beim Übersetzen einer ID, wurde der Ursprung aus der Tabelle wegeskaliert. Soll das wirklich so?";
                    assert newIdList.size() == 1 : "Es liegen mehrere neue Ids in der Tabelle vor, dass kann nicht sein, da das Spalten von Spalten nicht unterstützt wird!";
                    return newIdList.get(0);
                }).collect(Collectors.toCollection(TreeSet::new));
        return idSet.equals(newIdSet) ? idSet : newIdSet;
    }
}
