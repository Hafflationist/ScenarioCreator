package scenarioCreator.generation.processing.transformations.structural.base;

import scenarioCreator.data.Schema;
import scenarioCreator.data.column.constraint.ColumnConstraint;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKey;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKeyInverse;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.table.FunctionalDependency;
import scenarioCreator.generation.processing.integrity.IdentificationNumberCalculator;
import scenarioCreator.generation.processing.transformations.constraintBased.base.ConstraintUtils;
import scenarioCreator.utils.SSet;

import java.util.*;
import java.util.stream.Collectors;

public class IdTranslation {
    public static Schema translateConstraints(Schema schema, Map<Id, SortedSet<Id>> idTranslationMap, Set<Id> idPurgeSet) {
        final var newTableSet = schema.tableSet().stream().map(t -> {
            final var newFunctionalDependencySet = translateFunctionalDependencySet(
                    t.functionalDependencySet(), idTranslationMap, idPurgeSet, t.columnList()
            );
            final var newTable = ConstraintUtils
                    .replaceConstraints(t, c -> IdTranslation.translateConstraint(c, idTranslationMap, idPurgeSet).stream())
                    .withFunctionalDependencySet(newFunctionalDependencySet);
            if(newTable.equals(t)){
                return t;
            }
            return newTable;
        }).collect(Collectors.toCollection(TreeSet::new));
        return schema.withTableSet(newTableSet);
    }

    private static SortedSet<ColumnConstraint> translateConstraint(
            ColumnConstraint constraint, Map<Id, SortedSet<Id>> idTranslationMap, Set<Id> idPurgeSet
    ) {
        final var containsPurgedId = switch (constraint) {
            case ColumnConstraintForeignKey ccfk -> idPurgeSet.contains(ccfk.foreignColumnId());
            case ColumnConstraintForeignKeyInverse ccfki -> idPurgeSet.contains(ccfki.foreignColumnId());
            default -> false;
        };
        if (containsPurgedId) {
            return SSet.of();
        }
        final var containsChangedId = switch (constraint) {
            case ColumnConstraintForeignKey ccfk -> idTranslationMap.containsKey(ccfk.foreignColumnId());
            case ColumnConstraintForeignKeyInverse ccfki -> idTranslationMap.containsKey(ccfki.foreignColumnId());
            default -> false;
        };
        if (!containsChangedId) {
            return SSet.of(constraint);
        }
        return switch (constraint) {
            case ColumnConstraintForeignKey ccfk -> idTranslationMap.get(ccfk.foreignColumnId()).stream()
                    .map(ColumnConstraintForeignKey::new)
                    .collect(Collectors.toCollection(TreeSet::new));
            case ColumnConstraintForeignKeyInverse ccfki -> idTranslationMap.get(ccfki.foreignColumnId()).stream()
                    .map(ColumnConstraintForeignKeyInverse::new)
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
//        final var newFdSet = fdSet.stream()
//                .filter(fd -> fd.left().stream().noneMatch(idPurgeSet::contains))
//                .map(fd -> {
//                    final var newLeft = translateSideOfFunctionalDependency(
//                            fd.left(), idTranslationMap, idPurgeSet, columnList
//                    );
//                    final var newRight = translateSideOfFunctionalDependency(
//                            fd.right(), idTranslationMap, idPurgeSet, columnList
//                    );
//                    final var newFd = new FunctionalDependency(newLeft, newRight);
//                    return fd.equals(newFd) ? fd : newFd;
//                })
//                .filter(fd -> !fd.left().isEmpty() && !fd.right().isEmpty())
//                .collect(Collectors.toCollection(TreeSet::new));
//        return fdSet.equals(newFdSet) ? fdSet : newFdSet;
        return fdSet;
    }

    private static SortedSet<Id> translateSideOfFunctionalDependency(
            SortedSet<Id> idSet,
            Map<Id, SortedSet<Id>> idTranslationMap,
            Set<Id> idPurgeSet,
            List<Column> columnList
    ) {
        final var columnIdSet = columnList.stream()
                .flatMap(column -> IdentificationNumberCalculator.columnToIdStream(column, false))
                .collect(Collectors.toSet());

        final var newIdSet = idSet.stream()
                .filter(id -> !idPurgeSet.contains(id))
                .map(id -> {
                    if (!idTranslationMap.containsKey(id)) {
                        return id;
                    }
                    final var newIdList = idTranslationMap.get(id).stream()
                            .filter(columnIdSet::contains)
                            .toList();

                    assert !newIdList.isEmpty() : "Beim Übersetzen einer ID, wurde der Ursprung aus der Tabelle wegeskaliert. Soll das wirklich so?";
                    assert newIdList.size() == 1 : "Es liegen mehrere neue Ids in der Tabelle vor, dass kann nicht sein, da das Spalten von Spalten nicht unterstützt wird!";
                    return newIdList.get(0);
                }).collect(Collectors.toCollection(TreeSet::new));
        return idSet.equals(newIdSet) ? idSet : newIdSet;
    }
}
