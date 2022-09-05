package de.mrobohm.processing.transformations.constraintBased.base;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.identification.IdMerge;
import de.mrobohm.data.identification.IdPart;
import de.mrobohm.data.identification.MergeOrSplitType;
import de.mrobohm.data.table.FunctionalDependency;
import de.mrobohm.processing.integrity.IdentificationNumberCalculator;
import de.mrobohm.utils.Pair;
import de.mrobohm.utils.SSet;
import one.util.streamex.StreamEx;

import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class FunctionalDependencyManager {
    private FunctionalDependencyManager() {
    }

    public static SortedSet<FunctionalDependency> getValidFdSet(
            SortedSet<FunctionalDependency> functionalDependencySet,
            List<Column> columnList
    ) {
        final var allColumnIdSet = columnList.stream()
                .flatMap(column -> IdentificationNumberCalculator.columnToIdStream(column, false))
                .collect(Collectors.toSet());
        return functionalDependencySet.stream()
                .map(fd -> {
                    final var newLeft = getValidLeftHandSide(fd.left(), allColumnIdSet);
                    final var newRight = getValidRightHandSide(fd.right(), allColumnIdSet);
                    return new FunctionalDependency(newLeft, newRight);
                })
                .filter(fd -> !fd.left().isEmpty())
                .filter(fd -> !fd.right().isEmpty())
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private static SortedSet<Id> getValidLeftHandSide(SortedSet<Id> leftHandSide, Set<Id> allColumnIdSet) {
        // Wenn der splitType AND ist, wäre die linke Seite der funktionalen Abhängigkeit genau dann gültig,
        // wenn man alle Nachfolge-IdParts hinzufügen würde.
        // Da Spalten eigentlich nicht gespalten werden, wird dieser Fall nicht beachtet.
        // (Können ColumnNodes gespalten werden?)

        final var predToIdPartMap = allColumnIdSet.stream()
                .filter(id -> id instanceof IdPart idp && idp.splitType().equals(MergeOrSplitType.Xor)
                        || id instanceof IdMerge)
                .flatMap(id -> switch (id) {
                    case IdPart idp -> Stream.of(new Pair<>(idp.predecessorId(), id));
                    case IdMerge idm -> Stream.of(
                            new Pair<>(idm.predecessorId1(), id),
                            new Pair<>(idm.predecessorId2(), id)
                    );
                    default -> throw new IllegalStateException("Unexpected value: " + id);
                })
                .collect(Collectors.toMap(Pair::first, Pair::second));

        final var newLeftHandSide = leftHandSide.stream()
                .map(id -> predToIdPartMap.getOrDefault(id, id))
                .collect(Collectors.toCollection(TreeSet::new));

        final var criticalIdSet = newLeftHandSide.stream()
                .filter(id -> !allColumnIdSet.contains(id))
                .collect(Collectors.toSet());
        if (criticalIdSet.isEmpty()) {
            return newLeftHandSide;
        }
        return SSet.of();
    }

    private static SortedSet<Id> getValidRightHandSide(SortedSet<Id> rightHandSide, Set<Id> allColumnIdSet) {
        final var predToIdPartMap = allColumnIdSet.stream()
                .filter(id -> id instanceof IdPart)
                .map(id -> (IdPart) id)
                .collect(Collectors.toMap(IdPart::predecessorId, idp -> (Id) idp));

        final var validIdMerge = allColumnIdSet.stream()
                .filter(id -> id instanceof IdMerge)
                .map(id -> (IdMerge) id)
                .filter(idm -> rightHandSide.contains(idm.predecessorId1()) && rightHandSide.contains(idm.predecessorId2()))
                .map(idm -> (Id) idm);

        return SSet.concat(
                rightHandSide.stream()
                        .map(id -> predToIdPartMap.getOrDefault(id, id))
                        .filter(allColumnIdSet::contains)
                        .collect(Collectors.toCollection(TreeSet::new)),
                validIdMerge
        );
    }

    public static Schema transClosure(Schema schema) {
        final var newTableSet = schema.tableSet().stream()
                .map(t -> {
                    final var newFdSet = transClosure(t.functionalDependencySet());
                    if (newFdSet.equals(t.functionalDependencySet())) {
                        return t;
                    }
                    return t.withFunctionalDependencySet(newFdSet);
                })
                .collect(Collectors.toCollection(TreeSet::new));
        if (schema.tableSet().equals(newTableSet)) {
            return schema;
        }
        return schema.withTableSet(newTableSet);
    }

    /**
     * @param fdSet
     * @return for each fd the right-hand side is expanded to the attribute closure of the left-hand side
     */
    public static SortedSet<FunctionalDependency> transClosure(SortedSet<FunctionalDependency> fdSet) {
        return transClosure(fdSet, true);
    }

    /**
     * @param fdSet
     * @return for each fd the right-hand side is expanded to the attribute closure of the left-hand side
     */
    public static SortedSet<FunctionalDependency> transClosure(SortedSet<FunctionalDependency> fdSet, boolean groupRightSide) {
        return fdSet.stream()
                .map(FunctionalDependency::left)
                .flatMap(attrSet -> {
                    final var attrClosure = attributeClosure(attrSet, fdSet).stream()
                            .filter(id -> !attrSet.contains(id))
                            .collect(Collectors.toCollection(TreeSet::new));
                    if (groupRightSide) {
                        return Stream.of(new FunctionalDependency(attrSet, attrClosure));
                    }
                    return attrClosure.stream().map(rid -> new FunctionalDependency(attrSet, SSet.of(rid)));
                })
                .collect(Collectors.toCollection(TreeSet::new));
    }
    // In Wirklichkeit brauchen wir die abgeschlossene Hülle,
    // wobei die linke und rechte Seite jeder funktionalen Abhängigkeit disjunkt sein müssen.
    // Die restlichen funktionalen Abhängigkeiten bringen hier keinen Mehrwert.

    public static SortedSet<Id> attributeClosure(SortedSet<Id> forColumnIdSet, SortedSet<FunctionalDependency> fdSet) {
        return StreamEx
                .iterate(forColumnIdSet, acc -> {
                    final var newAttributes = fdSet.stream()
                            .filter(fd -> acc.containsAll(fd.left()))
                            .flatMap(fd -> fd.right().stream());
                    return SSet.concat(acc, newAttributes);
                })
                .pairMap(Pair::new)
                .findFirst(pair -> pair.first().size() == pair.second().size())
                .map(Pair::first)
                .orElseThrow();
    }

    public static boolean membership(FunctionalDependency fd, SortedSet<FunctionalDependency> fdSet) {
        final var attrClosure = attributeClosure(fd.left(), fdSet);
        return attrClosure.containsAll(fd.right());
    }

    public static SortedSet<FunctionalDependency> minimalCover(SortedSet<FunctionalDependency> fdSet) {
        return SSet.foldLeft(fdSet, fdSet, (acc, fd) -> {
            final var accWithoutFd = acc.stream()
                    .filter(f -> !f.equals(fd))
                    .collect(Collectors.toCollection(TreeSet::new));
            if (membership(fd, accWithoutFd)) {
                return accWithoutFd;
            }
            return acc;
        });
    }
}