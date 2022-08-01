package de.mrobohm.processing.integrity;

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
import de.mrobohm.utils.Pair;

import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class IntegrityChecker {
    private IntegrityChecker() {
    }

    public static void assertValidSchema(Schema schema) {
        assertUniquenessOfIds(schema);
        assertValidForeignKeyConstraints(schema);
        assertValidForeignKeyConstraints(schema);
        assertValidFunctionalDependencies(schema);
    }

    private static void assertUniquenessOfIds(Schema schema) {
        var allIdList = IdentificationNumberCalculator.getAllIds(schema, false).toList();
        var nonUniqueIdSet = allIdList.stream()
                .filter(id -> allIdList.stream().filter(id2 -> Objects.equals(id2, id)).count() >= 2)
                .collect(Collectors.toCollection(TreeSet::new));
        assert nonUniqueIdSet.isEmpty() : "Non unique ids found: " + nonUniqueIdSet;
    }

    private static void assertValidForeignKeyConstraints(Schema schema) {
        var constraintIdPairList = extractConstraints(schema).toList();

        var problematicConstraints = constraintIdPairList.stream()
                .filter(pair -> pair.second() instanceof ColumnConstraintForeignKey)
                .filter(pair -> {
                    var sourceColumnId = pair.first();
                    var constraintForeignKey = (ColumnConstraintForeignKey) pair.second();
                    // searching for corresponding ColumnConstraintForeignKeyInverse:
                    return constraintIdPairList.stream()
                            .noneMatch(targetPair -> targetPair.first().equals(constraintForeignKey.foreignColumnId())
                                    && targetPair.second() instanceof ColumnConstraintForeignKeyInverse inverse
                                    && inverse.foreignColumnId().equals(sourceColumnId));
                })
                .map(pair -> {
                    var notFoundId = ((ColumnConstraintForeignKey) pair.second()).foreignColumnId();
                    return "relationship " + pair.first() + "->" + notFoundId + ": " + notFoundId + " or its (inverse) constraint part missing!\n";
                })
                .distinct()
                .toList();
        assert problematicConstraints.isEmpty()
                : "Invalid foreign key constraint found! (target missing: " + problematicConstraints + ")";

        var problematicConstraints2 = constraintIdPairList.stream()
                .filter(pair -> pair.second() instanceof ColumnConstraintForeignKeyInverse)
                .filter(pair -> {
                    var sourceColumnId = pair.first();
                    var constraintForeignKeyInverse = (ColumnConstraintForeignKeyInverse) pair.second();
                    // searching for corresponding ColumnConstraintForeignKeyInverse:
                    return constraintIdPairList.stream()
                            .noneMatch(targetPair -> targetPair.first().equals(constraintForeignKeyInverse.foreignColumnId())
                                    && targetPair.second() instanceof ColumnConstraintForeignKey inverse
                                    && inverse.foreignColumnId().equals(sourceColumnId));
                })
                .map(pair -> {
                    var notFoundId = ((ColumnConstraintForeignKeyInverse) pair.second()).foreignColumnId();
                    return "relationship " + notFoundId + "->" + pair.first() + ": " + notFoundId + " or its constraint part missing!\n";
                })
                .distinct()
                .toList();
        assert problematicConstraints2.isEmpty()
                : "Invalid foreign key constraint found! (source missing : " + problematicConstraints2 + ")";
    }

    private static Stream<Pair<Id, ColumnConstraint>> extractConstraints(Schema schema) {
        return schema.tableSet().stream()
                .flatMap(t -> t.columnList().stream())
                .flatMap(IntegrityChecker::extractConstraints);
    }

    private static Stream<Pair<Id, ColumnConstraint>> extractConstraints(Column column) {
        var constraints = switch (column) {
            case ColumnLeaf leaf -> leaf.constraintSet().stream().map(c -> new Pair<>(leaf.id(), c));
            case ColumnNode node -> Stream.concat(
                    node.constraintSet().stream().map(c -> new Pair<>(node.id(), c)),
                    node.columnList().stream().flatMap(IntegrityChecker::extractConstraints));
            case ColumnCollection collection -> Stream.concat(
                    collection.constraintSet().stream().map(c -> new Pair<>(collection.id(), c)),
                    collection.columnList().stream().flatMap(IntegrityChecker::extractConstraints));
        };
        return constraints
                .filter(t2 -> t2.second() instanceof ColumnConstraintForeignKey
                        || t2.second() instanceof ColumnConstraintForeignKeyInverse);
    }

    private static void assertValidFunctionalDependencies(Schema schema) {
        var errorReportSet = schema.tableSet().stream()
                .flatMap(t -> t.functionalDependencySet().stream()
                        .map(fd -> {
                            var circle = fd.left().stream().anyMatch(fd.right()::contains);
                            var allColumnIdInTableSet = t.columnList().stream()
                                    .flatMap(column -> IdentificationNumberCalculator.columnToIdStream(column, false))
                                    .collect(Collectors.toSet());
                            var invalidRefSet = Stream
                                    .concat(fd.right().stream(), fd.left().stream())
                                    .filter(id -> !allColumnIdInTableSet.contains(id))
                                    .collect(Collectors.toSet());
                            return new FunctionalDependencyErrorReport(t.id(), fd, circle, invalidRefSet);
                        })
                        .filter(fder -> fder.circle() || !fder.invalidRefSet().isEmpty()))
                .collect(Collectors.toSet());
        assert errorReportSet.isEmpty()
                : "Invalid functional dependencies found! (erroneous fds : " + errorReportSet + ")";
    }

    private record FunctionalDependencyErrorReport(
            Id tableId,
            FunctionalDependency fd,
            boolean circle,
            Set<Id> invalidRefSet
    ) {
    }
}
