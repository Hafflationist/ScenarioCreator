package de.mrobohm.integrity;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.constraint.ColumnConstraint;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.*;
import de.mrobohm.utils.Pair;

import java.util.stream.Stream;

public final class IntegrityChecker {
    private IntegrityChecker() {
    }

    public static void assertValidSchema(Schema schema) {
        var constraintIdPairStream = schema.tableSet().stream()
                .flatMap(t -> t.columnList().stream())
                .flatMap(IntegrityChecker::extractConstraints)
                .toList();

        // TODO: Man könnte die ganze Sachen auch umgekehrt betrachten, denn jedes Inversteil müsste ein entsprechendes
        // normales Teil besitzen. Wenn das normale Teil nicht vorliegt, entsteht allerdings kein Schaden an anderer Stelle
        var everythingOk = constraintIdPairStream.stream()
                .filter(pair -> pair.second() instanceof ColumnConstraintForeignKey)
                .allMatch(pair -> {
            var sourceColumnId = pair.first();
            var constraintForeignKey = (ColumnConstraintForeignKey) pair.second();
            // searching for corresponding ColumnConstraintForeignKeyInverse:
            return constraintIdPairStream.stream()
                    .anyMatch(targetPair -> targetPair.first() == constraintForeignKey.foreignColumnId()
                        && targetPair.second() instanceof ColumnConstraintForeignKeyInverse inverse
                        && inverse.foreignColumnId() == sourceColumnId);
        });

        assert everythingOk : "Invalid foreign key constraint found!";
    }

    private static Stream<Pair<Integer, ColumnConstraint>> extractConstraints(Column column) {
        var constraints = switch (column) {
            case ColumnLeaf leaf -> leaf.constraintSet().stream().map(c -> new Pair<>(leaf.id(), c));
            case ColumnNode node ->
                    Stream.concat(
                            node.constraintSet().stream().map(c -> new Pair<>(node.id(), c)),
                            node.columnList().stream().flatMap(IntegrityChecker::extractConstraints));
            case ColumnCollection collection ->
                    Stream.concat(
                            collection.constraintSet().stream().map(c -> new Pair<>(collection.id(), c)),
                            collection.columnList().stream().flatMap(IntegrityChecker::extractConstraints));
        };
        return constraints
                .filter(t2 -> t2.second() instanceof ColumnConstraintForeignKey
                        || t2.second() instanceof ColumnConstraintForeignKeyInverse);
    }
}
