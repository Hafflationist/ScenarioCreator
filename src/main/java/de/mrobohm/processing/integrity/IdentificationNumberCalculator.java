package de.mrobohm.processing.integrity;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.constraint.ColumnConstraint;
import de.mrobohm.data.column.constraint.ColumnConstraintUnique;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.identification.IdMerge;
import de.mrobohm.data.identification.IdPart;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.table.Table;
import de.mrobohm.utils.StreamExtensions;

import java.util.SortedSet;
import java.util.function.Function;
import java.util.stream.Stream;

public class IdentificationNumberCalculator {

    public static Stream<IdSimple> extractIdSimple(Id id) {
        return switch (id) {
            case IdSimple ids -> Stream.of(ids);
            case IdMerge idm -> Stream.concat(
                    extractIdSimple(Stream.of(idm.predecessorId1())),
                    extractIdSimple(Stream.of(idm.predecessorId2()))
            );
            case IdPart idp -> extractIdSimple(Stream.of(idp.predecessorId()));
        };
    }

    public static Stream<IdSimple> extractIdSimple(Stream<Id> idStream) {
        return idStream.parallel().flatMap(IdentificationNumberCalculator::extractIdSimple);
    }

    public static Stream<Id> getAllIds(Schema schema, boolean checkConstraints) {
        var tableSet = schema.tableSet();
        var tableIdStream = tableSet.parallelStream().map(Table::id);
        var columnIdStream = tableSet.parallelStream()
                .flatMap(t -> IdentificationNumberCalculator.tableToIdStream(t, checkConstraints));
        return Stream.concat(Stream.of(schema.id()), Stream.concat(tableIdStream, columnIdStream));
    }

    public static Stream<Id> tableToIdStream(Table table, boolean checkConstraints) {
        return table.columnList().stream()
                .flatMap(column -> columnToIdStream(column, checkConstraints));
    }

    public static Stream<Id> columnToIdStream(Column column, boolean checkConstraints) {
        var constraintIdStream = checkConstraints
                ? constraintsToIdStream(column.constraintSet())
                : Stream.<Id>empty();
        return switch (column) {
            case ColumnCollection collection -> Stream.of(
                            Stream.of(collection.id()),
                            constraintIdStream,
                            collection.columnList().parallelStream().flatMap(cc -> columnToIdStream(cc, checkConstraints)))
                    .flatMap(Function.identity());
            case ColumnLeaf leaf -> StreamExtensions.prepend(constraintIdStream, leaf.id());
            case ColumnNode node -> Stream.of(
                            Stream.of(node.id()),
                            constraintIdStream,
                            node.columnList().parallelStream().flatMap(cc -> columnToIdStream(cc, checkConstraints)))
                    .flatMap(Function.identity());
        };
    }

    private static Stream<Id> constraintsToIdStream(SortedSet<ColumnConstraint> constraintSet) {
        return constraintSet.parallelStream()
                .filter(c -> c instanceof ColumnConstraintUnique)
                .map(c -> (ColumnConstraintUnique) c)
                .map(ColumnConstraintUnique::getUniqueGroupId);
    }
}
