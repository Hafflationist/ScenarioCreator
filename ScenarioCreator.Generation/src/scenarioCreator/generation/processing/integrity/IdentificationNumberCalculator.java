package scenarioCreator.generation.processing.integrity;

import scenarioCreator.data.Schema;
import scenarioCreator.data.column.constraint.ColumnConstraint;
import scenarioCreator.data.column.constraint.ColumnConstraintUnique;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnCollection;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.column.nesting.ColumnNode;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.identification.IdMerge;
import scenarioCreator.data.identification.IdPart;
import scenarioCreator.data.identification.IdSimple;
import scenarioCreator.data.table.Table;
import scenarioCreator.utils.StreamExtensions;

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
        final var tableSet = schema.tableSet();
        final var tableIdStream = tableSet.parallelStream().map(Table::id);
        final var columnIdStream = tableSet.parallelStream()
                .flatMap(t -> IdentificationNumberCalculator.tableToIdStream(t, checkConstraints));
        return Stream.concat(Stream.of(schema.id()), Stream.concat(tableIdStream, columnIdStream));
    }

    public static Stream<Id> tableToIdStream(Table table, boolean checkConstraints) {
        return table.columnList().stream()
                .flatMap(column -> columnToIdStream(column, checkConstraints));
    }

    public static Stream<Id> columnToIdStream(Column column, boolean checkConstraints) {
        final var constraintIdStream = checkConstraints
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
