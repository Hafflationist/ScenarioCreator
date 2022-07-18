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

import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

public class IdentificationNumberCalculator {

    public static Stream<IdSimple> extractIdSimple(Stream<Id> idStream) {
        return idStream.flatMap(id -> switch(id) {
            case IdSimple ids -> Stream.of(ids);
            case IdMerge idm -> Stream.concat(
                    extractIdSimple(Stream.of(idm.predecessorId1())),
                    extractIdSimple(Stream.of(idm.predecessorId2()))
            );
            case IdPart idp -> extractIdSimple(Stream.of(idp.predecessorId()));
        });
    }


    public static Stream<Id> getAllIds(Schema schema, boolean checkConstraints){
        var tableSet = schema.tableSet();
        var tableIdStream = tableSet.stream().map(Table::id);
        var columnIdStream = tableSet.stream()
                .flatMap(t -> t.columnList().stream().flatMap(column -> columnToIdStream(column, checkConstraints)));
        return Stream.concat(Stream.of(schema.id()), Stream.concat(tableIdStream, columnIdStream));
    }

    public static Stream<Id> columnToIdStream(Column column, boolean checkConstraints) {
        var constraintIdStream = checkConstraints
                ? constraintsToIdStream(column.constraintSet())
                : Stream.<Id>empty();
        return switch (column) {
            case ColumnCollection collection -> Stream.of(
                            Stream.of(collection.id()),
                            constraintIdStream,
                            collection.columnList().stream().flatMap(cc -> columnToIdStream(cc, checkConstraints)))
                    .flatMap(Function.identity());
            case ColumnLeaf leaf -> StreamExtensions.prepend(constraintIdStream, leaf.id());
            case ColumnNode node -> Stream.of(
                            Stream.of(node.id()),
                            constraintIdStream,
                            node.columnList().stream().flatMap(cc -> columnToIdStream(cc, checkConstraints)))
                    .flatMap(Function.identity());
        };
    }

    private static Stream<Id> constraintsToIdStream(Set<ColumnConstraint> constraintSet) {
        return constraintSet.stream()
                .filter(c -> c instanceof ColumnConstraintUnique)
                .map(c -> (ColumnConstraintUnique) c)
                .map(ColumnConstraintUnique::getUniqueGroupId);
    }
}
