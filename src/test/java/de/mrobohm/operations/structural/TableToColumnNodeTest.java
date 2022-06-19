package de.mrobohm.operations.structural;

import de.mrobohm.data.*;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.integrity.IntegrityChecker;
import junit.framework.AssertionFailedError;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class TableToColumnNodeTest {

    @Test
    void transformWithoutDeletionOfIngestingColumn() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var column1 = new ColumnLeaf(1, name, dataType, ColumnContext.getDefault(), Set.of());
        var column2 = new ColumnLeaf(3, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(4, Set.of())));
        var ingestedColumn = new ColumnLeaf(2, name, dataType.withIsNullable(true), ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(4, Set.of())));
        var ingestingColumn = new ColumnLeaf(4, name, dataType.withIsNullable(true), ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKeyInverse(2, Set.of()),
                        new ColumnConstraintForeignKeyInverse(3, Set.of()))
        );

        var table = new Table(12, name, List.of(column2), Context.getDefault(), Set.of());
        var ingestingTable = new Table(10, name, List.of(ingestingColumn), Context.getDefault(), Set.of());
        var ingestedTable = new Table(11, name, List.of(column1, ingestedColumn), Context.getDefault(), Set.of());
        var tableSet = Set.of(ingestingTable, ingestedTable, table);
        var schema = new Schema(15, name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        var transformation = new TableToColumnNode(false, false);

        // --- Act
        var newSchema = transformation.transform(schema, new Random());

        // --- Assert
        IntegrityChecker.assertValidSchema(newSchema);
        var oldIdSet = schema.tableSet().stream()
                .flatMap(t -> t.columnList().stream())
                .map(Column::id)
                .collect(Collectors.toSet());
        var newIdSet = newSchema.tableSet().stream()
                .flatMap(t -> t.columnList().stream())
                .flatMap(column -> switch (column) {
                    case ColumnLeaf leaf -> Stream.of(leaf);
                    case ColumnNode node -> node.columnList().stream();
                    case ColumnCollection ignore -> throw new AssertionFailedError();
                })
                .map(Column::id)
                .collect(Collectors.toSet());
        Assertions.assertEquals(oldIdSet, newIdSet);
    }

    @Test
    void transformWithDeletionOfIngestingColumn() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var column1 = new ColumnLeaf(1, name, dataType, ColumnContext.getDefault(), Set.of());
        var ingestedColumn = new ColumnLeaf(2, name, dataType.withIsNullable(true), ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(4, Set.of())));
        var ingestingColumn = new ColumnLeaf(4, name, dataType.withIsNullable(true), ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKeyInverse(2, Set.of()))
        );

        var ingestingTable = new Table(10, name, List.of(ingestingColumn), Context.getDefault(), Set.of());
        var ingestedTable = new Table(11, name, List.of(column1, ingestedColumn), Context.getDefault(), Set.of());
        var tableSet = Set.of(ingestingTable, ingestedTable);
        var schema = new Schema(15, name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        var transformation = new TableToColumnNode(false, false);

        // --- Act
        var newSchema = transformation.transform(schema, new Random());

        // --- Assert
        IntegrityChecker.assertValidSchema(newSchema);
        var newIdSet = newSchema.tableSet().stream()
                .flatMap(t -> t.columnList().stream())
                .flatMap(column -> switch (column) {
                    case ColumnLeaf leaf -> Stream.of(leaf);
                    case ColumnNode node -> node.columnList().stream();
                    case ColumnCollection ignore -> throw new AssertionFailedError();
                })
                .map(Column::id)
                .collect(Collectors.toSet());
        Assertions.assertEquals(Set.of(1, 2), newIdSet);
    }

    @ParameterizedTest
    @ValueSource(ints = {0b00, 0b01, 0b10, 0b11})
    void isExecutableShouldReturnTrue(int flags) {
        var shouldStayNormalized = (flags & 0b10) > 0;
        var shouldConserveAllRecords = (flags & 0b01) > 0;

        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var column1 = new ColumnLeaf(1, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKeyInverse(6, Set.of())));
        var ingestedColumn = new ColumnLeaf(2, name, dataType.withIsNullable(!shouldConserveAllRecords), ColumnContext.getDefault(),
                shouldStayNormalized
                        ? Set.of(new ColumnConstraintForeignKey(4, Set.of()),
                        new ColumnConstraintForeignKeyInverse(4, Set.of()))
                        : Set.of(new ColumnConstraintForeignKeyInverse(4, Set.of()))
        );
        var ingestingColumn = new ColumnLeaf(4, name, dataType.withIsNullable(true), ColumnContext.getDefault(),
                shouldStayNormalized
                        ? Set.of(new ColumnConstraintForeignKeyInverse(2, Set.of()),
                        new ColumnConstraintForeignKey(2, Set.of()))
                        : Set.of(new ColumnConstraintForeignKeyInverse(2, Set.of()))
        );

        var ingestingTable = new Table(10, name, List.of(ingestingColumn), Context.getDefault(), Set.of());
        var ingestedTable = new Table(11, name, List.of(column1, ingestedColumn), Context.getDefault(), Set.of());
        var tableSet = Set.of(ingestingTable, ingestedTable);
        var schema = new Schema(15, name, Context.getDefault(), tableSet);
        var transformation = new TableToColumnNode(shouldStayNormalized, shouldConserveAllRecords);

        // --- Act
        var isExecutable = transformation.isExecutable(schema);

        // --- Assert
        Assertions.assertTrue(isExecutable);
    }
}