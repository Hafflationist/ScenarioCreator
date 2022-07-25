package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.integrity.IntegrityChecker;
import de.mrobohm.utils.SSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.stream.Collectors;

class TableToColumnLeafsTest {

    @Test
    void transformWithoutDeletionOfIngestingColumn() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var column1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column2 = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(4), SSet.of())));
        var ingestedColumn = new ColumnLeaf(new IdSimple(2), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(4), SSet.of())));
        var ingestingColumn = new ColumnLeaf(new IdSimple(4), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(2), SSet.of()),
                        new ColumnConstraintForeignKeyInverse(new IdSimple(3), SSet.of()))
        );

        var table = new Table(new IdSimple(12), name, List.of(column2), Context.getDefault(), SSet.of());
        var ingestingTable = new Table(new IdSimple(10), name, List.of(ingestingColumn), Context.getDefault(), SSet.of());
        var ingestedTable = new Table(new IdSimple(11), name, List.of(column1, ingestedColumn), Context.getDefault(), SSet.of());
        var tableSet = SSet.of(ingestingTable, ingestedTable, table);
        var schema = new Schema(new IdSimple(15), name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        var transformation = new TableToColumnLeafs(false, false);

        // --- Act
        var newSchema = transformation.transform(schema, new Random());

        // --- Assert
        IntegrityChecker.assertValidSchema(newSchema);
        var oldIdSet = schema.tableSet().stream()
                .flatMap(t -> t.columnList().stream())
                .map(Column::id)
                .collect(Collectors.toCollection(TreeSet::new));
        var newIdSet = newSchema.tableSet().stream()
                .flatMap(t -> t.columnList().stream())
                .map(Column::id)
                .collect(Collectors.toCollection(TreeSet::new));
        Assertions.assertEquals(oldIdSet, newIdSet);
    }

    @Test
    void transformWithDeletionOfIngestingColumn() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var column1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(), SSet.of());
        var ingestedColumn = new ColumnLeaf(new IdSimple(2), name, dataType.withIsNullable(true), ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(4), SSet.of())));
        var ingestingColumn = new ColumnLeaf(new IdSimple(4), name, dataType.withIsNullable(true), ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(2), SSet.of()))
        );

        var ingestingTable = new Table(new IdSimple(10), name, List.of(ingestingColumn), Context.getDefault(), SSet.of());
        var ingestedTable = new Table(new IdSimple(11), name, List.of(column1, ingestedColumn), Context.getDefault(), SSet.of());
        var tableSet = SSet.of(ingestingTable, ingestedTable);
        var schema = new Schema(new IdSimple(15), name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        var transformation = new TableToColumnLeafs(false, false);

        // --- Act
        var newSchema = transformation.transform(schema, new Random());

        // --- Assert
        IntegrityChecker.assertValidSchema(newSchema);
        var newIdSet = newSchema.tableSet().stream()
                .flatMap(t -> t.columnList().stream())
                .map(Column::id)
                .collect(Collectors.toCollection(TreeSet::new));
        Assertions.assertEquals(SSet.of(new IdSimple(1), new IdSimple(2)), newIdSet);
    }

    @ParameterizedTest
    @ValueSource(ints = {0b00, 0b01, 0b10, 0b11})
    void isExecutableShouldReturnFalse(int flags) {
        var shouldStayNormalized = (flags & 0b10) > 0;
        var shouldConserveAllRecords = (flags & 0b01) > 0;

        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var column1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(6), SSet.of())));
        var column2 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(7), SSet.of())));
        var column3 = new ColumnLeaf(new IdSimple(4), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(), SSet.of());

        var table1 = new Table(new IdSimple(10), name, List.of(column1), Context.getDefault(), SSet.of());
        var table2 = new Table(new IdSimple(11), name, List.of(column1, column2), Context.getDefault(), SSet.of());
        var table3 = new Table(new IdSimple(14), name,
                List.of(column1, column2, column3), Context.getDefault(), SSet.of());
        var tableSet = SSet.of(table1, table2, table3);
        var schema = new Schema(new IdSimple(15), name, Context.getDefault(), tableSet);
        var transformation = new TableToColumnLeafs(shouldStayNormalized, shouldConserveAllRecords);

        // --- Act
        var isExecutable = transformation.isExecutable(schema);

        // --- Assert
        Assertions.assertFalse(isExecutable);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void isExecutableShouldReturnFalse2(boolean shouldConserveAllRecords) {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var column1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(6), SSet.of())));
        var ingestedColumn = new ColumnLeaf(new IdSimple(2), name, dataType.withIsNullable(!shouldConserveAllRecords), ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(4), SSet.of()))
        );
        var ingestingColumn = new ColumnLeaf(new IdSimple(4), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(2), SSet.of()))
        );

        var ingestingTable = new Table(new IdSimple(10), name, List.of(ingestingColumn),
                Context.getDefault(), SSet.of());
        var ingestedTable = new Table(new IdSimple(11), name, List.of(column1, ingestedColumn),
                Context.getDefault(), SSet.of());
        var tableSet = SSet.of(ingestingTable, ingestedTable);
        var schema = new Schema(new IdSimple(15), name, Context.getDefault(), tableSet);
        var transformation = new TableToColumnLeafs(true, shouldConserveAllRecords);

        // --- Act
        var isExecutable = transformation.isExecutable(schema);

        // --- Assert
        Assertions.assertFalse(isExecutable);
    }

    @ParameterizedTest
    @ValueSource(ints = {0b00, 0b01, 0b10, 0b11})
    void isExecutableShouldReturnTrue(int flags) {
        var shouldStayNormalized = (flags & 0b10) > 0;
        var shouldConserveAllRecords = (flags & 0b01) > 0;

        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var column1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(6), SSet.of())));
        var ingestedColumn = new ColumnLeaf(new IdSimple(2), name, dataType.withIsNullable(!shouldConserveAllRecords), ColumnContext.getDefault(),
                shouldStayNormalized
                        ? SSet.of(new ColumnConstraintForeignKey(new IdSimple(4), SSet.of()),
                        new ColumnConstraintForeignKeyInverse(new IdSimple(4), SSet.of()))
                        : SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(4), SSet.of()))
        );
        var ingestingColumn = new ColumnLeaf(new IdSimple(4), name, dataType.withIsNullable(true), ColumnContext.getDefault(),
                shouldStayNormalized
                        ? SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(2), SSet.of()),
                        new ColumnConstraintForeignKey(new IdSimple(2), SSet.of()))
                        : SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(2), SSet.of()))
        );

        var ingestingTable = new Table(new IdSimple(10), name, List.of(ingestingColumn),
                Context.getDefault(), SSet.of());
        var ingestedTable = new Table(new IdSimple(11), name, List.of(column1, ingestedColumn),
                Context.getDefault(), SSet.of());
        var tableSet = SSet.of(ingestingTable, ingestedTable);
        var schema = new Schema(new IdSimple(15), name, Context.getDefault(), tableSet);
        var transformation = new TableToColumnLeafs(shouldStayNormalized, shouldConserveAllRecords);

        // --- Act
        var isExecutable = transformation.isExecutable(schema);

        // --- Assert
        Assertions.assertTrue(isExecutable);
    }
}