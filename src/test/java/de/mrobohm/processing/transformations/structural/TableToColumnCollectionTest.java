package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.column.context.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.integrity.IntegrityChecker;
import de.mrobohm.utils.SSet;
import junit.framework.AssertionFailedError;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class TableToColumnCollectionTest {

    @Test
    void transformWithoutDeletionOfIngestingColumn() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var neutralColumn1 = new ColumnLeaf(new IdSimple(14), name, dataType, ColumnContext.getDefault(), SSet.of());
        var neutralColumn2 = new ColumnLeaf(new IdSimple(13), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column2 = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(4), SSet.of())));
        var ingestedColumn = new ColumnLeaf(new IdSimple(2), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(), SSet.of(new ColumnConstraintForeignKey(new IdSimple(4), SSet.of())));
        var ingestingColumn = new ColumnLeaf(new IdSimple(4), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(2), SSet.of()),
                        new ColumnConstraintForeignKeyInverse(new IdSimple(3), SSet.of()))
        );

        var table = StructuralTestingUtils.createTable(
                12, List.of(column2)
        );
        var ingestingTable = StructuralTestingUtils.createTable(
                10, List.of(ingestingColumn, neutralColumn1)
        );
        var ingestedTable = StructuralTestingUtils.createTable(
                11, List.of(column1, ingestedColumn, neutralColumn2)
        );
        var tableSet = SSet.of(ingestingTable, ingestedTable, table);
        var schema = new Schema(new IdSimple(15), name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        var transformation = new TableToColumnCollection(false);

        // --- Act
        var newSchema = transformation.transform(schema, new Random(1));

        // --- Assert
        IntegrityChecker.assertValidSchema(newSchema);
        var oldIdSet = schema.tableSet().stream()
                .flatMap(t -> t.columnList().stream())
                .map(Column::id)
                .collect(Collectors.toCollection(TreeSet::new));
        var newIdSet = newSchema.tableSet().stream()
                .flatMap(t -> t.columnList().stream())
                .flatMap(column -> switch (column) {
                    case ColumnLeaf leaf -> Stream.of(leaf);
                    case ColumnNode ignore -> throw new AssertionFailedError();
                    case ColumnCollection col -> col.columnList().stream();
                })
                .map(Column::id)
                .collect(Collectors.toCollection(TreeSet::new));
        Assertions.assertEquals(oldIdSet, newIdSet);
        Assertions.assertTrue(newSchema.tableSet().stream()
                .anyMatch(t ->
                        t.functionalDependencySet().size()
                                == ingestedTable.functionalDependencySet().size()
                                + ingestingTable.functionalDependencySet().size()
                )
        );
    }

    @Test
    void transformWithDeletionOfIngestingColumn() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var neutralColumn1 = new ColumnLeaf(new IdSimple(12), name, dataType, ColumnContext.getDefault(), SSet.of());
        var neutralColumn2 = new ColumnLeaf(new IdSimple(13), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(), SSet.of());
        var ingestedColumn = new ColumnLeaf(new IdSimple(2), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(), SSet.of(new ColumnConstraintForeignKey(new IdSimple(4), SSet.of())));
        var ingestingColumn = new ColumnLeaf(new IdSimple(4), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(), SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(2), SSet.of()))
        );

        var ingestingTable = StructuralTestingUtils.createTable(
                10, List.of(ingestingColumn, neutralColumn1)
        );
        var ingestedTable = StructuralTestingUtils.createTable(
                11, List.of(column1, ingestedColumn, neutralColumn2)
        );
        var tableSet = SSet.of(ingestingTable, ingestedTable);
        var schema = new Schema(new IdSimple(15), name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        var transformation = new TableToColumnCollection(false);

        // --- Act
        var newSchema = transformation.transform(schema, new Random());

        // --- Assert
        IntegrityChecker.assertValidSchema(newSchema);
        var newIdSet = newSchema.tableSet().stream()
                .flatMap(t -> t.columnList().stream())
                .flatMap(column -> switch (column) {
                    case ColumnLeaf leaf -> Stream.of(leaf);
                    case ColumnNode ignore -> throw new AssertionFailedError();
                    case ColumnCollection col -> col.columnList().stream();
                })
                .map(Column::id)
                .collect(Collectors.toCollection(TreeSet::new));
        Assertions.assertEquals(
                SSet.of(new IdSimple(1), new IdSimple(2), new IdSimple(13), new IdSimple(12)),
                newIdSet
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void isExecutableShouldReturnFalse(boolean shouldConserveAllRecords) {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var column1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(6), SSet.of())));
        var column2 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(7), SSet.of())));
        var column3 = new ColumnLeaf(
                new IdSimple(4), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(), SSet.of());

        var table1 = new Table(new IdSimple(10), name, List.of(column1),
                Context.getDefault(), SSet.of(), SSet.of());
        var table2 = new Table(new IdSimple(10), name, List.of(column1, column2),
                Context.getDefault(), SSet.of(), SSet.of());
        var table3 = new Table(new IdSimple(14), name,
                List.of(column1, column2, column3),
                Context.getDefault(), SSet.of(), SSet.of());
        var tableSet = SSet.of(table1, table2, table3);
        var schema = new Schema(new IdSimple(15), name, Context.getDefault(), tableSet);
        var transformation = new TableToColumnCollection(shouldConserveAllRecords);

        // --- Act
        var isExecutable = transformation.isExecutable(schema);

        // --- Assert
        Assertions.assertFalse(isExecutable);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void isExecutableShouldReturnTrue(boolean shouldConserveAllRecords) {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var column1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(6), SSet.of())));
        var ingestedColumn = new ColumnLeaf(new IdSimple(2), name,
                dataType.withIsNullable(!shouldConserveAllRecords), ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(4), SSet.of())));
        var ingestingColumn = new ColumnLeaf(new IdSimple(4), name,
                dataType.withIsNullable(true), ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(2), SSet.of()))
        );

        var ingestingTable = new Table(new IdSimple(10), name, List.of(ingestingColumn),
                Context.getDefault(), SSet.of(), SSet.of());
        var ingestedTable = new Table(new IdSimple(11), name, List.of(column1, ingestedColumn),
                Context.getDefault(), SSet.of(), SSet.of());
        var tableSet = SSet.of(ingestingTable, ingestedTable);
        var schema = new Schema(new IdSimple(15), name, Context.getDefault(), tableSet);
        var transformation = new TableToColumnCollection(shouldConserveAllRecords);

        // --- Act
        var isExecutable = transformation.isExecutable(schema);

        // --- Assert
        Assertions.assertTrue(isExecutable);
    }
}