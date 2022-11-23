package scenarioCreator.generation.processing.transformations.structural;

import junit.framework.AssertionFailedError;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scenarioCreator.data.Context;
import scenarioCreator.data.Language;
import scenarioCreator.data.Schema;
import scenarioCreator.data.column.DataType;
import scenarioCreator.data.column.DataTypeEnum;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKey;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKeyInverse;
import scenarioCreator.data.column.context.ColumnContext;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnCollection;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.column.nesting.ColumnNode;
import scenarioCreator.data.identification.IdSimple;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.data.table.Table;
import scenarioCreator.generation.processing.integrity.IntegrityChecker;
import scenarioCreator.utils.SSet;

import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class TableToColumnNodeTest {

    @Test
    void transformWithoutDeletionOfIngestingColumn() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var neutralColumn1 = new ColumnLeaf(new IdSimple(14), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var neutralColumn2 = new ColumnLeaf(new IdSimple(13), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var column1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var column2 = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(4))));
        final var ingestedColumn = new ColumnLeaf(new IdSimple(2), name,
                dataType.withIsNullable(true), ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(4))));
        final var ingestingColumn = new ColumnLeaf(new IdSimple(4), name,
                dataType.withIsNullable(true), ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(2)),
                        new ColumnConstraintForeignKeyInverse(new IdSimple(3)))
        );

        final var table = StructuralTestingUtils.createTable(
                12, List.of(column2)
        );
        final var ingestingTable = StructuralTestingUtils.createTable(
                10, List.of(ingestingColumn, neutralColumn1)
        );
        final var ingestedTable = StructuralTestingUtils.createTable(
                11, List.of(column1, ingestedColumn, neutralColumn2)
        );
        final var tableSet = SSet.of(ingestingTable, ingestedTable, table);
        final var schema = new Schema(new IdSimple(15), name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        final var transformation = new TableToColumnNode(false, false);

        // --- Act
        final var newSchema = transformation.transform(schema, new Random());

        // --- Assert
        IntegrityChecker.assertValidSchema(newSchema);
        final var oldIdSet = schema.tableSet().stream()
                .flatMap(t -> t.columnList().stream())
                .map(Column::id)
                .collect(Collectors.toCollection(TreeSet::new));
        final var newIdSet = newSchema.tableSet().stream()
                .flatMap(t -> t.columnList().stream())
                .flatMap(column -> switch (column) {
                    case ColumnLeaf leaf -> Stream.of(leaf);
                    case ColumnNode node -> node.columnList().stream();
                    case ColumnCollection ignore -> throw new AssertionFailedError();
                })
                .map(Column::id)
                .collect(Collectors.toCollection(TreeSet::new));
        Assertions.assertEquals(oldIdSet, newIdSet);
    }

    @Test
    void transformWithDeletionOfIngestingColumn() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var neutralColumn1 = new ColumnLeaf(new IdSimple(12), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var neutralColumn2 = new ColumnLeaf(new IdSimple(13), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var column1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var ingestedColumn = new ColumnLeaf(new IdSimple(2), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(4))));
        final var ingestingColumn = new ColumnLeaf(new IdSimple(4), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(2)))
        );

        final var ingestingTable = StructuralTestingUtils.createTable(
                10, List.of(ingestingColumn, neutralColumn1)
        );
        final var ingestedTable = StructuralTestingUtils.createTable(
                11, List.of(column1, ingestedColumn, neutralColumn2)
        );
        final var tableSet = SSet.of(ingestingTable, ingestedTable);
        final var schema = new Schema(new IdSimple(15), name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        final var transformation = new TableToColumnNode(false, false);

        // --- Act
        final var newSchema = transformation.transform(schema, new Random());

        // --- Assert
        IntegrityChecker.assertValidSchema(newSchema);
        final var newIdSet = newSchema.tableSet().stream()
                .flatMap(t -> t.columnList().stream())
                .flatMap(column -> switch (column) {
                    case ColumnLeaf leaf -> Stream.of(leaf);
                    case ColumnNode node -> node.columnList().stream();
                    case ColumnCollection ignore -> throw new AssertionFailedError();
                })
                .map(Column::id)
                .collect(Collectors.toCollection(TreeSet::new));
        Assertions.assertEquals(
                SSet.of(new IdSimple(1), new IdSimple(2), new IdSimple(12), new IdSimple(13)),
                newIdSet
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {0b00, 0b01, 0b10, 0b11})
    void isExecutableShouldReturnTrue(int flags) {
        final var shouldStayNormalized = (flags & 0b10) > 0;
        final var shouldConserveAllRecords = (flags & 0b01) > 0;

        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var column1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(6))));
        final var ingestedColumn = new ColumnLeaf(new IdSimple(2), name,
                dataType.withIsNullable(!shouldConserveAllRecords), ColumnContext.getDefault(),
                shouldStayNormalized
                        ? SSet.of(new ColumnConstraintForeignKey(new IdSimple(4)),
                        new ColumnConstraintForeignKeyInverse(new IdSimple(4)))
                        : SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(4)))
        );
        final var ingestingColumn = new ColumnLeaf(new IdSimple(4), name,
                dataType.withIsNullable(true), ColumnContext.getDefault(),
                shouldStayNormalized
                        ? SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(2)),
                        new ColumnConstraintForeignKey(new IdSimple(2)))
                        : SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(2)))
        );

        final var ingestingTable = new Table(
                new IdSimple(10), name, List.of(ingestingColumn), Context.getDefault(), SSet.of(), SSet.of());
        final var ingestedTable = new Table(
                new IdSimple(11), name, List.of(column1, ingestedColumn),
                Context.getDefault(), SSet.of(), SSet.of());
        final var tableSet = SSet.of(ingestingTable, ingestedTable);
        final var schema = new Schema(new IdSimple(15), name, Context.getDefault(), tableSet);
        final var transformation = new TableToColumnNode(shouldStayNormalized, shouldConserveAllRecords);

        // --- Act
        final var isExecutable = transformation.isExecutable(schema);

        // --- Assert
        Assertions.assertTrue(isExecutable);
    }
}