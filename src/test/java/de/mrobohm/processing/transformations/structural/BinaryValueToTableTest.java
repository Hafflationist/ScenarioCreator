package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.column.context.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.dataset.Value;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.identification.IdMerge;
import de.mrobohm.data.identification.IdPart;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.integrity.IntegrityChecker;
import de.mrobohm.utils.SSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

class BinaryValueToTableTest {

    @Test
    void transform2() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var valueSet = SSet.of(new Value("Männlein"), new Value("Weiblein"));
        final var neutralColumn1 = new ColumnLeaf(new IdSimple(12), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var neutralColumn2 = new ColumnLeaf(new IdSimple(13), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var invalidColumn = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(7))));
        final var validColumn = invalidColumn
                .withConstraintSet(SSet.of())
                .withValueSet(valueSet)
                .withId(new IdSimple(3));
        final var targetTable = StructuralTestingUtils.createTable(
                6, List.of(invalidColumn, validColumn, neutralColumn1, neutralColumn2)
        );
        final var tableSet = SSet.of(targetTable);
        final var schema = new Schema(new IdSimple(0), name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        final var transformation = new BinaryValueToTable();

        // --- Act
        final var newSchema = transformation.transform(schema, new Random());

        // --- Assert
        Assertions.assertEquals(2, newSchema.tableSet().size());
        Assertions.assertFalse(newSchema.tableSet().contains(targetTable));
        final var tableList = newSchema.tableSet().stream().toList();
        final var table1 = tableList.get(0);
        final var table2 = tableList.get(1);
        Assertions.assertFalse(
                table1.columnList().stream()
                        .map(Column::id)
                        .anyMatch(id -> containsId(validColumn.id(), id))
        );
        Assertions.assertFalse(
                table2.columnList().stream()
                        .map(Column::id)
                        .anyMatch(id -> containsId(validColumn.id(), id))
        );
        IntegrityChecker.assertValidSchema(newSchema);
    }

    @Test
    void transform3() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var valueSet = SSet.of(new Value("M"), new Value("W"), new Value("D"));
        final var neutralColumn1 = new ColumnLeaf(new IdSimple(12), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var neutralColumn2 = new ColumnLeaf(new IdSimple(13), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var invalidColumn = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(7))));
        final var validColumn = invalidColumn
                .withConstraintSet(SSet.of())
                .withValueSet(valueSet)
                .withId(new IdSimple(3));
        final var targetTable = StructuralTestingUtils.createTable(
                6, List.of(invalidColumn, validColumn, neutralColumn1, neutralColumn2)
        );
        final var tableSet = SSet.of(targetTable);
        final var schema = new Schema(new IdSimple(0), name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        final var transformation = new BinaryValueToTable();

        // --- Act
        final var newSchema = transformation.transform(schema, new Random());

        // --- Assert
        Assertions.assertEquals(3, newSchema.tableSet().size());
        Assertions.assertFalse(newSchema.tableSet().contains(targetTable));
        final var tableList = newSchema.tableSet().stream().toList();
        final var table1 = tableList.get(0);
        final var table2 = tableList.get(1);
        final var table3 = tableList.get(2);
        Assertions.assertFalse(
                table1.columnList().stream()
                        .map(Column::id)
                        .anyMatch(id -> containsId(validColumn.id(), id))
        );
        Assertions.assertFalse(
                table2.columnList().stream()
                        .map(Column::id)
                        .anyMatch(id -> containsId(validColumn.id(), id))
        );
        Assertions.assertFalse(
                table3.columnList().stream()
                        .map(Column::id)
                        .anyMatch(id -> containsId(validColumn.id(), id))
        );
        IntegrityChecker.assertValidSchema(newSchema);
    }

    @Test
    void transform2WithForeignKeyConstraints() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var valueSet = SSet.of(new Value("Männlein"), new Value("Weiblein"));
        final var invalidColumn = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(7))));
        final var neutralColumn1 = new ColumnLeaf(new IdSimple(12), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var neutralColumn2 = new ColumnLeaf(new IdSimple(13), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var neutralColumn3 = new ColumnLeaf(new IdSimple(14), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var referencedColumn = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(3), SSet.of())));
        final var referencingColumn = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(referencedColumn.id(), SSet.of())));
        final var validColumn = invalidColumn
                .withConstraintSet(SSet.of())
                .withValueSet(valueSet)
                .withId(new IdSimple(4));
        final var referencedTable = StructuralTestingUtils.createTable(
                20, List.of(referencedColumn, neutralColumn1)
        );
        final var targetTable = StructuralTestingUtils.createTable(
                21, List.of(validColumn, referencingColumn, neutralColumn2, neutralColumn3)
        );
        final var tableSet = SSet.of(referencedTable, targetTable);
        final var schema = new Schema(new IdSimple(0), name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        final var transformation = new BinaryValueToTable();

        // --- Act
        final var newSchema = transformation.transform(schema, new Random());

        // --- Assert
        Assertions.assertEquals(3, newSchema.tableSet().size());
        Assertions.assertTrue(newSchema.tableSet().stream().anyMatch(t -> t.id().equals(referencedTable.id())));
        Assertions.assertFalse(newSchema.tableSet().contains(targetTable));
        final var tableList = newSchema.tableSet().stream().toList();
        final var table1 = tableList.get(0);
        final var table2 = tableList.get(1);
        Assertions.assertFalse(
                table1.columnList().stream()
                        .map(Column::id)
                        .anyMatch(id -> containsId(validColumn.id(), id))
        );
        Assertions.assertFalse(
                table2.columnList().stream()
                        .map(Column::id)
                        .anyMatch(id -> containsId(validColumn.id(), id))
        );
        IntegrityChecker.assertValidSchema(newSchema);
    }

    @Test
    void transform3WithForeignKeyConstraints() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var valueSet = SSet.of(new Value("M"), new Value("W"), new Value("D"));
        final var invalidColumn = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(7))));
        final var neutralColumn1 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var neutralColumn2 = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var neutralColumn3 = new ColumnLeaf(new IdSimple(4), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var referencedColumn = new ColumnLeaf(new IdSimple(5), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(6), SSet.of())));
        final var referencingColumn = new ColumnLeaf(new IdSimple(6), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(referencedColumn.id(), SSet.of())));
        final var validColumn = invalidColumn
                .withConstraintSet(SSet.of())
                .withValueSet(valueSet)
                .withId(new IdSimple(7));
        final var referencedTable = StructuralTestingUtils.createTable(
                10, List.of(referencedColumn, neutralColumn1)
        );
        final var targetTable = StructuralTestingUtils.createTable(
                11, List.of(validColumn, referencingColumn, neutralColumn2, neutralColumn3)
        );
        final var tableSet = SSet.of(referencedTable, targetTable);
        final var schema = new Schema(new IdSimple(0), name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        final var transformation = new BinaryValueToTable();

        // --- Act
        final var newSchema = transformation.transform(schema, new Random());

        // --- Assert
        Assertions.assertEquals(4, newSchema.tableSet().size());
        Assertions.assertTrue(newSchema.tableSet().stream().anyMatch(t -> t.id().equals(referencedTable.id())));
        Assertions.assertFalse(newSchema.tableSet().contains(targetTable));
        final var tableList = newSchema.tableSet().stream().toList();
        final var table1 = tableList.get(0);
        final var table2 = tableList.get(1);
        final var table3 = tableList.get(2);
        Assertions.assertFalse(
                table1.columnList().stream()
                        .map(Column::id)
                        .anyMatch(id -> containsId(validColumn.id(), id))
        );
        Assertions.assertFalse(
                table2.columnList().stream()
                        .map(Column::id)
                        .anyMatch(id -> containsId(validColumn.id(), id))
        );
        Assertions.assertFalse(
                table3.columnList().stream()
                        .map(Column::id)
                        .anyMatch(id -> containsId(validColumn.id(), id))
        );
        IntegrityChecker.assertValidSchema(newSchema);
    }

    private boolean containsId(Id rootId, Id newId) {
        return switch (newId) {
            case IdSimple ids -> ids.equals(rootId);
            case IdPart idp -> idp.predecessorId().equals(rootId);
            case IdMerge idm -> idm.predecessorId1().equals(rootId) || idm.predecessorId2().equals(rootId);
        };
    }

    @Test
    void getCandidatesContainsValidTable1() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var valueSet = SSet.of(new Value("Männlein"), new Value("Weiblein"));
        final var primColumn = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(7))));
        final var invalidColumn = new ColumnLeaf(new IdSimple(5), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(2), SSet.of())));
        final var neutralColumn = new ColumnLeaf(new IdSimple(55), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(2), SSet.of())));
        final var validColumn = primColumn
                .withConstraintSet(SSet.of())
                .withValueSet(valueSet)
                .withId(new IdSimple(3));

        final var invalidTable1 = StructuralTestingUtils.createTable(2, List.of(primColumn, invalidColumn));
        final var invalidTable2 = StructuralTestingUtils.createTable(2, List.of(primColumn));
        final var invalidTable3 = StructuralTestingUtils.createTable(4, List.of(validColumn));
        final var invalidTable4 = StructuralTestingUtils.createTable(4, List.of(primColumn, validColumn, invalidColumn));
        final var invalidTable5 = StructuralTestingUtils.createTable(2, List.of(primColumn, invalidColumn, neutralColumn));
        final var validTable1 = StructuralTestingUtils.createTable(6, List.of(primColumn, validColumn));
        final var tableSet = SSet.of(
                invalidTable1, invalidTable2, invalidTable3, invalidTable4, invalidTable5,
                validTable1
        );
        final var schema = new Schema(new IdSimple(0), name, Context.getDefault(), tableSet);
        final var transformation = new BinaryValueToTable();

        // --- Act
        final var isExecutable = transformation.isExecutable(schema);

        // --- Assert
        Assertions.assertTrue(isExecutable);
    }

    @Test
    void getCandidatesContainsValidTable2() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var valueSet = SSet.of(new Value("Männlein"), new Value("Weiblein"));
        final var primColumn = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(7))));
        final var invalidColumn = new ColumnLeaf(new IdSimple(5), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(2), SSet.of())));
        final var neutralColumn = new ColumnLeaf(new IdSimple(55), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(2), SSet.of())));
        final var validColumn = primColumn
                .withConstraintSet(SSet.of())
                .withValueSet(valueSet)
                .withId(new IdSimple(3));

        final var invalidTable1 = new Table(new IdSimple(2), name, List.of(primColumn, invalidColumn),
                Context.getDefault(), SSet.of(), SSet.of());
        final var invalidTable2 = new Table(new IdSimple(2), name, List.of(primColumn),
                Context.getDefault(), SSet.of(), SSet.of());
        final var invalidTable3 = new Table(new IdSimple(4), name, List.of(validColumn),
                Context.getDefault(), SSet.of(), SSet.of());
        final var invalidTable4 = new Table(new IdSimple(4), name, List.of(primColumn, validColumn, invalidColumn),
                Context.getDefault(), SSet.of(), SSet.of());
        final var invalidTable5 = new Table(new IdSimple(2), name, List.of(primColumn, invalidColumn, neutralColumn),
                Context.getDefault(), SSet.of(), SSet.of());
        final var validTable2 = new Table(
                new IdSimple(6),
                name,
                List.of(primColumn, validColumn, neutralColumn),
                Context.getDefault(),
                SSet.of(),
                SSet.of()
        );
        final var tableSet = SSet.of(
                invalidTable1, invalidTable2, invalidTable3, invalidTable4, invalidTable5,
                validTable2
        );
        final var schema = new Schema(new IdSimple(0), name, Context.getDefault(), tableSet);
        final var transformation = new BinaryValueToTable();

        // --- Act
        final var isExecutable = transformation.isExecutable(schema);

        // --- Assert
        Assertions.assertTrue(isExecutable);
    }

    @Test
    void getCandidatesContainsValidTable3() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var valueSet = SSet.of(new Value("M"), new Value("W"), new Value("D"));
        final var primColumn = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(7))));
        final var invalidColumn = new ColumnLeaf(new IdSimple(5), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(2), SSet.of())));
        final var neutralColumn = new ColumnLeaf(new IdSimple(55), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(2), SSet.of())));
        final var validColumn = primColumn
                .withConstraintSet(SSet.of())
                .withValueSet(valueSet)
                .withId(new IdSimple(3));

        final var invalidTable1 = new Table(new IdSimple(2), name, List.of(primColumn, invalidColumn),
                Context.getDefault(), SSet.of(), SSet.of());
        final var invalidTable2 = new Table(new IdSimple(2), name, List.of(primColumn),
                Context.getDefault(), SSet.of(), SSet.of());
        final var invalidTable3 = new Table(new IdSimple(4), name, List.of(validColumn),
                Context.getDefault(), SSet.of(), SSet.of());
        final var invalidTable4 = new Table(new IdSimple(4), name, List.of(primColumn, validColumn, invalidColumn),
                Context.getDefault(), SSet.of(), SSet.of());
        final var invalidTable5 = new Table(new IdSimple(2), name, List.of(primColumn, invalidColumn, neutralColumn),
                Context.getDefault(), SSet.of(), SSet.of());
        final var validTable2 = new Table(
                new IdSimple(6),
                name,
                List.of(primColumn, validColumn, neutralColumn),
                Context.getDefault(),
                SSet.of(),
                SSet.of()
        );
        final var tableSet = SSet.of(
                invalidTable1, invalidTable2, invalidTable3, invalidTable4, invalidTable5,
                validTable2
        );
        final var schema = new Schema(new IdSimple(0), name, Context.getDefault(), tableSet);
        final var transformation = new BinaryValueToTable();

        // --- Act
        final var isExecutable = transformation.isExecutable(schema);

        // --- Assert
        Assertions.assertTrue(isExecutable);
    }

    @Test
    void getCandidatesContainsNoValidTable() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var valueSet = SSet.of(new Value("Männlein"), new Value("Weiblein"));
        final var valueSetInvalid = SSet.of(
                new Value("Männlein"),
                new Value("Weiblein"),
                new Value("Weiblein2"),
                new Value("Weiblein3")
        );
        final var primColumn = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(7))));
        final var invalidColumn = new ColumnLeaf(new IdSimple(5), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(2), SSet.of())));
        final var neutralColumn = new ColumnLeaf(new IdSimple(55), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(2), valueSetInvalid)));
        final var validColumn = primColumn
                .withConstraintSet(SSet.of())
                .withValueSet(valueSet)
                .withId(new IdSimple(3));

        final var invalidTable1 = new Table(new IdSimple(2), name, List.of(primColumn, invalidColumn),
                Context.getDefault(), SSet.of(), SSet.of());
        final var invalidTable2 = new Table(new IdSimple(2), name,
                List.of(primColumn), Context.getDefault(), SSet.of(), SSet.of());
        final var invalidTable3 = new Table(new IdSimple(4), name,
                List.of(validColumn), Context.getDefault(), SSet.of(), SSet.of());
        final var invalidTable4 = new Table(new IdSimple(4), name, List.of(primColumn, validColumn, invalidColumn),
                Context.getDefault(), SSet.of(), SSet.of());
        final var invalidTable5 = new Table(new IdSimple(2), name, List.of(primColumn, invalidColumn, neutralColumn),
                Context.getDefault(), SSet.of(), SSet.of());
        final var tableSet = SSet.of(
                invalidTable1, invalidTable2, invalidTable3, invalidTable4, invalidTable5
        );
        final var schema = new Schema(new IdSimple(0), name, Context.getDefault(), tableSet);
        final var transformation = new BinaryValueToTable();

        // --- Act
        final var isExecutable = transformation.isExecutable(schema);

        // --- Assert
        Assertions.assertFalse(isExecutable);
    }
}