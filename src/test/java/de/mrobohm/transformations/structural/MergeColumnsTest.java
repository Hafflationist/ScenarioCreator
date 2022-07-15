package de.mrobohm.transformations.structural;

import de.mrobohm.data.*;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.identification.IdMerge;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.integrity.IntegrityChecker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Random;
import java.util.Set;

class MergeColumnsTest {

    Table getTable(Id id, Set<Table> tableSet) {
        var tableOpt = tableSet.stream().filter(t -> t.id().equals(id)).findFirst();
        assert tableOpt.isPresent();
        return tableOpt.get();
    }

    @Test
    void transformTrue() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var validDataType = new DataType(DataTypeEnum.NVARCHAR, false);
        var semivalidColumn1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(new IdSimple(3), Set.of())));
        var semivalidColumn2 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(new IdSimple(3), Set.of())));
        var semivalidColumn3 = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKeyInverse(new IdSimple(1), Set.of()),
                        new ColumnConstraintForeignKeyInverse(new IdSimple(2), Set.of())));
        var invalidColumn1 = new ColumnLeaf(new IdSimple(4), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(new IdSimple(20))));
        var invalidColumn2 = new ColumnLeaf(new IdSimple(5), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(new IdSimple(21))));
        var invalidColumn3 = new ColumnLeaf(new IdSimple(6), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(new IdSimple(22))));
        var validColumn1 = new ColumnLeaf(new IdSimple(7), name, validDataType,
                ColumnContext.getDefault(), Set.of());
        var validColumn2 = validColumn1.withId(new IdSimple(8));

        var invalidTable = new Table(new IdSimple(10), name,
                List.of(invalidColumn1, semivalidColumn1), Context.getDefault(), Set.of());
        var semivalidTable = new Table(new IdSimple(11), name,
                List.of(invalidColumn2, semivalidColumn2, semivalidColumn3), Context.getDefault(), Set.of());
        var validTable = new Table(new IdSimple(14), name,
                List.of(invalidColumn3, validColumn1, validColumn2), Context.getDefault(), Set.of());
        var tableSet = Set.of(invalidTable, validTable, semivalidTable);
        var schema = new Schema(new IdSimple(0), name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        var transformation = new MergeColumns(true);

        // --- Act
        var newSchema = transformation.transform(schema, new Random());

        // --- Assert
        var newTableSet = newSchema.tableSet();
        Assertions.assertEquals(3, newTableSet.size());
        var newInvalidTable = getTable(invalidTable.id(), newTableSet);
        Assertions.assertEquals(invalidTable, newInvalidTable);
        var newSemivalidTable = getTable(semivalidTable.id(), newTableSet);
        Assertions.assertEquals(semivalidTable, newSemivalidTable);
        var newValidTable = getTable(validTable.id(), newTableSet);
        Assertions.assertNotEquals(validTable, newValidTable);
        Assertions.assertEquals(validTable.name(), newValidTable.name());
        Assertions.assertNotEquals(validTable.columnList(), newValidTable.columnList());
        Assertions.assertEquals(validTable.columnList().size() - 1, newValidTable.columnList().size());
        Assertions.assertTrue(newValidTable.columnList().stream().anyMatch(column -> column.id() instanceof IdMerge));
        Assertions.assertEquals(validTable.context(), newValidTable.context());
        Assertions.assertEquals(validTable.tableConstraintSet(), newValidTable.tableConstraintSet());
        IntegrityChecker.assertValidSchema(newSchema);
    }

    @Test
    void transformFalse() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var semivalidColumn1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(new IdSimple(3), Set.of())));
        var semivalidColumn2 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(new IdSimple(3), Set.of())));
        var semivalidColumn3 = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKeyInverse(new IdSimple(1), Set.of()),
                        new ColumnConstraintForeignKeyInverse(new IdSimple(2), Set.of())));
        var invalidColumn1 = new ColumnLeaf(new IdSimple(4), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(new IdSimple(20))));
        var invalidColumn2 = new ColumnLeaf(new IdSimple(5), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(new IdSimple(21))));

        var invalidTable = new Table(new IdSimple(10), name,
                List.of(invalidColumn1, semivalidColumn1), Context.getDefault(), Set.of());
        var semivalidTable = new Table(new IdSimple(11), name,
                List.of(invalidColumn2, semivalidColumn2, semivalidColumn3), Context.getDefault(), Set.of());
        var tableSet = Set.of(invalidTable, semivalidTable);
        var schema = new Schema(new IdSimple(0), name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        var transformation = new MergeColumns(false);

        // --- Act
        var newSchema = transformation.transform(schema, new Random());

        // --- Assert
        var newTableSet = newSchema.tableSet();
        Assertions.assertEquals(2, newTableSet.size());
        var newInvalidTable = getTable(invalidTable.id(), newTableSet);
        Assertions.assertEquals(invalidTable.name(), newInvalidTable.name());
        Assertions.assertEquals(invalidTable.context(), newInvalidTable.context());
        Assertions.assertEquals(invalidTable.tableConstraintSet(), newInvalidTable.tableConstraintSet());
        var newSemivalidTable = getTable(semivalidTable.id(), newTableSet);
        Assertions.assertNotEquals(semivalidTable, newSemivalidTable);
        Assertions.assertEquals(semivalidTable.name(), newSemivalidTable.name());
        Assertions.assertNotEquals(semivalidTable.columnList(), newSemivalidTable.columnList());
        Assertions.assertEquals(semivalidTable.columnList().size() - 1, newSemivalidTable.columnList().size());
        Assertions.assertEquals(semivalidTable.context(), newSemivalidTable.context());
        Assertions.assertEquals(semivalidTable.tableConstraintSet(), newSemivalidTable.tableConstraintSet());
        IntegrityChecker.assertValidSchema(newSchema);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void getCandidates(boolean keepForeignKeyIntegrity) {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var semivalidColumn1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKeyInverse(new IdSimple(6), Set.of())));
        var semivalidColumn2 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(new IdSimple(7), Set.of())));
        var invalidColumn = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(new IdSimple(8))));
        var validColumn1 = new ColumnLeaf(new IdSimple(4), name, dataType, ColumnContext.getDefault(), Set.of());
        var validColumn2 = validColumn1.withId(new IdSimple(5));

        var invalidTable = new Table(new IdSimple(10), name,
                List.of(invalidColumn, semivalidColumn1), Context.getDefault(), Set.of());
        var semivalidTable1 = new Table(new IdSimple(11), name,
                List.of(invalidColumn, semivalidColumn1, semivalidColumn2), Context.getDefault(), Set.of());
        var semivalidTable2 = new Table(new IdSimple(12), name,
                List.of(invalidColumn, semivalidColumn1, validColumn1), Context.getDefault(), Set.of());
        var semivalidTable3 = new Table(new IdSimple(13), name,
                List.of(invalidColumn, validColumn1, semivalidColumn2), Context.getDefault(), Set.of());
        var validTable = new Table(new IdSimple(14), name,
                List.of(invalidColumn, validColumn1, validColumn2), Context.getDefault(), Set.of());
        var tableSet = Set.of(invalidTable, semivalidTable1, semivalidTable2, semivalidTable3, validTable);
        var schema = new Schema(new IdSimple(15), name, Context.getDefault(), tableSet);
        var transformation = new MergeColumns(keepForeignKeyIntegrity);

        // --- Act
        var isExecutable = transformation.isExecutable(schema);

        // --- Assert
        Assertions.assertTrue(isExecutable);
    }
}