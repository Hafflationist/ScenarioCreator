package de.mrobohm.operations.structural;

import de.mrobohm.data.*;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.integrity.IntegrityChecker;
import de.mrobohm.utils.StreamExtensions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class MergeColumnsTest {

    Table getTable(int id, Set<Table> tableSet) {
        var tableOpt = tableSet.stream().filter(t -> t.id() == id).findFirst();
        assert tableOpt.isPresent();
        return tableOpt.get();
    }

    @Test
    void transformTrue() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var validDataType = new DataType(DataTypeEnum.NVARCHAR, false);
        var semivalidColumn1 = new ColumnLeaf(1, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(3, Set.of())));
        var semivalidColumn2 = new ColumnLeaf(2, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(3, Set.of())));
        var semivalidColumn3 = new ColumnLeaf(3, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKeyInverse(1, Set.of()),
                        new ColumnConstraintForeignKeyInverse(2, Set.of())));
        var invalidColumn1 = new ColumnLeaf(4, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(20)));
        var invalidColumn2 = new ColumnLeaf(5, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(21)));
        var invalidColumn3 = new ColumnLeaf(6, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(22)));
        var validColumn1 = new ColumnLeaf(7, name, validDataType, ColumnContext.getDefault(), Set.of());
        var validColumn2 = validColumn1.withId(8);

        var invalidTable = new Table(10, name,
                List.of(invalidColumn1, semivalidColumn1), Context.getDefault(), Set.of());
        var semivalidTable = new Table(11, name,
                List.of(invalidColumn2, semivalidColumn2, semivalidColumn3), Context.getDefault(), Set.of());
        var validTable = new Table(14, name,
                List.of(invalidColumn3, validColumn1, validColumn2), Context.getDefault(), Set.of());
        var tableSet = Set.of(invalidTable, validTable, semivalidTable);
        var schema = new Schema(0, name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        var transformation = new MergeColumns(true);

        // --- Act
        var newSchema = transformation.transform(schema, new Random());

        // --- Assert
        var newTableSet =newSchema.tableSet();
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
        Assertions.assertEquals(validTable.context(), newValidTable.context());
        Assertions.assertEquals(validTable.tableConstraintSet(), newValidTable.tableConstraintSet());
        IntegrityChecker.assertValidSchema(newSchema);
    }

    @Test
    void transformFalse() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var semivalidColumn1 = new ColumnLeaf(1, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(3, Set.of())));
        var semivalidColumn2 = new ColumnLeaf(2, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(3, Set.of())));
        var semivalidColumn3 = new ColumnLeaf(3, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKeyInverse(1, Set.of()),
                        new ColumnConstraintForeignKeyInverse(2, Set.of())));
        var invalidColumn1 = new ColumnLeaf(4, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(20)));
        var invalidColumn2 = new ColumnLeaf(5, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(21)));

        var invalidTable = new Table(10, name,
                List.of(invalidColumn1, semivalidColumn1), Context.getDefault(), Set.of());
        var semivalidTable = new Table(11, name,
                List.of(invalidColumn2, semivalidColumn2, semivalidColumn3), Context.getDefault(), Set.of());
        var tableSet = Set.of(invalidTable, semivalidTable);
        var schema = new Schema(0, name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        var transformation = new MergeColumns(false);

        // --- Act
        var newSchema = transformation.transform(schema, new Random());

        // --- Assert
        var newTableSet =newSchema.tableSet();
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
        var semivalidColumn1 = new ColumnLeaf(1, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKeyInverse(6, Set.of())));
        var semivalidColumn2 = new ColumnLeaf(2, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(7, Set.of())));
        var invalidColumn = new ColumnLeaf(3, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(8)));
        var validColumn1 = new ColumnLeaf(4, name, dataType, ColumnContext.getDefault(), Set.of());
        var validColumn2 = validColumn1.withId(5);

        var invalidTable = new Table(10, name,
                List.of(invalidColumn, semivalidColumn1), Context.getDefault(), Set.of());
        var semivalidTable1 = new Table(11, name,
                List.of(invalidColumn, semivalidColumn1, semivalidColumn2), Context.getDefault(), Set.of());
        var semivalidTable2 = new Table(12, name,
                List.of(invalidColumn, semivalidColumn1, validColumn1), Context.getDefault(), Set.of());
        var semivalidTable3 = new Table(13, name,
                List.of(invalidColumn, validColumn1, semivalidColumn2), Context.getDefault(), Set.of());
        var validTable = new Table(14, name,
                List.of(invalidColumn, validColumn1, validColumn2), Context.getDefault(), Set.of());
        var tableSet = Set.of(invalidTable, semivalidTable1, semivalidTable2, semivalidTable3, validTable);
        var schema = new Schema(15, name, Context.getDefault(), tableSet);
        var transformation = new MergeColumns(keepForeignKeyIntegrity);

        // --- Act
        var isExecutable = transformation.isExecutable(schema);

        // --- Assert
        Assertions.assertTrue(isExecutable);
    }
}