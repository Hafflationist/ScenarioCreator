package de.mrobohm.operations.structural;

import de.mrobohm.data.*;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.integrity.IntegrityChecker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.Set;

class ColumnNodeToTableTest {

    @Test
    void transform() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var columnLeaf = new ColumnLeaf(1, name, dataType, ColumnContext.getDefault(), Set.of());
        var invalidTable = new Table(2, name, List.of(columnLeaf), Context.getDefault(), Set.of());
        var columnLeafSub1 = columnLeaf.withId(3);
        var columnLeafSub2 = columnLeaf.withId(4);
        var columnNode = new ColumnNode(5, name, List.of(columnLeafSub1, columnLeafSub2), Set.of(), false);
        var targetTable = new Table(6, name, List.of(columnLeaf, columnNode), Context.getDefault(), Set.of());
        var tableSet = Set.of(invalidTable, targetTable);
        var idGenerator = StructuralTestingUtils.getIdGenerator(7);
        var transformation = new ColumnNodeToTable();

        // --- Act
        var newTableSet = transformation.transform(targetTable, tableSet, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(2, newTableSet.size());
        Assertions.assertFalse(newTableSet.contains(invalidTable));
        Assertions.assertFalse(newTableSet.contains(targetTable));
        var modifiedTable = newTableSet.stream().filter(t -> t.id() == targetTable.id()).toList().get(0);
        var newTable = newTableSet.stream().filter(t -> t.id() != targetTable.id()).toList().get(0);
        Assertions.assertEquals(targetTable.id(), modifiedTable.id());
        Assertions.assertEquals(targetTable.name(), modifiedTable.name());
        Assertions.assertNotEquals(targetTable.columnList(), modifiedTable.columnList());
        Assertions.assertEquals(targetTable.context(), modifiedTable.context());
        Assertions.assertEquals(targetTable.tableConstraintSet(), modifiedTable.tableConstraintSet());
        Assertions.assertEquals(targetTable.columnList().size(), modifiedTable.columnList().size());
        Assertions.assertTrue(modifiedTable.columnList().stream().anyMatch(column -> column.id() >= 7)); // checks for new column
        Assertions.assertTrue(modifiedTable.columnList().stream().anyMatch(column -> column.constraintSet().stream()
                .anyMatch(c -> c instanceof ColumnConstraintForeignKey)));
        Assertions.assertTrue(newTable.columnList().stream().anyMatch(column -> column.constraintSet().stream()
                .anyMatch(c -> c instanceof ColumnConstraintForeignKeyInverse)));
        Assertions.assertTrue(newTable.columnList().stream().anyMatch(column -> column.constraintSet().stream()
                .anyMatch(c -> c instanceof ColumnConstraintPrimaryKey)));
        Assertions.assertTrue(newTable.columnList().contains(columnLeafSub1));
        Assertions.assertTrue(newTable.columnList().contains(columnLeafSub2));
        IntegrityChecker.assertValidSchema(new Schema(0, name, Context.getDefault(), newTableSet));
    }

    @Test
    void getCandidates() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var columnLeaf = new ColumnLeaf(1, name, dataType, ColumnContext.getDefault(), Set.of());
        var invalidTable = new Table(2, name, List.of(columnLeaf), Context.getDefault(), Set.of());
        var columnLeafSub1 = columnLeaf.withId(3);
        var columnLeafSub2 = columnLeaf.withId(4);
        var columnNode = new ColumnNode(5, name, List.of(columnLeafSub1, columnLeafSub2), Set.of(), false);
        var validTable = new Table(6, name, List.of(columnLeaf, columnNode), Context.getDefault(), Set.of());
        var tableSet = Set.of(invalidTable, validTable);
        var transformation = new ColumnNodeToTable();

        // --- Act
        var newTableSet = transformation.getCandidates(tableSet);

        // --- Assert
        Assertions.assertEquals(1, newTableSet.size());
        Assertions.assertTrue(newTableSet.contains(validTable));
    }
}