package de.mrobohm.operations.structural;

import de.mrobohm.data.*;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.integrity.IntegrityChecker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.Set;

class ColumnLeafsToTableTest {

    @Test
    void transform() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var columnLeaf = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(new IdSimple(7))));
        var invalidTable = new Table(new IdSimple(2), name, List.of(columnLeaf), Context.getDefault(), Set.of());
        var columnLeafGroupable1 = columnLeaf.withConstraintSet(Set.of()).withId(new IdSimple(3));
        var columnLeafGroupable2 = columnLeaf.withConstraintSet(Set.of()).withId(new IdSimple(4));
        var targetTable = new Table(new IdSimple(6), name, List.of(columnLeaf, columnLeafGroupable1, columnLeafGroupable2), Context.getDefault(), Set.of());
        var tableSet = Set.of(invalidTable, targetTable);
        var idGenerator = StructuralTestingUtils.getIdGenerator(8);
        var transformation = new ColumnLeafsToTable();

        // --- Act
        var newTableSet = transformation.transform(targetTable, tableSet, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(2, newTableSet.size());
        Assertions.assertFalse(newTableSet.contains(invalidTable));
        Assertions.assertFalse(newTableSet.contains(targetTable));
        var modifiedTable = newTableSet.stream().filter(t -> t.id().equals(targetTable.id())).toList().get(0);
        var newTable = newTableSet.stream().filter(t -> !t.id().equals(targetTable.id())).toList().get(0);
        Assertions.assertEquals(targetTable.id(), modifiedTable.id());
        Assertions.assertEquals(targetTable.name(), modifiedTable.name());
        Assertions.assertNotEquals(targetTable.columnList(), modifiedTable.columnList());
        Assertions.assertEquals(targetTable.context(), modifiedTable.context());
        Assertions.assertEquals(targetTable.tableConstraintSet(), modifiedTable.tableConstraintSet());
        Assertions.assertEquals(targetTable.columnList().size() + 2, modifiedTable.columnList().size() + newTable.columnList().size());
        Assertions.assertTrue(modifiedTable.columnList().stream().anyMatch(
                column -> column.id() instanceof IdSimple ids && ids.number() >= 7)); // checks for new column
        Assertions.assertTrue(modifiedTable.columnList().stream().anyMatch(column -> column.constraintSet().stream()
                .anyMatch(c -> c instanceof ColumnConstraintForeignKey)));
        Assertions.assertTrue(newTable.columnList().stream().anyMatch(column -> column.constraintSet().stream()
                .anyMatch(c -> c instanceof ColumnConstraintForeignKeyInverse)));
        Assertions.assertTrue(newTable.columnList().stream().anyMatch(column -> column.constraintSet().stream()
                .anyMatch(c -> c instanceof ColumnConstraintPrimaryKey)));
        Assertions.assertTrue(newTable.columnList().contains(columnLeafGroupable1)
                || newTable.columnList().contains(columnLeafGroupable2));
        IntegrityChecker.assertValidSchema(new Schema(0, name, Context.getDefault(), newTableSet));
    }

    @Test
    void getCandidates() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var columnLeaf = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(new IdSimple(7))));
        var invalidTable = new Table(new IdSimple(2), name, List.of(columnLeaf), Context.getDefault(), Set.of());
        var columnLeafGroupable1 = columnLeaf.withConstraintSet(Set.of()).withId(new IdSimple(3));
        var columnLeafGroupable2 = columnLeaf.withConstraintSet(Set.of()).withId(new IdSimple(4));
        var validTable = new Table(new IdSimple(6), name, List.of(columnLeaf, columnLeafGroupable1, columnLeafGroupable2), Context.getDefault(), Set.of());
        var tableSet = Set.of(invalidTable, validTable);
        var transformation = new ColumnLeafsToTable();

        // --- Act
        var newTableSet = transformation.getCandidates(tableSet);

        // --- Assert
        Assertions.assertEquals(1, newTableSet.size());
        Assertions.assertTrue(newTableSet.contains(validTable));
    }
}