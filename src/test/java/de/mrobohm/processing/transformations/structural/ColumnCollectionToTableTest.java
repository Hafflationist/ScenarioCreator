package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
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

class ColumnCollectionToTableTest {

    @Test
    void transform() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var targetTableName = new StringPlusNaked("Tabelle", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var columnLeaf = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(), SSet.of());
        var columnLeafSub1 = columnLeaf.withId(new IdSimple(3));
        var columnLeafSub2 = columnLeaf.withId(new IdSimple(4));
        var columnCollection = new ColumnCollection(
                new IdSimple(5), name, List.of(columnLeafSub1, columnLeafSub2), SSet.of(), false);
        var targetTable = new Table(
                new IdSimple(6), targetTableName, List.of(columnLeaf, columnCollection), Context.getDefault(), SSet.of());
        var idGenerator = StructuralTestingUtils.getIdGenerator(7);
        var transformation = new ColumnCollectionToTable();

        // --- Act
        var newTableSet = transformation.transform(targetTable, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(2, newTableSet.size());
        Assertions.assertFalse(newTableSet.contains(targetTable));
        var newTableList = newTableSet.stream()
                .filter(t -> t.id() instanceof IdPart idp && idp.predecessorId().equals(targetTable.id()))
                .toList();
        var originTable = newTableList.stream()
                .filter(t -> t.name().equals(targetTable.name())).findFirst().get();
        var extractedTable = newTableList.stream()
                .filter(t -> !t.name().equals(targetTable.name())).findFirst().get();
        Assertions.assertNotEquals(targetTable.columnList(), originTable.columnList());
        Assertions.assertEquals(targetTable.context(), originTable.context());
        Assertions.assertEquals(targetTable.tableConstraintSet(), originTable.tableConstraintSet());
        Assertions.assertEquals(targetTable.columnList().size(), originTable.columnList().size());
        Assertions.assertTrue(originTable.columnList().stream()
                .anyMatch(column -> column.id() instanceof IdSimple ids && ids.number() >= 7)); // checks for new column
        Assertions.assertTrue(originTable.columnList().stream()
                .anyMatch(column -> column.containsConstraint(ColumnConstraintForeignKey.class)));
        Assertions.assertTrue(extractedTable.columnList().stream()
                .anyMatch(column -> column.containsConstraint(ColumnConstraintForeignKeyInverse.class)));
        Assertions.assertTrue(extractedTable.columnList().stream()
                .anyMatch(column -> column.containsConstraint(ColumnConstraintPrimaryKey.class)));
        Assertions.assertTrue(extractedTable.columnList().contains(columnLeafSub1));
        Assertions.assertTrue(extractedTable.columnList().contains(columnLeafSub2));
        IntegrityChecker.assertValidSchema(new Schema(new IdSimple(0), name, Context.getDefault(), newTableSet));
    }

    @Test
    void getCandidates() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var columnLeaf = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(), SSet.of());
        var invalidTable = new Table(new IdSimple(2), name, List.of(columnLeaf), Context.getDefault(), SSet.of());
        var columnLeafSub1 = columnLeaf.withId(new IdSimple(3));
        var columnLeafSub2 = columnLeaf.withId(new IdSimple(4));
        var columnCollection = new ColumnCollection(
                new IdSimple(5), name, List.of(columnLeafSub1, columnLeafSub2), SSet.of(), false);
        var validTable = new Table(
                new IdSimple(6), name, List.of(columnLeaf, columnCollection), Context.getDefault(), SSet.of());
        var tableSet = SSet.of(invalidTable, validTable);
        var transformation = new ColumnCollectionToTable();

        // --- Act
        var newTableSet = transformation.getCandidates(tableSet);

        // --- Assert
        Assertions.assertEquals(1, newTableSet.size());
        Assertions.assertTrue(newTableSet.contains(validTable));
    }
}