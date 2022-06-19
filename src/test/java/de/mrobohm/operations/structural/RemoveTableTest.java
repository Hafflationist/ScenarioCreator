package de.mrobohm.operations.structural;

import de.mrobohm.data.Context;
import de.mrobohm.data.DataType;
import de.mrobohm.data.DataTypeEnum;
import de.mrobohm.data.Language;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.Set;

class RemoveTableTest {

    @Test
    void transform() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var invalidColumn1 = new ColumnLeaf(2, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKeyInverse(8, Set.of())));
        var invalidColumn2 = new ColumnLeaf(3, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(9, Set.of())));
        var validColumn1 = new ColumnLeaf(6, name, dataType, ColumnContext.getDefault(), Set.of());
        var validColumn2 = new ColumnLeaf(1, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(7)));

        var invalidTable1 = new Table(10, name, List.of(invalidColumn1), Context.getDefault(), Set.of());
        var invalidTable2 = invalidTable1.withId(11).withColumnList(List.of(invalidColumn2));
        var validTable = invalidTable1.withId(12).withColumnList(List.of(validColumn1));
        var targetTable = invalidTable1.withId(13).withColumnList(List.of(validColumn2));
        var tableSet = Set.of(invalidTable1, invalidTable2, validTable, targetTable);
        var idGenerator = StructuralTestingUtils.getIdGenerator(8);
        var transformation = new RemoveTable();

        // --- Act
        var candidates = transformation.transform(validTable, tableSet, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(0, candidates.size());
    }

    @Test
    void getCandidates() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var invalidColumn1 = new ColumnLeaf(2, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKeyInverse(8, Set.of())));
        var invalidColumn2 = new ColumnLeaf(3, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(9, Set.of())));
        var validColumn1 = new ColumnLeaf(6, name, dataType, ColumnContext.getDefault(), Set.of());
        var validColumn2 = new ColumnLeaf(1, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(7)));

        var invalidTable1 = new Table(10, name, List.of(invalidColumn1), Context.getDefault(), Set.of());
        var invalidTable2 = invalidTable1.withId(11).withColumnList(List.of(invalidColumn2));
        var validTable1 = invalidTable1.withId(12).withColumnList(List.of(validColumn1));
        var validTable2 = invalidTable1.withId(13).withColumnList(List.of(validColumn2));
        var tableSet = Set.of(invalidTable1, invalidTable2, validTable1, validTable2);
        var transformation = new RemoveTable();

        // --- Act
        var candidates = transformation.getCandidates(tableSet);

        // --- Assert
        Assertions.assertEquals(2, candidates.size());
        Assertions.assertFalse(candidates.contains(invalidTable1));
        Assertions.assertFalse(candidates.contains(invalidTable2));
        Assertions.assertTrue(candidates.contains(validTable1));
        Assertions.assertTrue(candidates.contains(validTable2));
    }
}