package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.Context;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.Language;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.IdSimple;
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
        var invalidColumn1 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKeyInverse(new IdSimple(8), Set.of())));
        var validColumn1 = new ColumnLeaf(new IdSimple(6), name, dataType, ColumnContext.getDefault(), Set.of());

        var invalidTable1 = new Table(new IdSimple(10), name, List.of(invalidColumn1), Context.getDefault(), Set.of());
        var validTable = invalidTable1.withId(new IdSimple(12)).withColumnList(List.of(validColumn1));
        var idGenerator = StructuralTestingUtils.getIdGenerator(8);
        var transformation = new RemoveTable();

        // --- Act
        var candidates = transformation.transform(validTable, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(0, candidates.size());
    }

    @Test
    void getCandidates() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var invalidColumn1 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKeyInverse(new IdSimple(8), Set.of())));
        var invalidColumn2 = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(new IdSimple(9), Set.of())));
        var validColumn1 = new ColumnLeaf(new IdSimple(6), name, dataType, ColumnContext.getDefault(), Set.of());
        var validColumn2 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(new IdSimple(7))));

        var invalidTable1 = new Table(new IdSimple(10), name, List.of(invalidColumn1), Context.getDefault(), Set.of());
        var invalidTable2 = invalidTable1.withId(new IdSimple(11)).withColumnList(List.of(invalidColumn2));
        var validTable1 = invalidTable1.withId(new IdSimple(12)).withColumnList(List.of(validColumn1));
        var validTable2 = invalidTable1.withId(new IdSimple(13)).withColumnList(List.of(validColumn2));
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