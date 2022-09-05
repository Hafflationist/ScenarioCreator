package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.column.context.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.utils.SSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

class RemoveTableTest {

    @Test
    void transform() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var invalidColumn1 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(8), SSet.of())));
        final var validColumn1 = new ColumnLeaf(new IdSimple(6), name, dataType, ColumnContext.getDefault(), SSet.of());

        final var invalidTable1 = new Table(new IdSimple(10), name, List.of(invalidColumn1),
                Context.getDefault(), SSet.of(), SSet.of());
        final var validTable = invalidTable1.withId(new IdSimple(12)).withColumnList(List.of(validColumn1));
        final var idGenerator = StructuralTestingUtils.getIdGenerator(8);
        final var transformation = new RemoveTable();

        // --- Act
        final var candidates = transformation.transform(validTable, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(0, candidates.size());
    }

    @Test
    void getCandidates() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var invalidColumn1 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(8), SSet.of())));
        final var invalidColumn2 = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(9), SSet.of())));
        final var validColumn1 = new ColumnLeaf(new IdSimple(6), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var validColumn2 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(7))));

        final var invalidTable1 = new Table(new IdSimple(10), name, List.of(invalidColumn1),
                Context.getDefault(), SSet.of(), SSet.of());
        final var invalidTable2 = invalidTable1.withId(new IdSimple(11)).withColumnList(List.of(invalidColumn2));
        final var validTable1 = invalidTable1.withId(new IdSimple(12)).withColumnList(List.of(validColumn1));
        final var validTable2 = invalidTable1.withId(new IdSimple(13)).withColumnList(List.of(validColumn2));
        final var tableSet = SSet.of(invalidTable1, invalidTable2, validTable1, validTable2);
        final var transformation = new RemoveTable();

        // --- Act
        final var candidates = transformation.getCandidates(tableSet);

        // --- Assert
        Assertions.assertEquals(2, candidates.size());
        Assertions.assertFalse(candidates.contains(invalidTable1));
        Assertions.assertFalse(candidates.contains(invalidTable2));
        Assertions.assertTrue(candidates.contains(validTable1));
        Assertions.assertTrue(candidates.contains(validTable2));
    }
}