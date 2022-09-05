package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.Language;
import de.mrobohm.data.column.context.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.utils.SSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

class DeNullificationTest {

    @Test
    void transform() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var validDataType = DataType.getRandom(new Random()).withIsNullable(true);
        final var column = new ColumnLeaf(new IdSimple(1), name, validDataType, ColumnContext.getDefault(), SSet.of());
        final var idGenerator = StructuralTestingUtils.getIdGenerator(0);
        final var transformation = new DeNullification();

        // --- Act
        final var newColumnList = transformation.transform(column, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(1, newColumnList.size());
        Assertions.assertTrue(newColumnList.get(0) instanceof ColumnLeaf);
        final var newColumn = (ColumnLeaf) newColumnList.get(0);
        Assertions.assertEquals(column.id(), newColumn.id());
        Assertions.assertEquals(column.name(), newColumn.name());
        Assertions.assertEquals(column.dataType().withIsNullable(false), newColumn.dataType());
        Assertions.assertEquals(column.context(), newColumn.context());
        Assertions.assertEquals(column.constraintSet(), newColumn.constraintSet());
    }

    @Test
    void getCandidates() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var invalidDataType = new DataType(DataTypeEnum.INT32, false);
        final var validDataType = invalidDataType.withIsNullable(true);
        final var invalidColumn1 = new ColumnLeaf(
                new IdSimple(1), name, invalidDataType, ColumnContext.getDefault(), SSet.of());
        final var invalidColumn2 = new ColumnLeaf(
                new IdSimple(2), name, validDataType, ColumnContext.getDefault(), SSet.of(
                new ColumnConstraintForeignKey(new IdSimple(3), SSet.of())
        ));
        final var validColumn = new ColumnLeaf(new IdSimple(4), name, validDataType, ColumnContext.getDefault(), SSet.of());
        final var columnList = List.of((Column) invalidColumn1, invalidColumn2, validColumn);
        final var transformation = new DeNullification();

        // --- Act
        final var candidateList = transformation.getCandidates(columnList);

        // --- Assert
        Assertions.assertEquals(1, candidateList.size());
        Assertions.assertTrue(candidateList.contains(validColumn));
    }
}