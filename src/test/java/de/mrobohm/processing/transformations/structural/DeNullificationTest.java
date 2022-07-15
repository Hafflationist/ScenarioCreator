package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.Language;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.Set;

class DeNullificationTest {

    @Test
    void transform() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var validDataType = DataType.getRandom(new Random()).withIsNullable(true);
        var column = new ColumnLeaf(new IdSimple(1), name, validDataType, ColumnContext.getDefault(), Set.of());
        var idGenerator = StructuralTestingUtils.getIdGenerator(0);
        var transformation = new DeNullification();

        // --- Act
        var newColumnList = transformation.transform(column, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(1, newColumnList.size());
        Assertions.assertTrue(newColumnList.get(0) instanceof ColumnLeaf);
        var newColumn = (ColumnLeaf) newColumnList.get(0);
        Assertions.assertEquals(column.id(), newColumn.id());
        Assertions.assertEquals(column.name(), newColumn.name());
        Assertions.assertEquals(column.dataType().withIsNullable(false), newColumn.dataType());
        Assertions.assertEquals(column.context(), newColumn.context());
        Assertions.assertEquals(column.constraintSet(), newColumn.constraintSet());
    }

    @Test
    void getCandidates() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var invalidDataType = new DataType(DataTypeEnum.INT32, false);
        var validDataType = invalidDataType.withIsNullable(true);
        var invalidColumn1 = new ColumnLeaf(
                new IdSimple(1), name, invalidDataType, ColumnContext.getDefault(), Set.of());
        var invalidColumn2 = new ColumnLeaf(
                new IdSimple(2), name, validDataType, ColumnContext.getDefault(), Set.of(
                new ColumnConstraintForeignKey(new IdSimple(3), Set.of())
        ));
        var validColumn = new ColumnLeaf(new IdSimple(4), name, validDataType, ColumnContext.getDefault(), Set.of());
        var columnList = List.of((Column)invalidColumn1, invalidColumn2, validColumn);
        var transformation = new DeNullification();

        // --- Act
        var candidateList = transformation.getCandidates(columnList);

        // --- Assert
        Assertions.assertEquals(1, candidateList.size());
        Assertions.assertTrue(candidateList.contains(validColumn));
    }
}