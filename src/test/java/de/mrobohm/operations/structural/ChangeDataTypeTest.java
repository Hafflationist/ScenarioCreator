package de.mrobohm.operations.structural;

import de.mrobohm.data.DataType;
import de.mrobohm.data.DataTypeEnum;
import de.mrobohm.data.Language;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.primitives.StringPlusNaked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.Set;

class ChangeDataTypeTest {

    @Test
    void transform() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var validDataType = new DataType(DataTypeEnum.INT32, false);
        var column = new ColumnLeaf(1, name, validDataType, ColumnContext.getDefault(), Set.of());
        var idGenerator = StructuralTestingUtils.getIdGenerator(0);
        var transformation = new ChangeDataType();

        // --- Act
        var newColumnList = transformation.transform(column, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(1, newColumnList.size());
        Assertions.assertTrue(newColumnList.get(0) instanceof ColumnLeaf);
        var newColumn = (ColumnLeaf) newColumnList.get(0);
        Assertions.assertEquals(column.id(), newColumn.id());
        Assertions.assertEquals(column.name(), newColumn.name());
        Assertions.assertNotEquals(column.dataType(), newColumn.dataType());
        Assertions.assertEquals(column.context(), newColumn.context());
        Assertions.assertEquals(column.constraintSet(), newColumn.constraintSet());
    }

    @Test
    void getCandidates() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var validDataType = new DataType(DataTypeEnum.INT32, false);
        var validColumn = new ColumnLeaf(1, name, validDataType, ColumnContext.getDefault(), Set.of());
        var invalidColumn1 = new ColumnNode(2, name, List.of(), Set.of(), false);
        var invalidColumn2 = validColumn.withDataType(new DataType(DataTypeEnum.NVARCHAR, true));
        var invalidColumn3 = validColumn.withConstraintSet(Set.of(new ColumnConstraintForeignKey(2, Set.of())));
        var invalidColumn4 = validColumn.withConstraintSet(Set.of(new ColumnConstraintForeignKeyInverse(2, Set.of())));
        var columnList = List.of(validColumn, (Column) invalidColumn1, invalidColumn2, invalidColumn3, invalidColumn4);
        var transformation = new ChangeDataType();

        // --- Act
        var candidateList = transformation.getCandidates(columnList);

        // --- Assert
        Assertions.assertEquals(1, candidateList.size());
        Assertions.assertTrue(candidateList.contains(validColumn));
    }
}