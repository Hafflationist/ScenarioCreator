package de.mrobohm.transformations.structural;

import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.Language;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.Set;

class RemoveColumnTest {

    @Test
    void transform() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var validColumn = new ColumnLeaf(new IdSimple(6), name, dataType, ColumnContext.getDefault(), Set.of());
        var idGenerator = StructuralTestingUtils.getIdGenerator(8);
        var transformation = new RemoveColumn();

        // --- Act
        var newColumnList = transformation.transform(validColumn, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(0, newColumnList.size());
    }

    @Test
    void getCandidatesShouldHandleNormalCase() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var invalidColumn1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKeyInverse(new IdSimple(7), Set.of())));
        var invalidColumn2 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(new IdSimple(8), Set.of())));
        var invalidColumn3 = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(new IdSimple(9))));
        var validColumn = new ColumnLeaf(new IdSimple(6), name, dataType, ColumnContext.getDefault(), Set.of());
        var columnList = List.of((Column) invalidColumn1, invalidColumn2, invalidColumn3, validColumn);
        var transformation = new RemoveColumn();

        // --- Act
        var newColumnList = transformation.getCandidates(columnList);

        // --- Assert
        Assertions.assertEquals(1, newColumnList.size());
        Assertions.assertTrue(newColumnList.contains(validColumn));
    }

    @Test
    void getCandidatesShouldRejectSingletonList() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var validColumn = new ColumnLeaf(new IdSimple(6), name, dataType, ColumnContext.getDefault(), Set.of());
        var columnList = List.of((Column) validColumn);
        var transformation = new RemoveColumn();

        // --- Act
        var newColumnList = transformation.getCandidates(columnList);

        // --- Assert
        Assertions.assertEquals(0, newColumnList.size());
    }
}