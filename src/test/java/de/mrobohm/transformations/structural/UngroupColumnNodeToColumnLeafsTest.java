package de.mrobohm.transformations.structural;

import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.Language;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Random;
import java.util.Set;

class UngroupColumnNodeToColumnLeafsTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void transform(boolean isNodeNullable) {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var column1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKeyInverse(new IdSimple(6), Set.of())));
        var column2 = new ColumnLeaf(new IdSimple(2), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(new IdSimple(7), Set.of())));
        var columnNode = new ColumnNode(new IdSimple(5), name,
                List.of(column1, column2), Set.of(), isNodeNullable);

        var idGenerator = StructuralTestingUtils.getIdGenerator(0);
        var transformation = new UngroupColumnNodeToColumnLeafs();

        // --- Act
        var newColumnList = transformation.transform(columnNode, idGenerator, new Random());

        // --- Assert
        Assertions.assertTrue(
                newColumnList.contains(column2)
        );
        Assertions.assertTrue(
                newColumnList.contains(column1.withDataType(column1.dataType().withIsNullable(isNodeNullable)))
        );
    }

    @Test
    void getCandidates() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var column1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKeyInverse(new IdSimple(6), Set.of())));
        var column2 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(new IdSimple(7), Set.of())));
        var column3 = new ColumnLeaf(
                new IdSimple(4), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(), Set.of());
        var columnNode = new ColumnNode(new IdSimple(5), name,
                List.of(column2, column3), Set.of(), false);


        var columnList = List.of((Column) column1, column2, column3, columnNode);
        var transformation = new UngroupColumnNodeToColumnLeafs();

        // --- Act
        var candidates = transformation.getCandidates(columnList);

        // --- Assert
        Assertions.assertFalse(candidates.contains(column1));
        Assertions.assertFalse(candidates.contains(column2));
        Assertions.assertFalse(candidates.contains(column3));
        Assertions.assertTrue(candidates.contains(columnNode));
    }
}