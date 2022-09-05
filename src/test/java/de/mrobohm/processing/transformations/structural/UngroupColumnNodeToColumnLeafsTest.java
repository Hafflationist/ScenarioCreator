package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.Language;
import de.mrobohm.data.column.context.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.utils.SSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Random;

class UngroupColumnNodeToColumnLeafsTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void transform(boolean isNodeNullable) {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var column1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(6), SSet.of())));
        final var column2 = new ColumnLeaf(new IdSimple(2), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(7), SSet.of())));
        final var columnNode = new ColumnNode(new IdSimple(5), name,
                List.of(column1, column2), SSet.of(), isNodeNullable);

        final var idGenerator = StructuralTestingUtils.getIdGenerator(0);
        final var transformation = new UngroupColumnNodeToColumnLeafs();

        // --- Act
        final var newColumnList = transformation.transform(columnNode, idGenerator, new Random());

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
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var column1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(6), SSet.of())));
        final var column2 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(7), SSet.of())));
        final var column3 = new ColumnLeaf(
                new IdSimple(4), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(), SSet.of());
        final var columnNode = new ColumnNode(new IdSimple(5), name,
                List.of(column2, column3), SSet.of(), false);


        final var columnList = List.of((Column) column1, column2, column3, columnNode);
        final var transformation = new UngroupColumnNodeToColumnLeafs();

        // --- Act
        final var candidates = transformation.getCandidates(columnList);

        // --- Assert
        Assertions.assertFalse(candidates.contains(column1));
        Assertions.assertFalse(candidates.contains(column2));
        Assertions.assertFalse(candidates.contains(column3));
        Assertions.assertTrue(candidates.contains(columnNode));
    }
}