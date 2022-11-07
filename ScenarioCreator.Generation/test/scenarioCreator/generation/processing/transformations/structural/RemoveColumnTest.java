package scenarioCreator.generation.processing.transformations.structural;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scenarioCreator.data.Language;
import scenarioCreator.data.column.DataType;
import scenarioCreator.data.column.DataTypeEnum;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKey;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKeyInverse;
import scenarioCreator.data.column.constraint.ColumnConstraintPrimaryKey;
import scenarioCreator.data.column.context.ColumnContext;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.identification.IdSimple;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.utils.SSet;

import java.util.List;
import java.util.Random;

class RemoveColumnTest {

    @Test
    void transform() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var validColumn = new ColumnLeaf(new IdSimple(6), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var idGenerator = StructuralTestingUtils.getIdGenerator(8);
        final var transformation = new RemoveColumn();

        // --- Act
        final var newColumnList = transformation.transform(validColumn, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(0, newColumnList.size());
    }

    @Test
    void getCandidatesShouldHandleNormalCase() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var invalidColumn1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(7), SSet.of())));
        final var invalidColumn2 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(8), SSet.of())));
        final var invalidColumn3 = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(9))));
        final var validColumn = new ColumnLeaf(new IdSimple(6), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var columnList = List.of((Column) invalidColumn1, invalidColumn2, invalidColumn3, validColumn);
        final var transformation = new RemoveColumn();

        // --- Act
        final var newColumnList = transformation.getCandidates(columnList);

        // --- Assert
        Assertions.assertEquals(1, newColumnList.size());
        Assertions.assertTrue(newColumnList.contains(validColumn));
    }

    @Test
    void getCandidatesShouldRejectSingletonList() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var validColumn = new ColumnLeaf(new IdSimple(6), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var columnList = List.of((Column) validColumn);
        final var transformation = new RemoveColumn();

        // --- Act
        final var newColumnList = transformation.getCandidates(columnList);

        // --- Assert
        Assertions.assertEquals(0, newColumnList.size());
    }
}