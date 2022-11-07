package scenarioCreator.generation.processing.transformations.structural;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scenarioCreator.data.Language;
import scenarioCreator.data.column.DataType;
import scenarioCreator.data.column.DataTypeEnum;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKey;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKeyInverse;
import scenarioCreator.data.column.context.ColumnContext;
import scenarioCreator.data.column.context.NumericalDistribution;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.column.nesting.ColumnNode;
import scenarioCreator.data.identification.IdSimple;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.utils.SSet;

import java.util.List;
import java.util.Map;
import java.util.Random;

class ChangeDataTypeTest {

    @Test
    void transform() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var validDataType = new DataType(DataTypeEnum.INT32, false);
        final var nd = new NumericalDistribution(1.0, Map.of(1, 0.2, 2, 0.9));
        final var column = new ColumnLeaf(
                new IdSimple(1), name, validDataType, ColumnContext.getDefaultWithNd(nd), SSet.of()
        );
        final var idGenerator = StructuralTestingUtils.getIdGenerator(0);
        final var transformation = new ChangeDataType();

        // --- Act
        final var newColumnList = transformation.transform(column, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(1, newColumnList.size());
        Assertions.assertTrue(newColumnList.get(0) instanceof ColumnLeaf);
        final var newColumn = (ColumnLeaf) newColumnList.get(0);
        Assertions.assertEquals(column.id(), newColumn.id());
        Assertions.assertEquals(column.name(), newColumn.name());
        Assertions.assertNotEquals(column.dataType(), newColumn.dataType());
        Assertions.assertEquals(column.context().withNumericalDistribution(NumericalDistribution.getDefault()), newColumn.context());
        Assertions.assertEquals(column.constraintSet(), newColumn.constraintSet());
    }

    @Test
    void getCandidates() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var validDataType = new DataType(DataTypeEnum.INT32, false);
        final var validColumn = new ColumnLeaf(new IdSimple(1), name, validDataType, ColumnContext.getDefault(), SSet.of());
        final var invalidColumn1 = new ColumnNode(new IdSimple(2), name, List.of(), SSet.of(), false);
        final var invalidColumn2 = validColumn.withDataType(new DataType(DataTypeEnum.NVARCHAR, true));
        final var invalidColumn3 = validColumn.withConstraintSet(
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(2), SSet.of())));
        final var invalidColumn4 = validColumn.withConstraintSet(
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(2), SSet.of())));
        final var columnList = List.of(validColumn, (Column) invalidColumn1, invalidColumn2, invalidColumn3, invalidColumn4);
        final var transformation = new ChangeDataType();

        // --- Act
        final var candidateList = transformation.getCandidates(columnList);

        // --- Assert
        Assertions.assertEquals(1, candidateList.size());
        Assertions.assertTrue(candidateList.contains(validColumn));
    }
}