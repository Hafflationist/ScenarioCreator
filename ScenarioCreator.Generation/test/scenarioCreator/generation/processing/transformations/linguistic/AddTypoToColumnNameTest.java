package scenarioCreator.generation.processing.transformations.linguistic;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scenarioCreator.data.Language;
import scenarioCreator.data.column.DataType;
import scenarioCreator.data.column.DataTypeEnum;
import scenarioCreator.data.column.context.ColumnContext;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.identification.IdSimple;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.generation.processing.transformations.structural.StructuralTestingUtils;
import scenarioCreator.utils.SSet;

import java.util.Random;

class AddTypoToColumnNameTest {

    @Test
    void transform() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var column = new ColumnLeaf(
                new IdSimple(1), name, dataType, SSet.of(), ColumnContext.getDefault(), SSet.of()
        );
        final var idGenerator = StructuralTestingUtils.getIdGenerator(2);
        final var transformation = new AddTypoToColumnName();

        // --- Act
        final var newColumnList = transformation.transform(column, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(1, newColumnList.size());
        final var newColumn = newColumnList.get(0);
        Assertions.assertEquals(column.id(), newColumn.id());
        Assertions.assertNotEquals(column.name(), newColumn.name());
    }
}