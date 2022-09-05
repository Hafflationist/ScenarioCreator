package de.mrobohm.processing.transformations.linguistic;

import de.mrobohm.data.Language;
import de.mrobohm.data.column.context.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.processing.transformations.structural.StructuralTestingUtils;
import de.mrobohm.utils.SSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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