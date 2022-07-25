package de.mrobohm.processing.transformations.linguistic;

import de.mrobohm.data.Language;
import de.mrobohm.data.column.ColumnContext;
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
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var column = new ColumnLeaf(
                new IdSimple(1), name, dataType, SSet.of(), ColumnContext.getDefault(), SSet.of()
        );
        var idGenerator = StructuralTestingUtils.getIdGenerator(2);
        var transformation = new AddTypoToColumnName();

        // --- Act
        var newColumnList = transformation.transform(column, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(1, newColumnList.size());
        var newColumn = newColumnList.get(0);
        Assertions.assertEquals(column.id(), newColumn.id());
        Assertions.assertNotEquals(column.name(), newColumn.name());
    }
}