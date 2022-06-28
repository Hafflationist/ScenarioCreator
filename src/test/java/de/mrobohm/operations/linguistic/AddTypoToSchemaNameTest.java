package de.mrobohm.operations.linguistic;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.operations.structural.StructuralTestingUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class AddTypoToSchemaNameTest {

    @Test
    void transform() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var schema = new Schema(new IdSimple(1), name, Context.getDefault(), Set.of());
        var transformation = new AddTypoToSchemaName();

        // --- Act
        var newSchema = transformation.transform(schema, new Random());

        // --- Assert
        Assertions.assertEquals(schema.id(), newSchema.id());
        Assertions.assertNotEquals(schema.name(), newSchema.name());
    }
}