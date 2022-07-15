package de.mrobohm.transformations.linguistic;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.processing.transformations.linguistic.AddTypoToSchemaName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.Set;

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