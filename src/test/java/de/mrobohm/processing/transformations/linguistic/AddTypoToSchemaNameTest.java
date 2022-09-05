package de.mrobohm.processing.transformations.linguistic;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.utils.SSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Random;

class AddTypoToSchemaNameTest {

    @Test
    void transform() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var schema = new Schema(new IdSimple(1), name, Context.getDefault(), SSet.of());
        final var transformation = new AddTypoToSchemaName();

        // --- Act
        final var newSchema = transformation.transform(schema, new Random());

        // --- Assert
        Assertions.assertEquals(schema.id(), newSchema.id());
        Assertions.assertNotEquals(schema.name(), newSchema.name());
    }
}