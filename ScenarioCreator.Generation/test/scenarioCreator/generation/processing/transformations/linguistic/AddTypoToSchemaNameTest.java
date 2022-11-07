package processing.transformations.linguistic;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scenarioCreator.data.Context;
import scenarioCreator.data.Language;
import scenarioCreator.data.Schema;
import scenarioCreator.data.identification.IdSimple;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.generation.processing.transformations.linguistic.AddTypoToSchemaName;
import scenarioCreator.utils.SSet;

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