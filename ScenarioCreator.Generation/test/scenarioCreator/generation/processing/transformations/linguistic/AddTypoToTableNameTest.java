package scenarioCreator.generation.processing.transformations.linguistic;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scenarioCreator.data.Context;
import scenarioCreator.data.Language;
import scenarioCreator.data.identification.IdSimple;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.data.table.Table;
import scenarioCreator.generation.processing.transformations.structural.StructuralTestingUtils;
import scenarioCreator.utils.SSet;

import java.util.List;
import java.util.Random;

class AddTypoToTableNameTest {

    @Test
    void transform() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var table = new Table(new IdSimple(1), name, List.of(), Context.getDefault(), SSet.of(), SSet.of());
        final var idGenerator = StructuralTestingUtils.getIdGenerator(2);
        final var transformation = new AddTypoToTableName();

        // --- Act
        final var newTableSet = transformation.transform(table, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(1, newTableSet.size());
        final var newTable = newTableSet.stream().toList().get(0);
        Assertions.assertEquals(table.id(), newTable.id());
        Assertions.assertNotEquals(table.name(), newTable.name());
    }
}