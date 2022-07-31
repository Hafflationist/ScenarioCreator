package de.mrobohm.processing.transformations.linguistic;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.transformations.structural.StructuralTestingUtils;
import de.mrobohm.utils.SSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

class AddTypoToTableNameTest {

    @Test
    void transform() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var table = new Table(new IdSimple(1), name, List.of(), Context.getDefault(), SSet.of(), SSet.of());
        var idGenerator = StructuralTestingUtils.getIdGenerator(2);
        var transformation = new AddTypoToTableName();

        // --- Act
        var newTableSet = transformation.transform(table, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(1, newTableSet.size());
        var newTable = newTableSet.stream().toList().get(0);
        Assertions.assertEquals(table.id(), newTable.id());
        Assertions.assertNotEquals(table.name(), newTable.name());
    }
}