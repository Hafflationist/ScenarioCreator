package de.mrobohm.operations.linguistic;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.operations.structural.StructuralTestingUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class AddTypoToTableNameTest {

    @Test
    void transform() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var table = new Table(new IdSimple(1), name, List.of(), Context.getDefault(), Set.of());
        var idGenerator = StructuralTestingUtils.getIdGenerator(2);
        var transformation = new AddTypoToTableName();

        // --- Act
        var newTableSet = transformation.transform(table, Set.of(table), idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(1, newTableSet.size());
        var newTable = newTableSet.stream().toList().get(0);
        Assertions.assertEquals(table.id(), newTable.id());
        Assertions.assertNotEquals(table.name(), newTable.name());
    }
}