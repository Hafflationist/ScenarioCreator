package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.dataset.Value;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.identification.IdMerge;
import de.mrobohm.data.identification.IdPart;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.integrity.IntegrityChecker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.Set;

class BinaryValueToTableTest {

    @Test
    void transform() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var valueSet = Set.of(new Value("Männlein"), new Value("Weiblein"));
        var invalidColumn = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(new IdSimple(7))));
        var validColumn = invalidColumn
                .withConstraintSet(Set.of())
                .withValueSet(valueSet)
                .withId(new IdSimple(3));
        var targetTable = new Table(
                new IdSimple(6),
                name,
                List.of(invalidColumn, validColumn),
                Context.getDefault(),
                Set.of()
        );
        var idGenerator = StructuralTestingUtils.getIdGenerator(8);
        var transformation = new BinaryValueToTable();

        // --- Act
        var newTableSet = transformation.transform(targetTable, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(2, newTableSet.size());
        Assertions.assertFalse(newTableSet.contains(targetTable));
        var tableList = newTableSet.stream().toList();
        var table1 = tableList.get(0);
        var table2 = tableList.get(1);
        Assertions.assertFalse(
                table1.columnList().stream()
                        .map(Column::id)
                        .anyMatch(id -> containsId(validColumn.id(), id))
        );
        Assertions.assertFalse(
                table2.columnList().stream()
                        .map(Column::id)
                        .anyMatch(id -> containsId(validColumn.id(), id))
        );
        IntegrityChecker.assertValidSchema(new Schema(new IdSimple(0), name, Context.getDefault(), newTableSet));
    }

    private boolean containsId(Id rootId, Id newId) {
        return switch (newId) {
            case IdSimple ids -> ids.equals(rootId);
            case IdPart idp -> idp.predecessorId().equals(rootId);
            case IdMerge idm -> idm.predecessorId1().equals(rootId) || idm.predecessorId2().equals(rootId);
        };
    }

    @Test
    void getCandidates() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var valueSet = Set.of(new Value("Männlein"), new Value("Weiblein"));
        var invalidColumn = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(new IdSimple(7))));
        var invalidTable1 = new Table(new IdSimple(2), name, List.of(invalidColumn), Context.getDefault(), Set.of());
        var validColumn = invalidColumn
                .withConstraintSet(Set.of())
                .withValueSet(valueSet)
                .withId(new IdSimple(3));
        var validTable = new Table(
                new IdSimple(6),
                name,
                List.of(invalidColumn, validColumn),
                Context.getDefault(),
                Set.of()
        );
        var invalidTable2 = new Table(new IdSimple(4), name, List.of(validColumn), Context.getDefault(), Set.of());
        var tableSet = Set.of(invalidTable1, invalidTable2, validTable);
        var transformation = new BinaryValueToTable();

        // --- Act
        var newTableSet = transformation.getCandidates(tableSet);

        // --- Assert
        Assertions.assertEquals(1, newTableSet.size());
        Assertions.assertTrue(newTableSet.contains(validTable));
    }
}