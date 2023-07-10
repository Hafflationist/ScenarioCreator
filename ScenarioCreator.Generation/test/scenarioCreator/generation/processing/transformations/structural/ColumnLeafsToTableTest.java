package scenarioCreator.generation.processing.transformations.structural;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scenarioCreator.data.Context;
import scenarioCreator.data.Language;
import scenarioCreator.data.Schema;
import scenarioCreator.data.column.DataType;
import scenarioCreator.data.column.DataTypeEnum;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKey;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKeyInverse;
import scenarioCreator.data.column.constraint.ColumnConstraintPrimaryKey;
import scenarioCreator.data.column.context.ColumnContext;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.identification.IdPart;
import scenarioCreator.data.identification.IdSimple;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.data.table.Table;
import scenarioCreator.generation.processing.integrity.IntegrityChecker;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.SSet;

import java.util.List;
import java.util.Random;

class ColumnLeafsToTableTest {
    @Test
    void transform() {
        // --- Arrange
        final var random = new Random();
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var columnLeaf = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(7))));
        final var columnLeafGroupable1 = columnLeaf.withConstraintSet(SSet.of()).withId(new IdSimple(3));
        final var columnLeafGroupable2 = columnLeaf.withConstraintSet(SSet.of()).withId(new IdSimple(4));
        final var targetTable = StructuralTestingUtils.createTable(
                6,
                List.of(columnLeaf, columnLeafGroupable1, columnLeafGroupable2),
                random
        );
        final var idGenerator = StructuralTestingUtils.getIdGenerator(8);
        final var transformation = new ColumnLeafsToTable();

        // --- Act
        final var pair = transformation.transform(targetTable, idGenerator, random);
        final var newTableSet = pair.first();
        final var tgdList = pair.second();

        // --- Assert
        Assertions.assertFalse(tgdList.isEmpty());
        Assertions.assertEquals(2, newTableSet.size());
        Assertions.assertFalse(newTableSet.contains(targetTable));
        final var newTableList = newTableSet.stream()
                .filter(t -> t.id() instanceof IdPart idp && idp.predecessorId().equals(targetTable.id()))
                .toList();
        final var originTable = newTableList.stream()
                .filter(t -> t.name().equals(targetTable.name())).findFirst().get();
        final var extractedTable = newTableList.stream()
                .filter(t -> !t.name().equals(targetTable.name())).findFirst().get();
        Assertions.assertNotEquals(targetTable.columnList(), originTable.columnList());
        Assertions.assertEquals(targetTable.context(), originTable.context());
        Assertions.assertEquals(targetTable.tableConstraintSet(), originTable.tableConstraintSet());
        Assertions.assertEquals(
                targetTable.columnList().size() + 2,
                originTable.columnList().size() + extractedTable.columnList().size()
        );
        Assertions.assertTrue(originTable.columnList().stream().anyMatch(
                column -> column.id() instanceof IdSimple ids && ids.number() >= 7)); // checks for new column
        Assertions.assertTrue(originTable.columnList().stream()
                .anyMatch(column -> column.containsConstraint(ColumnConstraintForeignKey.class)));
        Assertions.assertTrue(extractedTable.columnList().stream()
                .anyMatch(column -> column.containsConstraint(ColumnConstraintForeignKeyInverse.class)));
        Assertions.assertTrue(extractedTable.columnList().stream()
                .anyMatch(column -> column.containsConstraint(ColumnConstraintPrimaryKey.class)));
        Assertions.assertTrue(extractedTable.columnList().stream()
                .anyMatch(column -> column.constraintSet().stream()
                        .anyMatch(c -> c instanceof ColumnConstraintForeignKeyInverse)));
        Assertions.assertTrue(extractedTable.columnList().stream().anyMatch(column -> column.constraintSet().stream()
                .anyMatch(c -> c instanceof ColumnConstraintPrimaryKey)));
        Assertions.assertTrue(extractedTable.columnList().contains(columnLeafGroupable1)
                || extractedTable.columnList().contains(columnLeafGroupable2));
        IntegrityChecker.assertValidSchema(new Schema(new IdSimple(0), name, Context.getDefault(), newTableSet));
    }

    @Test
    void getCandidates() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var columnLeaf = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(7))));
        final var invalidTable = new Table(new IdSimple(2), name, List.of(columnLeaf),
                Context.getDefault(), SSet.of(), SSet.of());
        final var columnLeafGroupable1 = columnLeaf.withConstraintSet(SSet.of()).withId(new IdSimple(3));
        final var columnLeafGroupable2 = columnLeaf.withConstraintSet(SSet.of()).withId(new IdSimple(4));
        final var validTable = new Table(new IdSimple(6), name, List.of(columnLeaf, columnLeafGroupable1, columnLeafGroupable2),
                Context.getDefault(), SSet.of(), SSet.of());
        final var tableSet = SSet.of(invalidTable, validTable);
        final var transformation = new ColumnLeafsToTable();

        // --- Act
        final var newTableSet = transformation.getCandidates(tableSet);

        // --- Assert
        Assertions.assertEquals(1, newTableSet.size());
        Assertions.assertTrue(newTableSet.contains(validTable));
    }
}
