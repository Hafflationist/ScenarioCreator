package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.column.context.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.integrity.IntegrityChecker;
import de.mrobohm.utils.SSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class GroupColumnLeafsToNodeTest {

    @Test
    void transform() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var neutralColumn1 = new ColumnLeaf(new IdSimple(12), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var neutralColumn2 = new ColumnLeaf(new IdSimple(13), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var columnLeaf = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(7))));
        final var columnLeafGroupable1 = columnLeaf.withConstraintSet(SSet.of()).withId(new IdSimple(3));
        final var columnLeafGroupable2 = columnLeaf.withConstraintSet(SSet.of()).withId(new IdSimple(4));
        final var targetTable = StructuralTestingUtils.createTable(
                6, List.of(columnLeaf, columnLeafGroupable1, columnLeafGroupable2, neutralColumn1, neutralColumn2)
        );
        final var idGenerator = StructuralTestingUtils.getIdGenerator(8);
        final var transformation = new GroupColumnLeafsToNode();

        // --- Act
        final var newTableSet = transformation.transform(targetTable, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(1, newTableSet.size());
        final var newTable = newTableSet.stream().toList().get(0);
        Assertions.assertEquals(targetTable.id(), newTable.id());
        Assertions.assertEquals(targetTable.name(), newTable.name());
        Assertions.assertNotEquals(targetTable.columnList(), newTable.columnList());
        Assertions.assertEquals(targetTable.context(), newTable.context());
        Assertions.assertEquals(targetTable.tableConstraintSet(), newTable.tableConstraintSet());

        final var flattenedColumnSet = newTable.columnList().stream().flatMap(column -> switch (column) {
            case ColumnLeaf leaf -> Stream.of(leaf);
            case ColumnNode node -> node.columnList().stream();
            case ColumnCollection col -> col.columnList().stream();
        }).collect(Collectors.toCollection(TreeSet::new));

        Assertions.assertEquals(new HashSet<>(targetTable.columnList()), flattenedColumnSet);

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
        final var validTable = new Table(
                new IdSimple(6), name, List.of(columnLeaf, columnLeafGroupable1, columnLeafGroupable2),
                Context.getDefault(), SSet.of(), SSet.of());
        final var tableSet = SSet.of(invalidTable, validTable);
        final var transformation = new GroupColumnLeafsToNode();

        // --- Act
        final var newTableSet = transformation.getCandidates(tableSet);

        // --- Assert
        Assertions.assertEquals(1, newTableSet.size());
        Assertions.assertTrue(newTableSet.contains(validTable));
    }
}