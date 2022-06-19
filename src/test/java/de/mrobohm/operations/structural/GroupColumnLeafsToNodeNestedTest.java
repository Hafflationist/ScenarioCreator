package de.mrobohm.operations.structural;

import de.mrobohm.data.*;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.integrity.IntegrityChecker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class GroupColumnLeafsToNodeNestedTest {

    @Test
    void transform() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var columnLeaf = new ColumnLeaf(1, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(7)));
        var columnLeafGroupable1 = columnLeaf.withConstraintSet(Set.of()).withId(3);
        var columnLeafGroupable2 = columnLeaf.withConstraintSet(Set.of()).withId(4);
        var targetColumn = new ColumnNode(
                6, name, List.of(columnLeaf, columnLeafGroupable1, columnLeafGroupable2), Set.of(), false
        );
        var idGenerator = StructuralTestingUtils.getIdGenerator(8);
        var transformation = new GroupColumnLeafsToNodeNested();

        // --- Act
        var newColumnList = transformation.transform(targetColumn, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(1, newColumnList.size());
        Assertions.assertTrue(newColumnList.stream().toList().get(0) instanceof ColumnNode);
        var newColumn = (ColumnNode)newColumnList.stream().toList().get(0);
        Assertions.assertEquals(targetColumn.id(), newColumn.id());
        Assertions.assertEquals(targetColumn.name(), newColumn.name());
        Assertions.assertNotEquals(targetColumn.columnList(), newColumn.columnList());
        Assertions.assertEquals(targetColumn.constraintSet(), newColumn.constraintSet());
        Assertions.assertEquals(targetColumn.isNullable(), newColumn.isNullable());

        var flattenedColumnSet = newColumn.columnList().stream().flatMap(column -> switch (column) {
            case ColumnLeaf leaf -> Stream.of(leaf);
            case ColumnNode node -> node.columnList().stream();
            case ColumnCollection col -> col.columnList().stream();
        }).collect(Collectors.toSet());

        Assertions.assertEquals(new HashSet<>(targetColumn.columnList()), flattenedColumnSet);
    }

    @Test
    void getCandidates() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var columnLeaf = new ColumnLeaf(1, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(7)));
        var invalidColumn = new ColumnNode(2, name, List.of(columnLeaf), Set.of(), false);
        var columnLeafGroupable1 = columnLeaf.withConstraintSet(Set.of()).withId(3);
        var columnLeafGroupable2 = columnLeaf.withConstraintSet(Set.of()).withId(4);
        var validColumn1 = new ColumnNode(
                6, name, List.of(columnLeaf, columnLeafGroupable1, columnLeafGroupable2), Set.of(), false
        );
        var validColumn2 = new ColumnCollection(
                6, name, List.of(columnLeaf, columnLeafGroupable1, columnLeafGroupable2), Set.of(), false
        );
        var columnList = List.of((Column)invalidColumn, validColumn1, validColumn2);
        var transformation = new GroupColumnLeafsToNodeNested();

        // --- Act
        var newColumnList = transformation.getCandidates(columnList);

        // --- Assert
        Assertions.assertEquals(2, newColumnList.size());
        Assertions.assertTrue(newColumnList.contains(validColumn1));
        Assertions.assertTrue(newColumnList.contains(validColumn2));
    }
}