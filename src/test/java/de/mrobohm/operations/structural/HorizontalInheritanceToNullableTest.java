package de.mrobohm.operations.structural;

import de.mrobohm.data.*;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.integrity.IntegrityChecker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Random;
import java.util.Set;

class HorizontalInheritanceToNullableTest {

    @Test
    void transform() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var nameDeriving = new StringPlusNaked("SpalteD", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var commonColumn1 = new ColumnLeaf(
                1, new StringPlusNaked("Spalte1", Language.Mixed), dataType,
                ColumnContext.getDefault(), Set.of()
        );
        var commonColumn2 = new ColumnLeaf(
                2, new StringPlusNaked("Spalte2", Language.Mixed), dataType,
                ColumnContext.getDefault(), Set.of()
        );
        var commonColumn3 = new ColumnLeaf(
                3, new StringPlusNaked("Spalte3", Language.Mixed), dataType,
                ColumnContext.getDefault(), Set.of()
        );
        var extraColumn = new ColumnLeaf(9, name, dataType, ColumnContext.getDefault(), Set.of());

        var baseTableColumnList = List.of((Column)commonColumn1, commonColumn2, commonColumn3);
        var baseTable = new Table(10, name, baseTableColumnList, Context.getDefault(), Set.of());
        var derivingTableColumnList = List.of(
                (Column)commonColumn1.withId(4),
                commonColumn2.withId(5),
                commonColumn3.withId(6),
                extraColumn);
        var derivingTable = new Table(11, nameDeriving, derivingTableColumnList, Context.getDefault(), Set.of());
        var tableSet = Set.of(baseTable, derivingTable);
        var schema = new Schema(15, name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        var transformation = new HorizontalInheritanceToNullable(1, 0.5);

        // --- Act
        var newSchema = transformation.transform(schema, new Random());

        // --- Assert
        IntegrityChecker.assertValidSchema(newSchema);
        Assertions.assertEquals(tableSet.size() - 1, newSchema.tableSet().size());
        var newTable = newSchema.tableSet().stream().toList().get(0);
        Assertions.assertEquals(baseTable.id(), newTable.id());
        Assertions.assertEquals(baseTable.name(), newTable.name());
        Assertions.assertEquals(baseTable.id(), newTable.id());
        var nullableExtraColumn = extraColumn.withDataType(extraColumn.dataType().withIsNullable(true));
        Assertions.assertTrue(newTable.columnList().contains(nullableExtraColumn));
        Assertions.assertTrue(newTable.columnList().contains(commonColumn1));
        Assertions.assertTrue(newTable.columnList().contains(commonColumn2));
        Assertions.assertTrue(newTable.columnList().contains(commonColumn3));
        Assertions.assertEquals(derivingTable.columnList().size(), newTable.columnList().size());
    }

    @Test
    void transformWithForeignKeys() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var nameDeriving = new StringPlusNaked("SpalteD", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var randomColumn = new ColumnLeaf(
                0, new StringPlusNaked("s", Language.Mixed), dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKeyInverse(1, Set.of()),
                        new ColumnConstraintForeignKeyInverse(4, Set.of())));
        var commonColumn1 = new ColumnLeaf(
                1, new StringPlusNaked("Spalte1", Language.Mixed), dataType,
                ColumnContext.getDefault(), Set.of(new ColumnConstraintForeignKey(0, Set.of()))
        );
        var commonColumn2 = new ColumnLeaf(
                2, new StringPlusNaked("Spalte2", Language.Mixed), dataType,
                ColumnContext.getDefault(), Set.of()
        );
        var commonColumn3 = new ColumnLeaf(
                3, new StringPlusNaked("Spalte3", Language.Mixed), dataType,
                ColumnContext.getDefault(), Set.of()
        );
        var extraColumn = new ColumnLeaf(9, name, dataType, ColumnContext.getDefault(), Set.of());

        var baseTableColumnList = List.of((Column)commonColumn1, commonColumn2, commonColumn3);
        var baseTable = new Table(10, name, baseTableColumnList, Context.getDefault(), Set.of());
        var derivingTableColumnList = List.of(
                (Column)commonColumn1.withId(4),
                commonColumn2.withId(5),
                commonColumn3.withId(6),
                extraColumn);
        var derivingTable = new Table(11, nameDeriving, derivingTableColumnList, Context.getDefault(), Set.of());
        var randomTable = new Table(12, name, List.of(randomColumn), Context.getDefault(), Set.of());
        var tableSet = Set.of(baseTable, derivingTable, randomTable);
        var schema = new Schema(15, name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        var transformation = new HorizontalInheritanceToNullable(1, 0.5);

        // --- Act
        var newSchema = transformation.transform(schema, new Random());

        // --- Assert
        IntegrityChecker.assertValidSchema(newSchema);
        Assertions.assertEquals(tableSet.size() - 1, newSchema.tableSet().size());
    }

    @ParameterizedTest
    @ValueSource(doubles = {0.5, 1.01})
    void isExecutableWithoutPrimaryKeys(double jaccardThreshold) {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var commonColumn1 = new ColumnLeaf(
                1, new StringPlusNaked("Spalte1", Language.Mixed), dataType,
                ColumnContext.getDefault(), Set.of()
        );
        var commonColumn2 = new ColumnLeaf(
                2, new StringPlusNaked("Spalte2", Language.Mixed), dataType,
                ColumnContext.getDefault(), Set.of()
        );
        var commonColumn3 = new ColumnLeaf(
                3, new StringPlusNaked("Spalte3", Language.Mixed), dataType,
                ColumnContext.getDefault(), Set.of()
        );
        var extraColumn = new ColumnLeaf(9, name, dataType, ColumnContext.getDefault(), Set.of());

        var baseTableColumnList = List.of((Column)commonColumn1, commonColumn2, commonColumn3);
        var baseTable = new Table(10, name, baseTableColumnList, Context.getDefault(), Set.of());
        var derivingTableColumnList = List.of(
                (Column)commonColumn1.withId(4),
                commonColumn2.withId(5),
                commonColumn3.withId(6),
                extraColumn);
        var derivingTable = new Table(11, name, derivingTableColumnList, Context.getDefault(), Set.of());
        var tableSet = Set.of(baseTable, derivingTable);
        var schema = new Schema(15, name, Context.getDefault(), tableSet);
        var transformation = new HorizontalInheritanceToNullable(1, jaccardThreshold);

        // --- Act
        var isExecutable = transformation.isExecutable(schema);

        // --- Assert
        Assertions.assertEquals(jaccardThreshold < 0.75, isExecutable);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9})
    void isExecutableWithPrimaryKeys(int primaryKeyCountThreshold) {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var commonColumn1 = new ColumnLeaf(
                1, new StringPlusNaked("Spalte1", Language.Mixed), dataType,
                ColumnContext.getDefault(), Set.of(new ColumnConstraintPrimaryKey(30))
        );
        var commonColumn2 = new ColumnLeaf(
                2, new StringPlusNaked("Spalte2", Language.Mixed), dataType,
                ColumnContext.getDefault(), Set.of(new ColumnConstraintPrimaryKey(30))
        );
        var commonColumn3 = new ColumnLeaf(
                3, new StringPlusNaked("Spalte3", Language.Mixed), dataType,
                ColumnContext.getDefault(), Set.of(new ColumnConstraintPrimaryKey(30))
        );
        var commonColumn11 = new ColumnLeaf(
                4, new StringPlusNaked("Spalte1", Language.Mixed), dataType,
                ColumnContext.getDefault(), Set.of(new ColumnConstraintPrimaryKey(31))
        );
        var commonColumn12 = new ColumnLeaf(
                5, new StringPlusNaked("Spalte2", Language.Mixed), dataType,
                ColumnContext.getDefault(), Set.of(new ColumnConstraintPrimaryKey(31))
        );
        var commonColumn13 = new ColumnLeaf(
                6, new StringPlusNaked("Spalte3", Language.Mixed), dataType,
                ColumnContext.getDefault(), Set.of(new ColumnConstraintPrimaryKey(31))
        );
        var extraColumn1 = new ColumnLeaf(
                7, new StringPlusNaked("Spalte4", Language.Mixed), dataType,
                ColumnContext.getDefault(), Set.of()
        );
        var extraColumn2 = new ColumnLeaf(
                8, new StringPlusNaked("Spalte5", Language.Mixed), dataType,
                ColumnContext.getDefault(), Set.of()
        );
        var extraColumn3 = new ColumnLeaf(
                9, new StringPlusNaked("Spalte6", Language.Mixed), dataType,
                ColumnContext.getDefault(), Set.of()
        );
        var extraColumn4 = new ColumnLeaf(10, name, dataType, ColumnContext.getDefault(), Set.of());

        var baseTableColumnList = List.of((Column)commonColumn1, commonColumn2, commonColumn3);
        var baseTable = new Table(20, name, baseTableColumnList, Context.getDefault(), Set.of());
        var derivingTableColumnList = List.of(
                (Column)commonColumn11,
                commonColumn12,
                commonColumn13,
                extraColumn1,
                extraColumn2,
                extraColumn3,
                extraColumn4);
        var derivingTable = new Table(21, name, derivingTableColumnList, Context.getDefault(), Set.of());
        var tableSet = Set.of(baseTable, derivingTable);
        var schema = new Schema(100, name, Context.getDefault(), tableSet);
        var transformation = new HorizontalInheritanceToNullable(primaryKeyCountThreshold, 1.01);

        // --- Act
        var isExecutable = transformation.isExecutable(schema);

        // --- Assert
        Assertions.assertEquals(primaryKeyCountThreshold <= 3, isExecutable);
    }
}