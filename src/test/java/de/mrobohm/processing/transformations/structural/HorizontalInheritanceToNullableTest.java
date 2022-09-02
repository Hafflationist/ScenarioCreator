package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.column.context.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.IdMerge;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.integrity.IntegrityChecker;
import de.mrobohm.utils.SSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Random;

class HorizontalInheritanceToNullableTest {

    @Test
    void transform() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
//        var nameDeriving = new StringPlusNaked("SpalteD", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var commonColumn1 = new ColumnLeaf(
                new IdSimple(1), new StringPlusNaked("Spalte1", Language.Mixed), dataType,
                ColumnContext.getDefault(), SSet.of()
        );
        var commonColumn2 = new ColumnLeaf(
                new IdSimple(2), new StringPlusNaked("Spalte2", Language.Mixed), dataType,
                ColumnContext.getDefault(), SSet.of()
        );
        var commonColumn3 = new ColumnLeaf(
                new IdSimple(3), new StringPlusNaked("Spalte3", Language.Mixed), dataType,
                ColumnContext.getDefault(), SSet.of()
        );
        var extraColumn = new ColumnLeaf(new IdSimple(9), name, dataType, ColumnContext.getDefault(), SSet.of());

        var baseTableColumnList = List.of((Column) commonColumn1, commonColumn2, commonColumn3);
        var baseTable = StructuralTestingUtils.createTable(
                10, baseTableColumnList
        );
        var derivingTableColumnList = List.of(
                (Column) commonColumn1.withId(new IdSimple(4)),
                commonColumn2.withId(new IdSimple(5)),
                commonColumn3.withId(new IdSimple(6)),
                extraColumn);
        var derivingTable = StructuralTestingUtils.createTable(11, derivingTableColumnList);
        var tableSet = SSet.of(baseTable, derivingTable);
        var schema = new Schema(new IdSimple(15), name, Context.getDefault(), tableSet);
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
        Assertions.assertEquals(baseTableColumnList.size(),
                newTable.columnList().stream().filter(column -> column.id() instanceof IdMerge).count());
        Assertions.assertEquals(derivingTable.columnList().size(), newTable.columnList().size());
    }

    @Test
    void transformWithForeignKeys() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var nameDeriving = new StringPlusNaked("SpalteD", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var randomColumn = new ColumnLeaf(
                new IdSimple(0), new StringPlusNaked("s", Language.Mixed),
                dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(1), SSet.of()),
                        new ColumnConstraintForeignKeyInverse(new IdSimple(4), SSet.of())));
        var commonColumn1 = new ColumnLeaf(
                new IdSimple(1), new StringPlusNaked("Spalte1", Language.Mixed),
                dataType,
                ColumnContext.getDefault(), SSet.of(new ColumnConstraintForeignKey(new IdSimple(0), SSet.of()))
        );
        var commonColumn2 = new ColumnLeaf(
                new IdSimple(2), new StringPlusNaked("Spalte2", Language.Mixed),
                dataType,
                ColumnContext.getDefault(), SSet.of()
        );
        var commonColumn3 = new ColumnLeaf(
                new IdSimple(3), new StringPlusNaked("Spalte3", Language.Mixed),
                dataType,
                ColumnContext.getDefault(), SSet.of()
        );
        var extraColumn = new ColumnLeaf(new IdSimple(9), name, dataType, ColumnContext.getDefault(), SSet.of());

        var baseTableColumnList = List.of((Column) commonColumn1, commonColumn2, commonColumn3);
        var baseTable = StructuralTestingUtils.createTable(
                10, baseTableColumnList
        );
        var derivingTableColumnList = List.of(
                (Column) commonColumn1.withId(new IdSimple(4)),
                commonColumn2.withId(new IdSimple(5)),
                commonColumn3.withId(new IdSimple(6)),
                extraColumn);
        var derivingTable = StructuralTestingUtils.createTable(11, derivingTableColumnList);
        var randomTable = new Table(new IdSimple(12), name, List.of(randomColumn), Context.getDefault(), SSet.of(), SSet.of());
        var tableSet = SSet.of(baseTable, derivingTable, randomTable);
        var schema = new Schema(new IdSimple(15), name, Context.getDefault(), tableSet);
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
                new IdSimple(1), new StringPlusNaked("Spalte1", Language.Mixed), dataType,
                ColumnContext.getDefault(), SSet.of()
        );
        var commonColumn2 = new ColumnLeaf(
                new IdSimple(2), new StringPlusNaked("Spalte2", Language.Mixed), dataType,
                ColumnContext.getDefault(), SSet.of()
        );
        var commonColumn3 = new ColumnLeaf(
                new IdSimple(3), new StringPlusNaked("Spalte3", Language.Mixed), dataType,
                ColumnContext.getDefault(), SSet.of()
        );
        var extraColumn = new ColumnLeaf(new IdSimple(9), name, dataType, ColumnContext.getDefault(), SSet.of());

        var baseTableColumnList = List.of((Column) commonColumn1, commonColumn2, commonColumn3);
        var baseTable = new Table(new IdSimple(10), name, baseTableColumnList, Context.getDefault(), SSet.of(), SSet.of());
        var derivingTableColumnList = List.of(
                (Column) commonColumn1.withId(new IdSimple(4)),
                commonColumn2.withId(new IdSimple(5)),
                commonColumn3.withId(new IdSimple(6)),
                extraColumn);
        var derivingTable = new Table(
                new IdSimple(11), name, derivingTableColumnList, Context.getDefault(), SSet.of(), SSet.of());
        var tableSet = SSet.of(baseTable, derivingTable);
        var schema = new Schema(new IdSimple(15), name, Context.getDefault(), tableSet);
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
                new IdSimple(1), new StringPlusNaked("Spalte1", Language.Mixed), dataType,
                ColumnContext.getDefault(), SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(30)))
        );
        var commonColumn2 = new ColumnLeaf(
                new IdSimple(2), new StringPlusNaked("Spalte2", Language.Mixed), dataType,
                ColumnContext.getDefault(), SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(30)))
        );
        var commonColumn3 = new ColumnLeaf(
                new IdSimple(3), new StringPlusNaked("Spalte3", Language.Mixed), dataType,
                ColumnContext.getDefault(), SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(30)))
        );
        var commonColumn11 = new ColumnLeaf(
                new IdSimple(4), new StringPlusNaked("Spalte1", Language.Mixed), dataType,
                ColumnContext.getDefault(), SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(31)))
        );
        var commonColumn12 = new ColumnLeaf(
                new IdSimple(5), new StringPlusNaked("Spalte2", Language.Mixed), dataType,
                ColumnContext.getDefault(), SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(31)))
        );
        var commonColumn13 = new ColumnLeaf(
                new IdSimple(6), new StringPlusNaked("Spalte3", Language.Mixed), dataType,
                ColumnContext.getDefault(), SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(31)))
        );
        var extraColumn1 = new ColumnLeaf(
                new IdSimple(7), new StringPlusNaked("Spalte4", Language.Mixed), dataType,
                ColumnContext.getDefault(), SSet.of()
        );
        var extraColumn2 = new ColumnLeaf(
                new IdSimple(8), new StringPlusNaked("Spalte5", Language.Mixed), dataType,
                ColumnContext.getDefault(), SSet.of()
        );
        var extraColumn3 = new ColumnLeaf(
                new IdSimple(9), new StringPlusNaked("Spalte6", Language.Mixed), dataType,
                ColumnContext.getDefault(), SSet.of()
        );
        var extraColumn4 = new ColumnLeaf(new IdSimple(10), name, dataType, ColumnContext.getDefault(), SSet.of());

        var baseTableColumnList = List.of((Column) commonColumn1, commonColumn2, commonColumn3);
        var baseTable = new Table(new IdSimple(20), name, baseTableColumnList, Context.getDefault(), SSet.of(), SSet.of());
        var derivingTableColumnList = List.of(
                (Column) commonColumn11,
                commonColumn12,
                commonColumn13,
                extraColumn1,
                extraColumn2,
                extraColumn3,
                extraColumn4);
        var derivingTable = new Table(
                new IdSimple(21), name, derivingTableColumnList, Context.getDefault(), SSet.of(), SSet.of());
        var tableSet = SSet.of(baseTable, derivingTable);
        var schema = new Schema(new IdSimple(100), name, Context.getDefault(), tableSet);
        var transformation = new HorizontalInheritanceToNullable(primaryKeyCountThreshold, 1.01);

        // --- Act
        var isExecutable = transformation.isExecutable(schema);

        // --- Assert
        Assertions.assertEquals(primaryKeyCountThreshold <= 3, isExecutable);
    }
}