package de.mrobohm.heterogenity;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.dataset.Value;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.heterogenity.ted.Ted;
import de.mrobohm.processing.integrity.IntegrityChecker;
import de.mrobohm.processing.transformations.SingleTransformationExecuter;
import de.mrobohm.processing.transformations.exceptions.NoColumnFoundException;
import de.mrobohm.processing.transformations.exceptions.NoTableFoundException;
import de.mrobohm.processing.transformations.structural.*;
import de.mrobohm.utils.SSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

class StructuralDistanceMeasureTest {

    private String appendSpacesToName(String name) {
        var spaces = "                             ";
        return name + spaces.substring(name.length());
    }

    @ParameterizedTest
    @MethodSource("argSource")
    void calculateDistanceToRootAbsolute_BinaryValueToTable(int seed) throws NoTableFoundException, NoColumnFoundException, IOException {
        // --- arrange
        var name = new StringPlusNaked("name", Language.Technical);
        var dt = new DataType(DataTypeEnum.NVARCHAR, false);
        var vs = SSet.of(new Value("Weiblein"), new Value("Männlein"));
        var column1 = new ColumnLeaf(new IdSimple(101), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column2 = new ColumnLeaf(new IdSimple(102), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column3 = new ColumnLeaf(new IdSimple(103), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column4 = new ColumnLeaf(new IdSimple(104), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var columnList = List.of((Column) column1, column2, column3, column4);
        var table = new Table(new IdSimple(201), name, columnList, Context.getDefault(), SSet.of(), SSet.of());
        var rootSchema = new Schema(new IdSimple(301), name, Context.getDefault(), SSet.of(table));
        var transformation = new BinaryValueToTable();
        var ste = new SingleTransformationExecuter(null);
        var newSchema = ste.executeTransformation(rootSchema, transformation, new Random(seed));

        // --- act
        var diffElementary = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);
        var diffTed = Ted.calculateDistanceAbsolute(rootSchema, newSchema);

        // --- assert
        IntegrityChecker.assertValidSchema(rootSchema);
        IntegrityChecker.assertValidSchema(newSchema);
        System.out.println(appendSpacesToName("BinaryValueToTable:") + diffElementary + "\t" + diffTed);
        Assertions.assertTrue(diffElementary > 0);
    }


    public static Stream<Arguments> argSource(){
        return Stream.of(Arguments.of(new Random().nextInt()));
//        return Stream.iterate(0, i -> i + 1).map(Arguments::of).limit(10000);
    }

    @ParameterizedTest
    @MethodSource("argSource")
    void calculateDistanceToRootAbsolute_RemoveColumn(int seed) throws NoTableFoundException, NoColumnFoundException, IOException {
        // --- arrange
        var name = new StringPlusNaked("name", Language.Technical);
        var dt = new DataType(DataTypeEnum.NVARCHAR, false);
        var vs = SSet.of(new Value("Weiblein"), new Value("Männlein"));
        var column1 = new ColumnLeaf(new IdSimple(101), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column2 = new ColumnLeaf(new IdSimple(102), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column3 = new ColumnLeaf(new IdSimple(103), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column4 = new ColumnLeaf(new IdSimple(104), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var columnList = List.of((Column) column1, column2, column3, column4);
        var table = new Table(new IdSimple(201), name, columnList, Context.getDefault(), SSet.of(), SSet.of());
        var rootSchema = new Schema(new IdSimple(301), name, Context.getDefault(), SSet.of(table));
        var transformation = new RemoveColumn();
        var ste = new SingleTransformationExecuter(null);
        var newSchema = ste.executeTransformation(rootSchema, transformation, new Random(seed));

        // --- act
        var diffElementary = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);
        var diffTed = Ted.calculateDistanceAbsolute(rootSchema, newSchema);

        // --- assert
        IntegrityChecker.assertValidSchema(rootSchema);
        IntegrityChecker.assertValidSchema(newSchema);
        System.out.println(appendSpacesToName("RemoveColumn:") + diffElementary + "\t" + diffTed);
        Assertions.assertTrue(diffElementary > 0);
    }

    @ParameterizedTest
    @MethodSource("argSource")
    void calculateDistanceToRootAbsolute_RemoveTable(int seed) throws NoTableFoundException, NoColumnFoundException, IOException {
        // --- arrange
        var name = new StringPlusNaked("name", Language.Technical);
        var dt = new DataType(DataTypeEnum.NVARCHAR, false);
        var vs = SSet.of(new Value("Weiblein"), new Value("Männlein"));
        var column1 = new ColumnLeaf(new IdSimple(101), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column2 = new ColumnLeaf(new IdSimple(102), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column3 = new ColumnLeaf(new IdSimple(103), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column4 = new ColumnLeaf(new IdSimple(104), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var columnList = List.of((Column) column1, column2, column3, column4);
        var table = new Table(new IdSimple(201), name, columnList, Context.getDefault(), SSet.of(), SSet.of());
        var rootSchema = new Schema(new IdSimple(301), name, Context.getDefault(), SSet.of(table));
        var transformation = new RemoveTable();
        var ste = new SingleTransformationExecuter(null);
        var newSchema = ste.executeTransformation(rootSchema, transformation, new Random(seed));

        // --- act
        var diffElementary = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);
        var diffTed = Ted.calculateDistanceAbsolute(rootSchema, newSchema);

        // --- assert
        IntegrityChecker.assertValidSchema(rootSchema);
        IntegrityChecker.assertValidSchema(newSchema);
        System.out.println(appendSpacesToName("RemoveTable:") + diffElementary + "\t" + diffTed);
        Assertions.assertTrue(diffElementary > 0);
    }

    @Test
    void calculateDistanceToRootAbsolute_Manual_Grouping() throws IOException {
        // --- arrange
        var name = new StringPlusNaked("name", Language.Technical);
        var dt = new DataType(DataTypeEnum.NVARCHAR, false);
        var vs = SSet.of(new Value("Weiblein"), new Value("Männlein"));
        var column1 = new ColumnLeaf(new IdSimple(101), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column2 = new ColumnLeaf(new IdSimple(102), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column3 = new ColumnLeaf(new IdSimple(103), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column4 = new ColumnLeaf(new IdSimple(104), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var columnList = List.of((Column) column1, column2, column3, column4);
        var columnNode = new ColumnNode(new IdSimple(105), name, columnList, SSet.of(), false);
        var rootTable = new Table(new IdSimple(201), name, columnList, Context.getDefault(), SSet.of(), SSet.of());
        var rootSchema = new Schema(new IdSimple(301), name, Context.getDefault(), SSet.of(rootTable));
        var newTable = rootTable.withColumnList(List.of(columnNode));
        var newSchema = rootSchema.withTables(SSet.of(newTable));
        // --- act
        var diffElementary = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);
        var diffTed = Ted.calculateDistanceAbsolute(rootSchema, newSchema);

        // --- assert
        System.out.println(appendSpacesToName("Manual_Grouping:") + diffElementary + "\t" + diffTed);
        Assertions.assertTrue(diffElementary > 0);
    }

    @Test
    void calculateDistanceToRootAbsolute_Manual_ChangeGrouping() throws IOException {
        // --- arrange
        var name = new StringPlusNaked("name", Language.Technical);
        var dt = new DataType(DataTypeEnum.NVARCHAR, false);
        var vs = SSet.of(new Value("Weiblein"), new Value("Männlein"));
        var column1 = new ColumnLeaf(new IdSimple(101), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column2 = new ColumnLeaf(new IdSimple(102), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column3 = new ColumnLeaf(new IdSimple(103), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column4 = new ColumnLeaf(new IdSimple(104), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var columnNode = new ColumnNode(new IdSimple(105), name, List.of(column4), SSet.of(), false);
        var columnList = List.of((Column) column1, column2, column3, columnNode);
        var rootTable = new Table(new IdSimple(201), name, columnList, Context.getDefault(), SSet.of(), SSet.of());
        var rootSchema = new Schema(new IdSimple(301), name, Context.getDefault(), SSet.of(rootTable));
        var newColumnNode = columnNode.withColumnList(List.of(column3, column4));
        var newColumnList = List.of((Column) column1, column2, newColumnNode);
        var newTable = rootTable.withColumnList(newColumnList);
        var newSchema = rootSchema.withTables(SSet.of(newTable));
        // --- act
        var diffElementary = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);
        var diffTed = Ted.calculateDistanceAbsolute(rootSchema, newSchema);

        // --- assert
        System.out.println(appendSpacesToName("Manual_ChangeGrouping:") + diffElementary + "\t" + diffTed);
        Assertions.assertTrue(diffElementary > 0);
    }

    @ParameterizedTest
    @MethodSource("argSource")
    void calculateDistanceToRootAbsolute_ColumnCollectionToTable(int seed) throws NoTableFoundException, NoColumnFoundException, IOException {
        // --- arrange
        var name = new StringPlusNaked("name", Language.Technical);
        var dt = new DataType(DataTypeEnum.NVARCHAR, false);
        var vs = SSet.of(new Value("Weiblein"), new Value("Männlein"));
        var column1 = new ColumnLeaf(new IdSimple(101), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column2 = new ColumnLeaf(new IdSimple(102), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column3 = new ColumnLeaf(new IdSimple(103), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column4 = new ColumnLeaf(new IdSimple(104), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var columnCol = new ColumnCollection(new IdSimple(105), name, List.of(column3, column4), SSet.of(), false);
        var columnList = List.of((Column) column1, column2, columnCol);
        var rootTable = new Table(new IdSimple(201), name, columnList, Context.getDefault(), SSet.of(), SSet.of());
        var rootSchema = new Schema(new IdSimple(301), name, Context.getDefault(), SSet.of(rootTable));
        var transformation = new ColumnCollectionToTable();
        var ste = new SingleTransformationExecuter(null);
        var newSchema = ste.executeTransformation(rootSchema, transformation, new Random(seed));
        IntegrityChecker.assertValidSchema(rootSchema);
        IntegrityChecker.assertValidSchema(newSchema);

        // --- act
        var diffElementary = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);
        var diffTed = Ted.calculateDistanceAbsolute(rootSchema, newSchema);

        // --- assert
        IntegrityChecker.assertValidSchema(rootSchema);
        IntegrityChecker.assertValidSchema(newSchema);
        System.out.println(appendSpacesToName("ColumnCollectionToTable:") + diffElementary + "\t" + diffTed);
        Assertions.assertTrue(diffElementary > 0);
    }

    @ParameterizedTest
    @MethodSource("argSource")
    void calculateDistanceToRootAbsolute_GroupColumnLeafsToNode(int seed) throws NoTableFoundException, NoColumnFoundException, IOException {
        // --- arrange
        var name = new StringPlusNaked("name", Language.Technical);
        var dt = new DataType(DataTypeEnum.NVARCHAR, false);
        var vs = SSet.of(new Value("Weiblein"), new Value("Männlein"));
        var column1 = new ColumnLeaf(new IdSimple(101), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column2 = new ColumnLeaf(new IdSimple(102), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column3 = new ColumnLeaf(new IdSimple(103), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column4 = new ColumnLeaf(new IdSimple(104), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var columnList = List.of((Column) column1, column2, column3, column4);
        var rootTable = new Table(new IdSimple(201), name, columnList, Context.getDefault(), SSet.of(), SSet.of());
        var rootSchema = new Schema(new IdSimple(301), name, Context.getDefault(), SSet.of(rootTable));
        var transformation = new GroupColumnLeafsToNode();
        var ste = new SingleTransformationExecuter(null);
        var newSchema = ste.executeTransformation(rootSchema, transformation, new Random(seed));

        // --- act
        var diffElementary = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);
        var diffTed = Ted.calculateDistanceAbsolute(rootSchema, newSchema);

        // --- assert
        IntegrityChecker.assertValidSchema(rootSchema);
        IntegrityChecker.assertValidSchema(newSchema);
        System.out.println(appendSpacesToName("GroupColumnLeafsToNode:") + diffElementary + "\t" + diffTed);
        Assertions.assertTrue(5 >= diffElementary);
        Assertions.assertTrue(diffElementary >= 2);
    }

    @ParameterizedTest
    @MethodSource("argSource")
    void calculateDistanceToRootAbsolute_MergeColumns(int seed) throws NoTableFoundException, NoColumnFoundException, IOException {
        // --- arrange
        var name = new StringPlusNaked("name", Language.Technical);
        var dt = new DataType(DataTypeEnum.NVARCHAR, false);
        var vs = SSet.of(new Value("Weiblein"), new Value("Männlein"));
        var column1 = new ColumnLeaf(new IdSimple(101), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column2 = new ColumnLeaf(new IdSimple(102), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column3 = new ColumnLeaf(new IdSimple(103), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var column4 = new ColumnLeaf(new IdSimple(104), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        var columnList = List.of((Column) column1, column2, column3, column4);
        var rootTable = new Table(new IdSimple(201), name, columnList, Context.getDefault(), SSet.of(), SSet.of());
        var rootSchema = new Schema(new IdSimple(301), name, Context.getDefault(), SSet.of(rootTable));
        var transformation = new MergeColumns(true);
        var ste = new SingleTransformationExecuter(null);
        var newSchema = ste.executeTransformation(rootSchema, transformation, new Random(seed));

        // --- act
        var diffElementary = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);
        var diffTed = Ted.calculateDistanceAbsolute(rootSchema, newSchema);

        // --- assert
        IntegrityChecker.assertValidSchema(rootSchema);
        IntegrityChecker.assertValidSchema(newSchema);
        System.out.println(appendSpacesToName("MergeColumns:") + diffElementary + "\t" + diffTed);
        Assertions.assertTrue(diffElementary > 0);
    }

    @ParameterizedTest
    @MethodSource("argSource")
    void calculateDistanceToRootAbsolute_TableToColumnCollection(int seed) throws NoTableFoundException, NoColumnFoundException, IOException {
        // --- arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var column1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(), SSet.of());
        var ingestedColumn = new ColumnLeaf(new IdSimple(2), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(), SSet.of(new ColumnConstraintForeignKey(new IdSimple(4), SSet.of())));
        var ingestingColumn = new ColumnLeaf(new IdSimple(4), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(2), SSet.of()))
        );
        var ingestingTable = new Table(
                new IdSimple(10), name, List.of(ingestingColumn), Context.getDefault(), SSet.of(), SSet.of());
        var ingestedTable = new Table(
                new IdSimple(11), name, List.of(column1, ingestedColumn), Context.getDefault(), SSet.of(), SSet.of());
        var tableSet = SSet.of(ingestingTable, ingestedTable);
        var rootSchema = new Schema(new IdSimple(15), name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(rootSchema);
        var transformation = new TableToColumnCollection(false);
        var ste = new SingleTransformationExecuter(null);
        var newSchema = ste.executeTransformation(rootSchema, transformation, new Random(seed));

        // --- act
        var diffElementary = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);
        var diffTed = Ted.calculateDistanceAbsolute(rootSchema, newSchema);

        // --- assert
        IntegrityChecker.assertValidSchema(rootSchema);
        IntegrityChecker.assertValidSchema(newSchema);
        System.out.println(appendSpacesToName("TableToColumnCollection:") + diffElementary + "\t" + diffTed);
        Assertions.assertTrue(diffElementary > 0);
    }
}