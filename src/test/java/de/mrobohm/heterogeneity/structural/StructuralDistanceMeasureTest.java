package de.mrobohm.heterogeneity.structural;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.column.context.ColumnContext;
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
import de.mrobohm.heterogeneity.structural.ted.Ted;
import de.mrobohm.processing.integrity.IntegrityChecker;
import de.mrobohm.processing.transformations.SingleTransformationExecutor;
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
        final var spaces = "                             ";
        return name + spaces.substring(name.length());
    }

    @ParameterizedTest
    @MethodSource("argSource")
    void calculateDistanceToRootAbsolute_BinaryValueToTable(int seed) throws NoTableFoundException, NoColumnFoundException, IOException {
        // --- Arrange
        final var name = new StringPlusNaked("name", Language.Technical);
        final var dt = new DataType(DataTypeEnum.NVARCHAR, false);
        final var vs = SSet.of(new Value("Weiblein"), new Value("Männlein"));
        final var column1 = new ColumnLeaf(new IdSimple(101), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column2 = new ColumnLeaf(new IdSimple(102), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column3 = new ColumnLeaf(new IdSimple(103), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column4 = new ColumnLeaf(new IdSimple(104), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var columnList = List.of((Column) column1, column2, column3, column4);
        final var table = new Table(new IdSimple(201), name, columnList, Context.getDefault(), SSet.of(), SSet.of());
        final var rootSchema = new Schema(new IdSimple(301), name, Context.getDefault(), SSet.of(table));
        final var transformation = new BinaryValueToTable();
        final var ste = new SingleTransformationExecutor(null);
        final var newSchema = ste.executeTransformation(rootSchema, transformation, new Random(seed));

        // --- Act
        final var diffElementary = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);
        final var diffTed = Ted.calculateDistanceAbsolute(rootSchema, newSchema);

        // --- Assert
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
        // --- Arrange
        final var name = new StringPlusNaked("name", Language.Technical);
        final var dt = new DataType(DataTypeEnum.NVARCHAR, false);
        final var vs = SSet.of(new Value("Weiblein"), new Value("Männlein"));
        final var column1 = new ColumnLeaf(new IdSimple(101), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column2 = new ColumnLeaf(new IdSimple(102), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column3 = new ColumnLeaf(new IdSimple(103), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column4 = new ColumnLeaf(new IdSimple(104), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var columnList = List.of((Column) column1, column2, column3, column4);
        final var table = new Table(new IdSimple(201), name, columnList, Context.getDefault(), SSet.of(), SSet.of());
        final var rootSchema = new Schema(new IdSimple(301), name, Context.getDefault(), SSet.of(table));
        final var transformation = new RemoveColumn();
        final var ste = new SingleTransformationExecutor(null);
        final var newSchema = ste.executeTransformation(rootSchema, transformation, new Random(seed));

        // --- Act
        final var diffElementary = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);
        final var diffTed = Ted.calculateDistanceAbsolute(rootSchema, newSchema);

        // --- Assert
        IntegrityChecker.assertValidSchema(rootSchema);
        IntegrityChecker.assertValidSchema(newSchema);
        System.out.println(appendSpacesToName("RemoveColumn:") + diffElementary + "\t" + diffTed);
        Assertions.assertTrue(diffElementary > 0);
    }

    @ParameterizedTest
    @MethodSource("argSource")
    void calculateDistanceToRootAbsolute_RemoveTable(int seed) throws NoTableFoundException, NoColumnFoundException, IOException {
        // --- Arrange
        final var name = new StringPlusNaked("name", Language.Technical);
        final var dt = new DataType(DataTypeEnum.NVARCHAR, false);
        final var vs = SSet.of(new Value("Weiblein"), new Value("Männlein"));
        final var column1 = new ColumnLeaf(new IdSimple(101), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column2 = new ColumnLeaf(new IdSimple(102), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column3 = new ColumnLeaf(new IdSimple(103), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column4 = new ColumnLeaf(new IdSimple(104), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var columnList = List.of((Column) column1, column2, column3, column4);
        final var table = new Table(new IdSimple(201), name, columnList, Context.getDefault(), SSet.of(), SSet.of());
        final var rootSchema = new Schema(new IdSimple(301), name, Context.getDefault(), SSet.of(table));
        final var transformation = new RemoveTable();
        final var ste = new SingleTransformationExecutor(null);
        final var newSchema = ste.executeTransformation(rootSchema, transformation, new Random(seed));

        // --- Act
        final var diffElementary = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);
        final var diffTed = Ted.calculateDistanceAbsolute(rootSchema, newSchema);

        // --- Assert
        IntegrityChecker.assertValidSchema(rootSchema);
        IntegrityChecker.assertValidSchema(newSchema);
        System.out.println(appendSpacesToName("RemoveTable:") + diffElementary + "\t" + diffTed);
        Assertions.assertTrue(diffElementary > 0);
    }

    @Test
    void calculateDistanceToRootAbsolute_Manual_Grouping() throws IOException {
        // --- Arrange
        final var name = new StringPlusNaked("name", Language.Technical);
        final var dt = new DataType(DataTypeEnum.NVARCHAR, false);
        final var vs = SSet.of(new Value("Weiblein"), new Value("Männlein"));
        final var column1 = new ColumnLeaf(new IdSimple(101), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column2 = new ColumnLeaf(new IdSimple(102), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column3 = new ColumnLeaf(new IdSimple(103), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column4 = new ColumnLeaf(new IdSimple(104), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var columnList = List.of((Column) column1, column2, column3, column4);
        final var columnNode = new ColumnNode(new IdSimple(105), name, columnList, SSet.of(), false);
        final var rootTable = new Table(new IdSimple(201), name, columnList, Context.getDefault(), SSet.of(), SSet.of());
        final var rootSchema = new Schema(new IdSimple(301), name, Context.getDefault(), SSet.of(rootTable));
        final var newTable = rootTable.withColumnList(List.of(columnNode));
        final var newSchema = rootSchema.withTableSet(SSet.of(newTable));
        // --- Act
        final var diffElementary = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);
        final var diffTed = Ted.calculateDistanceAbsolute(rootSchema, newSchema);

        // --- Assert
        System.out.println(appendSpacesToName("Manual_Grouping:") + diffElementary + "\t" + diffTed);
        Assertions.assertTrue(diffElementary > 0);
    }

    @Test
    void calculateDistanceToRootAbsolute_Manual_ChangeGrouping() throws IOException {
        // --- Arrange
        final var name = new StringPlusNaked("name", Language.Technical);
        final var dt = new DataType(DataTypeEnum.NVARCHAR, false);
        final var vs = SSet.of(new Value("Weiblein"), new Value("Männlein"));
        final var column1 = new ColumnLeaf(new IdSimple(101), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column2 = new ColumnLeaf(new IdSimple(102), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column3 = new ColumnLeaf(new IdSimple(103), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column4 = new ColumnLeaf(new IdSimple(104), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var columnNode = new ColumnNode(new IdSimple(105), name, List.of(column4), SSet.of(), false);
        final var columnList = List.of((Column) column1, column2, column3, columnNode);
        final var rootTable = new Table(new IdSimple(201), name, columnList, Context.getDefault(), SSet.of(), SSet.of());
        final var rootSchema = new Schema(new IdSimple(301), name, Context.getDefault(), SSet.of(rootTable));
        final var newColumnNode = columnNode.withColumnList(List.of(column3, column4));
        final var newColumnList = List.of((Column) column1, column2, newColumnNode);
        final var newTable = rootTable.withColumnList(newColumnList);
        final var newSchema = rootSchema.withTableSet(SSet.of(newTable));
        // --- Act
        final var diffElementary = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);
        final var diffTed = Ted.calculateDistanceAbsolute(rootSchema, newSchema);

        // --- Assert
        System.out.println(appendSpacesToName("Manual_ChangeGrouping:") + diffElementary + "\t" + diffTed);
        Assertions.assertTrue(diffElementary > 0);
    }

    @ParameterizedTest
    @MethodSource("argSource")
    void calculateDistanceToRootAbsolute_ColumnCollectionToTable(int seed) throws NoTableFoundException, NoColumnFoundException, IOException {
        // --- Arrange
        final var name = new StringPlusNaked("name", Language.Technical);
        final var dt = new DataType(DataTypeEnum.NVARCHAR, false);
        final var vs = SSet.of(new Value("Weiblein"), new Value("Männlein"));
        final var column1 = new ColumnLeaf(new IdSimple(101), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column2 = new ColumnLeaf(new IdSimple(102), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column3 = new ColumnLeaf(new IdSimple(103), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column4 = new ColumnLeaf(new IdSimple(104), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var columnCol = new ColumnCollection(new IdSimple(105), name, List.of(column3, column4), SSet.of(), false);
        final var columnList = List.of((Column) column1, column2, columnCol);
        final var rootTable = new Table(new IdSimple(201), name, columnList, Context.getDefault(), SSet.of(), SSet.of());
        final var rootSchema = new Schema(new IdSimple(301), name, Context.getDefault(), SSet.of(rootTable));
        final var transformation = new ColumnCollectionToTable();
        final var ste = new SingleTransformationExecutor(null);
        final var newSchema = ste.executeTransformation(rootSchema, transformation, new Random(seed));
        IntegrityChecker.assertValidSchema(rootSchema);
        IntegrityChecker.assertValidSchema(newSchema);

        // --- Act
        final var diffElementary = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);
        final var diffTed = Ted.calculateDistanceAbsolute(rootSchema, newSchema);

        // --- Assert
        IntegrityChecker.assertValidSchema(rootSchema);
        IntegrityChecker.assertValidSchema(newSchema);
        System.out.println(appendSpacesToName("ColumnCollectionToTable:") + diffElementary + "\t" + diffTed);
        Assertions.assertTrue(diffElementary > 0);
    }

    @ParameterizedTest
    @MethodSource("argSource")
    void calculateDistanceToRootAbsolute_GroupColumnLeafsToNode(int seed) throws NoTableFoundException, NoColumnFoundException, IOException {
        // --- Arrange
        final var name = new StringPlusNaked("name", Language.Technical);
        final var dt = new DataType(DataTypeEnum.NVARCHAR, false);
        final var vs = SSet.of(new Value("Weiblein"), new Value("Männlein"));
        final var column1 = new ColumnLeaf(new IdSimple(101), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column2 = new ColumnLeaf(new IdSimple(102), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column3 = new ColumnLeaf(new IdSimple(103), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column4 = new ColumnLeaf(new IdSimple(104), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var columnList = List.of((Column) column1, column2, column3, column4);
        final var rootTable = new Table(new IdSimple(201), name, columnList, Context.getDefault(), SSet.of(), SSet.of());
        final var rootSchema = new Schema(new IdSimple(301), name, Context.getDefault(), SSet.of(rootTable));
        final var transformation = new GroupColumnLeafsToNode();
        final var ste = new SingleTransformationExecutor(null);
        final var newSchema = ste.executeTransformation(rootSchema, transformation, new Random(seed));

        // --- Act
        final var diffElementary = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);
        final var diffTed = Ted.calculateDistanceAbsolute(rootSchema, newSchema);

        // --- Assert
        IntegrityChecker.assertValidSchema(rootSchema);
        IntegrityChecker.assertValidSchema(newSchema);
        System.out.println(appendSpacesToName("GroupColumnLeafsToNode:") + diffElementary + "\t" + diffTed);
        Assertions.assertTrue(5 >= diffElementary);
        Assertions.assertTrue(diffElementary >= 2);
    }

    @ParameterizedTest
    @MethodSource("argSource")
    void calculateDistanceToRootAbsolute_MergeColumns(int seed) throws NoTableFoundException, NoColumnFoundException, IOException {
        // --- Arrange
        final var name = new StringPlusNaked("name", Language.Technical);
        final var dt = new DataType(DataTypeEnum.NVARCHAR, false);
        final var vs = SSet.of(new Value("Weiblein"), new Value("Männlein"));
        final var column1 = new ColumnLeaf(new IdSimple(101), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column2 = new ColumnLeaf(new IdSimple(102), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column3 = new ColumnLeaf(new IdSimple(103), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var column4 = new ColumnLeaf(new IdSimple(104), name, dt, vs, ColumnContext.getDefault(), SSet.of());
        final var columnList = List.of((Column) column1, column2, column3, column4);
        final var rootTable = new Table(new IdSimple(201), name, columnList, Context.getDefault(), SSet.of(), SSet.of());
        final var rootSchema = new Schema(new IdSimple(301), name, Context.getDefault(), SSet.of(rootTable));
        final var transformation = new MergeColumns(true);
        final var ste = new SingleTransformationExecutor(null);
        final var newSchema = ste.executeTransformation(rootSchema, transformation, new Random(seed));

        // --- Act
        final var diffElementary = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);
        final var diffTed = Ted.calculateDistanceAbsolute(rootSchema, newSchema);

        // --- Assert
        IntegrityChecker.assertValidSchema(rootSchema);
        IntegrityChecker.assertValidSchema(newSchema);
        System.out.println(appendSpacesToName("MergeColumns:") + diffElementary + "\t" + diffTed);
        Assertions.assertTrue(diffElementary > 0);
    }

    @ParameterizedTest
    @MethodSource("argSource")
    void calculateDistanceToRootAbsolute_TableToColumnCollection(int seed) throws NoTableFoundException, NoColumnFoundException, IOException {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var column1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var ingestedColumn = new ColumnLeaf(new IdSimple(2), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(), SSet.of(new ColumnConstraintForeignKey(new IdSimple(4), SSet.of())));
        final var ingestingColumn = new ColumnLeaf(new IdSimple(4), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(new IdSimple(2), SSet.of()))
        );
        final var ingestingTable = new Table(
                new IdSimple(10), name, List.of(ingestingColumn), Context.getDefault(), SSet.of(), SSet.of());
        final var ingestedTable = new Table(
                new IdSimple(11), name, List.of(column1, ingestedColumn), Context.getDefault(), SSet.of(), SSet.of());
        final var tableSet = SSet.of(ingestingTable, ingestedTable);
        final var rootSchema = new Schema(new IdSimple(15), name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(rootSchema);
        final var transformation = new TableToColumnCollection(false);
        final var ste = new SingleTransformationExecutor(null);
        final var newSchema = ste.executeTransformation(rootSchema, transformation, new Random(seed));

        // --- Act
        final var diffElementary = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);
        final var diffTed = Ted.calculateDistanceAbsolute(rootSchema, newSchema);

        // --- Assert
        IntegrityChecker.assertValidSchema(rootSchema);
        IntegrityChecker.assertValidSchema(newSchema);
        System.out.println(appendSpacesToName("TableToColumnCollection:") + diffElementary + "\t" + diffTed);
        Assertions.assertTrue(diffElementary > 0);
    }
}