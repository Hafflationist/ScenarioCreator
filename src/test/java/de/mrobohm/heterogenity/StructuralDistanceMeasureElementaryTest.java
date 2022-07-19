package de.mrobohm.heterogenity;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.dataset.Value;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.primitives.StringPlusSemantical;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.preprocessing.SemanticSaturation;
import de.mrobohm.processing.transformations.SingleTransformationExecuter;
import de.mrobohm.processing.transformations.exceptions.NoColumnFoundException;
import de.mrobohm.processing.transformations.exceptions.NoTableFoundException;
import de.mrobohm.processing.transformations.structural.BinaryValueToTable;
import de.mrobohm.processing.transformations.structural.RemoveColumn;
import de.mrobohm.processing.transformations.structural.RemoveTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.spi.LocaleNameProvider;

import static org.junit.jupiter.api.Assertions.*;

class StructuralDistanceMeasureElementaryTest {

    private String appendSpacesToName(String name) {
        var spaces = "                        ";
        return name + spaces.substring(name.length());
    }

    @Test
    void calculateDistanceToRootAbsolute_BinaryValueToTable() throws NoTableFoundException, NoColumnFoundException {
        // --- arrange
        var name = new StringPlusNaked("name", Language.Technical);
        var dt = new DataType(DataTypeEnum.NVARCHAR, false);
        var vs = Set.of(new Value("Weiblein"), new Value("Männlein"));
        var column1 = new ColumnLeaf(new IdSimple(101), name, dt, vs, ColumnContext.getDefault(), Set.of());
        var column2 = new ColumnLeaf(new IdSimple(102), name, dt, vs, ColumnContext.getDefault(), Set.of());
        var column3 = new ColumnLeaf(new IdSimple(103), name, dt, vs, ColumnContext.getDefault(), Set.of());
        var column4 = new ColumnLeaf(new IdSimple(104), name, dt, vs, ColumnContext.getDefault(), Set.of());
        var columnList = List.of((Column) column1, column2, column3, column4);
        var table = new Table(new IdSimple(201), name, columnList, Context.getDefault(), Set.of());
        var rootSchema = new Schema(new IdSimple(301), name, Context.getDefault(), Set.of(table));
        var transformation = new BinaryValueToTable();
        var ste = new SingleTransformationExecuter(null);
        var newSchema = ste.executeTransformation(rootSchema, transformation, new Random());

        // --- act
        var diff = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);

        // --- assert
        System.out.println(appendSpacesToName("BinaryValueToTable:") + diff);
        Assertions.assertTrue(diff > 0);
    }

    @Test
    void calculateDistanceToRootAbsolute_RemoveColumn() throws NoTableFoundException, NoColumnFoundException {
        // --- arrange
        var name = new StringPlusNaked("name", Language.Technical);
        var dt = new DataType(DataTypeEnum.NVARCHAR, false);
        var vs = Set.of(new Value("Weiblein"), new Value("Männlein"));
        var column1 = new ColumnLeaf(new IdSimple(101), name, dt, vs, ColumnContext.getDefault(), Set.of());
        var column2 = new ColumnLeaf(new IdSimple(102), name, dt, vs, ColumnContext.getDefault(), Set.of());
        var column3 = new ColumnLeaf(new IdSimple(103), name, dt, vs, ColumnContext.getDefault(), Set.of());
        var column4 = new ColumnLeaf(new IdSimple(104), name, dt, vs, ColumnContext.getDefault(), Set.of());
        var columnList = List.of((Column) column1, column2, column3, column4);
        var table = new Table(new IdSimple(201), name, columnList, Context.getDefault(), Set.of());
        var rootSchema = new Schema(new IdSimple(301), name, Context.getDefault(), Set.of(table));
        var transformation = new RemoveColumn();
        var ste = new SingleTransformationExecuter(null);
        var newSchema = ste.executeTransformation(rootSchema, transformation, new Random());

        // --- act
        var diff = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);

        // --- assert
        System.out.println(appendSpacesToName("RemoveColumn:") + diff);
        Assertions.assertTrue(diff > 0);
    }

    @Test
    void calculateDistanceToRootAbsolute_RemoveTable() throws NoTableFoundException, NoColumnFoundException {
        // --- arrange
        var name = new StringPlusNaked("name", Language.Technical);
        var dt = new DataType(DataTypeEnum.NVARCHAR, false);
        var vs = Set.of(new Value("Weiblein"), new Value("Männlein"));
        var column1 = new ColumnLeaf(new IdSimple(101), name, dt, vs, ColumnContext.getDefault(), Set.of());
        var column2 = new ColumnLeaf(new IdSimple(102), name, dt, vs, ColumnContext.getDefault(), Set.of());
        var column3 = new ColumnLeaf(new IdSimple(103), name, dt, vs, ColumnContext.getDefault(), Set.of());
        var column4 = new ColumnLeaf(new IdSimple(104), name, dt, vs, ColumnContext.getDefault(), Set.of());
        var columnList = List.of((Column) column1, column2, column3, column4);
        var table = new Table(new IdSimple(201), name, columnList, Context.getDefault(), Set.of());
        var rootSchema = new Schema(new IdSimple(301), name, Context.getDefault(), Set.of(table));
        var transformation = new RemoveTable();
        var ste = new SingleTransformationExecuter(null);
        var newSchema = ste.executeTransformation(rootSchema, transformation, new Random());

        // --- act
        var diff = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);

        // --- assert
        System.out.println(appendSpacesToName("RemoveTable:") + diff);
        Assertions.assertTrue(diff > 0);
    }

    @Test
    void calculateDistanceToRootAbsolute_Manual_Grouping() {
        // --- arrange
        var name = new StringPlusNaked("name", Language.Technical);
        var dt = new DataType(DataTypeEnum.NVARCHAR, false);
        var vs = Set.of(new Value("Weiblein"), new Value("Männlein"));
        var column1 = new ColumnLeaf(new IdSimple(101), name, dt, vs, ColumnContext.getDefault(), Set.of());
        var column2 = new ColumnLeaf(new IdSimple(102), name, dt, vs, ColumnContext.getDefault(), Set.of());
        var column3 = new ColumnLeaf(new IdSimple(103), name, dt, vs, ColumnContext.getDefault(), Set.of());
        var column4 = new ColumnLeaf(new IdSimple(104), name, dt, vs, ColumnContext.getDefault(), Set.of());
        var columnList = List.of((Column) column1, column2, column3, column4);
        var columnNode = new ColumnNode(new IdSimple(105), name, columnList, Set.of(), false);
        var rootTable = new Table(new IdSimple(201), name, columnList, Context.getDefault(), Set.of());
        var rootSchema = new Schema(new IdSimple(301), name, Context.getDefault(), Set.of(rootTable));
        var newTable = rootTable.withColumnList(List.of(columnNode));
        var newSchema = rootSchema.withTables(Set.of(newTable));
        // --- act
        var diff = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);

        // --- assert
        System.out.println(appendSpacesToName("Manual_Grouping:") + diff);
        Assertions.assertTrue(diff > 0);
    }

    @Test
    void calculateDistanceToRootAbsolute_Manual_ChangeGrouping() {
        // --- arrange
        var name = new StringPlusNaked("name", Language.Technical);
        var dt = new DataType(DataTypeEnum.NVARCHAR, false);
        var vs = Set.of(new Value("Weiblein"), new Value("Männlein"));
        var column1 = new ColumnLeaf(new IdSimple(101), name, dt, vs, ColumnContext.getDefault(), Set.of());
        var column2 = new ColumnLeaf(new IdSimple(102), name, dt, vs, ColumnContext.getDefault(), Set.of());
        var column3 = new ColumnLeaf(new IdSimple(103), name, dt, vs, ColumnContext.getDefault(), Set.of());
        var column4 = new ColumnLeaf(new IdSimple(104), name, dt, vs, ColumnContext.getDefault(), Set.of());
        var columnNode = new ColumnNode(new IdSimple(105), name, List.of(column4), Set.of(), false);
        var columnList = List.of((Column) column1, column2, column3, columnNode);
        var rootTable = new Table(new IdSimple(201), name, columnList, Context.getDefault(), Set.of());
        var rootSchema = new Schema(new IdSimple(301), name, Context.getDefault(), Set.of(rootTable));
        var newColumnNode = columnNode.withColumnList(List.of(column3, column4));
        var newColumnList = List.of((Column) column1, column2, newColumnNode);
        var newTable = rootTable.withColumnList(newColumnList);
        var newSchema = rootSchema.withTables(Set.of(newTable));
        // --- act
        var diff = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);

        // --- assert
        System.out.println(appendSpacesToName("Manual_ChangeGrouping:") + diff);
        Assertions.assertTrue(diff > 0);
    }
}