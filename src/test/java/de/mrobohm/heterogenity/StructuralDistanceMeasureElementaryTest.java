package de.mrobohm.heterogenity;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.dataset.Value;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.primitives.StringPlusSemantical;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.transformations.structural.BinaryValueToTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.spi.LocaleNameProvider;

import static org.junit.jupiter.api.Assertions.*;

class StructuralDistanceMeasureElementaryTest {

    @Test
    void calculateDistanceToRootAbsolute() {
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
        var newSchema = transformation.transform(rootSchema, new Random());

        // --- act
        var diff = StructuralDistanceMeasureElementary.calculateDistanceToRootAbsolute(rootSchema, newSchema);

        // --- assert
        Assertions.assertTrue(diff > 0);
    }
}