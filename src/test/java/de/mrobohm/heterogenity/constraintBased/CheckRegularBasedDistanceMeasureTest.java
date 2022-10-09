package de.mrobohm.heterogenity.constraintBased;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.constraint.ColumnConstraintCheckRegex;
import de.mrobohm.data.column.constraint.regexy.RegularExpression;
import de.mrobohm.data.column.constraint.regexy.RegularKleene;
import de.mrobohm.data.column.constraint.regexy.RegularSum;
import de.mrobohm.data.column.constraint.regexy.RegularTerminal;
import de.mrobohm.data.column.context.ColumnContext;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.utils.SSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

class CheckRegularBasedDistanceMeasureTest {

    Schema regexToSchema(RegularExpression regex) {
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var cccr = new ColumnConstraintCheckRegex(regex);
        final var column = new ColumnLeaf(
                new IdSimple(1), name, dataType, SSet.of(), ColumnContext.getDefault(), SSet.of(cccr)
        );
        final var table = new Table(new IdSimple(0), name, List.of(column), Context.getDefault(), SSet.of(), SSet.of());
        return new Schema(new IdSimple(2), name, Context.getDefault(), SSet.of(table));
    }

    @Test
    void calculateDistanceAbsoluteEquivalence() {
        // --- Arrange
        final var regex1 = new RegularSum(
                new RegularKleene(new RegularTerminal('b')),
                new RegularKleene(new RegularTerminal('c'))
        );
        final var schema1 = regexToSchema(regex1);
        final var schema2 = regexToSchema(regex1);

        // --- Act
        final var diff = CheckRegularBasedDistanceMeasure.calculateDistanceAbsolute(schema1, schema2, new Random());

        // --- Assert
        System.out.println("Equivalence = " + diff);
    }

    @Test
    void calculateDistanceAbsoluteMedium() {
        // --- Arrange
        final var regex1 = new RegularSum(
                new RegularKleene(new RegularTerminal('b')),
                new RegularKleene(new RegularTerminal('c'))
        );
        final var schema1 = regexToSchema(regex1);
        final var regex2 = new RegularKleene(new RegularTerminal('c'));
        final var schema2 = regexToSchema(regex2);

        // --- Act
        final var diff = CheckRegularBasedDistanceMeasure.calculateDistanceAbsolute(schema1, schema2, new Random());

        // --- Assert
        System.out.println("Medium = " + diff);
    }
}