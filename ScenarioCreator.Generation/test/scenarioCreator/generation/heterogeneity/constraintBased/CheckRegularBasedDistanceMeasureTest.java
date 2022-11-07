package scenarioCreator.generation.heterogeneity.constraintBased;

import org.junit.jupiter.api.Test;
import scenarioCreator.data.Context;
import scenarioCreator.data.Language;
import scenarioCreator.data.Schema;
import scenarioCreator.data.column.DataType;
import scenarioCreator.data.column.DataTypeEnum;
import scenarioCreator.data.column.constraint.ColumnConstraintCheckRegex;
import scenarioCreator.data.column.constraint.regexy.RegularExpression;
import scenarioCreator.data.column.constraint.regexy.RegularKleene;
import scenarioCreator.data.column.constraint.regexy.RegularSum;
import scenarioCreator.data.column.constraint.regexy.RegularTerminal;
import scenarioCreator.data.column.context.ColumnContext;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.identification.IdSimple;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.data.table.Table;
import scenarioCreator.utils.SSet;

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