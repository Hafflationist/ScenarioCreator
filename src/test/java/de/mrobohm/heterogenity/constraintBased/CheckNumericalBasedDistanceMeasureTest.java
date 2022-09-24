package de.mrobohm.heterogenity.constraintBased;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.constraint.ColumnConstraintCheckNumerical;
import de.mrobohm.data.column.constraint.numerical.CheckConjunction;
import de.mrobohm.data.column.constraint.numerical.CheckPrimitive;
import de.mrobohm.data.column.constraint.numerical.ComparisonType;
import de.mrobohm.data.column.context.ColumnContext;
import de.mrobohm.data.column.context.NumericalDistribution;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.integrity.IntegrityChecker;
import de.mrobohm.utils.SSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class CheckNumericalBasedDistanceMeasureTest {

    @Test
    void calculateDistanceAbsolute() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.FLOAT64, false);
        final var nd = new NumericalDistribution(1.0, Map.of(
                1, 1.0,
                2, 1.0,
                3, 1.0,
                4, 1.0,
                5, 2.0,
                6, 2.0,
                7, 2.0,
                8, 3.0,
                9, 3.0
        ));
        final var cccna = new ColumnConstraintCheckNumerical(new CheckConjunction(SSet.of(
                new CheckPrimitive(ComparisonType.GreaterEquals, 0.0),
                new CheckPrimitive(ComparisonType.LowerEquals, 9.0)
        )));
        final var cccnb = new ColumnConstraintCheckNumerical(new CheckConjunction(SSet.of(
                new CheckPrimitive(ComparisonType.LowerEquals, 9.0)
        )));
        final var cccnc = new ColumnConstraintCheckNumerical(new CheckConjunction(SSet.of(
                new CheckPrimitive(ComparisonType.GreaterEquals, 0.0),
                new CheckPrimitive(ComparisonType.LowerEquals, 5.0)
        )));

        final var column1A = new ColumnLeaf(
                new IdSimple(1), name, dataType, ColumnContext.getDefaultWithNd(nd), SSet.of(cccna)
        );
        final var column1B = new ColumnLeaf(
                new IdSimple(1), name, dataType, ColumnContext.getDefaultWithNd(nd), SSet.of(cccnb)
        );
        final var column1C = new ColumnLeaf(
                new IdSimple(1), name, dataType, ColumnContext.getDefaultWithNd(nd), SSet.of(cccnc)
        );
        final var column2 = new ColumnLeaf(
                new IdSimple(2), name, dataType, ColumnContext.getDefault(), SSet.of()
        );

        final var tableA = new Table(
                new IdSimple(101), name, List.of(column1A, column2), Context.getDefault(), SSet.of(), SSet.of()
        );

        final var tableB = new Table(
                new IdSimple(101), name, List.of(column1B, column2), Context.getDefault(), SSet.of(), SSet.of()
        );

        final var tableC = new Table(
                new IdSimple(101), name, List.of(column1C, column2), Context.getDefault(), SSet.of(), SSet.of()
        );
        final var schemaA = new Schema(new IdSimple(1001), name, Context.getDefault(), SSet.of(tableA));
        final var schemaB = new Schema(new IdSimple(1001), name, Context.getDefault(), SSet.of(tableB));
        final var schemaC = new Schema(new IdSimple(1001), name, Context.getDefault(), SSet.of(tableC));
        IntegrityChecker.assertValidSchema(schemaA);
        IntegrityChecker.assertValidSchema(schemaB);
        IntegrityChecker.assertValidSchema(schemaC);

        // --- Act
        final var distAB = CheckNumericalBasedDistanceMeasure.calculateDistanceAbsolute(schemaA, schemaB);
        final var distAC = CheckNumericalBasedDistanceMeasure.calculateDistanceAbsolute(schemaA, schemaC);

        // --- Assert
        System.out.println("distAB: " + distAB + "  | distAC: " + distAC);
        Assertions.assertTrue(distAB < distAC);
    }

    @Test
    void calculateDistanceAbsolute2() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.FLOAT64, false);
        final var nd = new NumericalDistribution(1.0, Map.of(
                1, 1.0,
                2, 1.0,
                3, 1.0,
                4, 1.0,
                5, 2.0,
                6, 2.0,
                7, 2.0,
                8, 3.0,
                9, 3.0
        ));
        final var cccna = new ColumnConstraintCheckNumerical(new CheckConjunction(SSet.of(
                new CheckPrimitive(ComparisonType.GreaterEquals, 0.0),
                new CheckPrimitive(ComparisonType.LowerEquals, 9.0)
        )));
        final var cccnb = new ColumnConstraintCheckNumerical(new CheckConjunction(SSet.of(
                new CheckPrimitive(ComparisonType.GreaterEquals, 2.0),
                new CheckPrimitive(ComparisonType.LowerEquals, 9.0)
        )));
        final var cccnc = new ColumnConstraintCheckNumerical(new CheckConjunction(SSet.of(
                new CheckPrimitive(ComparisonType.GreaterEquals, 0.0),
                new CheckPrimitive(ComparisonType.LowerEquals, 7.0)
        )));

        final var column1A = new ColumnLeaf(
                new IdSimple(1), name, dataType, ColumnContext.getDefaultWithNd(nd), SSet.of(cccna)
        );
        final var column1B = new ColumnLeaf(
                new IdSimple(1), name, dataType, ColumnContext.getDefaultWithNd(nd), SSet.of(cccnb)
        );
        final var column1C = new ColumnLeaf(
                new IdSimple(1), name, dataType, ColumnContext.getDefaultWithNd(nd), SSet.of(cccnc)
        );
        final var column2 = new ColumnLeaf(
                new IdSimple(2), name, dataType, ColumnContext.getDefault(), SSet.of()
        );

        final var tableA = new Table(
                new IdSimple(101), name, List.of(column1A, column2), Context.getDefault(), SSet.of(), SSet.of()
        );

        final var tableB = new Table(
                new IdSimple(101), name, List.of(column1B, column2), Context.getDefault(), SSet.of(), SSet.of()
        );

        final var tableC = new Table(
                new IdSimple(101), name, List.of(column1C, column2), Context.getDefault(), SSet.of(), SSet.of()
        );
        final var schemaA = new Schema(new IdSimple(1001), name, Context.getDefault(), SSet.of(tableA));
        final var schemaB = new Schema(new IdSimple(1001), name, Context.getDefault(), SSet.of(tableB));
        final var schemaC = new Schema(new IdSimple(1001), name, Context.getDefault(), SSet.of(tableC));
        IntegrityChecker.assertValidSchema(schemaA);
        IntegrityChecker.assertValidSchema(schemaB);
        IntegrityChecker.assertValidSchema(schemaC);

        // --- Act
        final var distAB = CheckNumericalBasedDistanceMeasure.calculateDistanceAbsolute(schemaA, schemaB);
        final var distAC = CheckNumericalBasedDistanceMeasure.calculateDistanceAbsolute(schemaA, schemaC);

        // --- Assert
        System.out.println("distAB: " + distAB + "  | distAC: " + distAC);
        Assertions.assertTrue(distAB < distAC);
    }
}