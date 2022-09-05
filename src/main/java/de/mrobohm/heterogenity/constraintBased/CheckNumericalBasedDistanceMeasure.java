package de.mrobohm.heterogenity.constraintBased;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.constraint.ColumnConstraintCheckNumerical;
import de.mrobohm.data.column.constraint.numerical.CheckConjunction;
import de.mrobohm.data.column.constraint.numerical.CheckExpression;
import de.mrobohm.data.column.context.ColumnContext;
import de.mrobohm.data.column.context.NumericalDistribution;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.*;
import de.mrobohm.heterogenity.constraintBased.numerical.CheckExpressionEvaluation;
import de.mrobohm.heterogenity.constraintBased.numerical.Lagrange;
import de.mrobohm.processing.integrity.IdentificationNumberCalculator;
import de.mrobohm.processing.transformations.constraintBased.base.CheckNumericalManager;
import de.mrobohm.processing.transformations.constraintBased.base.StepIntervall;
import de.mrobohm.utils.Pair;
import de.mrobohm.utils.SSet;
import de.mrobohm.utils.StreamExtensions;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class CheckNumericalBasedDistanceMeasure {

    private static final int SAMPLE_SIZE = 1_000_000;

    private CheckNumericalBasedDistanceMeasure() {
    }

    public static double calculateDistanceRelative(
            Schema schema1, Schema schema2
    ) {
        var distanceAbsolute = calculateDistanceAbsolute(schema1, schema2);
        var schema1Size = IdentificationNumberCalculator.getAllIds(schema1, false).count();
        var schema2Size = IdentificationNumberCalculator.getAllIds(schema2, false).count();
        return (2.0 * distanceAbsolute) / (double) (schema1Size + schema2Size);
    }

    public static double calculateDistanceAbsolute(
            Schema schema1, Schema schema2
    ) {
        return aggregate(findCorrespondingTablePairs(schema1, schema2))
                .entrySet().stream()
                .mapToDouble(entry -> diffOfColumns(entry.getKey(), entry.getValue()))
                .sum();
    }

    private static Stream<Pair<Column, Column>> findCorrespondingTablePairs(Schema schema1, Schema schema2) {
        return BasedConstraintBasedBase
                .findCorrespondingEntityPairs(
                        extractRelevantColumns(schema1),
                        extractRelevantColumns(schema2)
                )
                .filter(pair -> isValidSplitMergeId(pair.first().id())
                        && isValidSplitMergeId(pair.second().id()));
    }

    private static Stream<Column> extractRelevantColumns(Schema schema) {
        return schema.tableSet().stream()
                .flatMap(t -> t.columnList().stream())
                .filter(column -> column.containsConstraint(ColumnConstraintCheckNumerical.class));
    }

    private static boolean isValidSplitMergeId(Id id) {
        return switch (id) {
            case IdSimple ignore -> true;
            case IdMerge idm -> idm.mergeType().equals(MergeOrSplitType.Xor);
            case IdPart idp -> idp.splitType().equals(MergeOrSplitType.Xor);
        };
    }

    private static Map<Column, SortedSet<Column>> aggregate(Stream<Pair<Column, Column>> correspondence) {
        var correspondenceList = correspondence.toList();
        var allColumnList = Stream
                .concat(
                        correspondenceList.stream().map(Pair::first),
                        correspondenceList.stream().map(Pair::second)
                )
                .collect(Collectors.toSet());
        return allColumnList.stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        column -> correspondenceList.stream()
                                .filter(corr -> Set.of(corr.first().id(), corr.second().id()).contains(column.id()))
                                .flatMap(corr -> Stream.of(corr.first(), corr.second()))
                                .collect(Collectors.toCollection(TreeSet::new)))
                );
    }

    private static double diffOfColumns(Column column, SortedSet<Column> columnSet) {
        if (!(column instanceof ColumnLeaf)) {
            return 0.0;
        }
        var columnLeafSet = columnSet.stream()
                .filter(col -> col instanceof ColumnLeaf)
                .map(col -> (ColumnLeaf) col)
                .collect(Collectors.toCollection(TreeSet::new));
        if (columnLeafSet.isEmpty()) {
            return 0.0;
        }

        var checkExpression1 = new CheckConjunction(columnToCheckExpression(column));
        var checkExpression2 = new CheckConjunction(
                columnLeafSet.stream()
                        .map(CheckNumericalBasedDistanceMeasure::columnToCheckExpression)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toCollection(TreeSet::new))
        );

        var ndOpt = StreamExtensions
                .prepend(
                        columnLeafSet.stream().map(ColumnLeaf::context).map(ColumnContext::numericalDistribution),
                        ((ColumnLeaf) column).context().numericalDistribution()
                )
                .reduce(CheckNumericalManager::merge);
        if (ndOpt.isEmpty()) {
            return 0.0;
        }

        return diffOfCheckExpressions(checkExpression1, checkExpression2, ndOpt.get());
    }

    private static SortedSet<CheckExpression> columnToCheckExpression(Column column) {
        return column.constraintSet().stream()
                .filter(c -> c instanceof ColumnConstraintCheckNumerical)
                .map(c -> (ColumnConstraintCheckNumerical) c)
                .map(ColumnConstraintCheckNumerical::checkExpression)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private static double diffOfCheckExpressions(
            CheckExpression checkExpression1, CheckExpression checkExpression2, NumericalDistribution nd
    ) {
        var partialDistributionFunction = new Lagrange.PartialFunction(nd);
        var fullDistributionFunction = Lagrange.polynomize(partialDistributionFunction);

        var testValueSet = generateTestValues(nd);
        var validSet1 = testValueSet.parallelStream()
                .filter(testValue -> CheckExpressionEvaluation.evaluate(checkExpression1, testValue))
                .collect(Collectors.toSet());
        var validSet2 = testValueSet.parallelStream()
                .filter(testValue -> CheckExpressionEvaluation.evaluate(checkExpression2, testValue))
                .collect(Collectors.toSet());
        var validSet12 = validSet1.stream().filter(validSet2::contains).collect(Collectors.toSet());
        var fullDistributionFunctionMemo = SSet
                .concat(validSet1, validSet2).stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        fullDistributionFunction
                ));

        var weight12 = validSet12.stream().mapToDouble(fullDistributionFunctionMemo::get).sum();
        var weightUnion = fullDistributionFunctionMemo.values().parallelStream().mapToDouble(x -> x).sum();
        return weight12 / weightUnion;
    }

    private static SortedSet<Double> generateTestValues(NumericalDistribution nd) {
        var extremePair = StepIntervall.extremes(nd);
        var min = extremePair.first();
        var max = extremePair.second();
        var length = max - min;
        var extendedMin = min - length;
        var distBetweenTestValues = (length * 3.0 / (SAMPLE_SIZE - 1));
        return Stream
                .iterate(extendedMin, v -> v + distBetweenTestValues)
                .limit(SAMPLE_SIZE)
                .collect(Collectors.toCollection(TreeSet::new));
    }
}