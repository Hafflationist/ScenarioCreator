package scenarioCreator.generation.heterogeneity.constraintBased;

import scenarioCreator.data.Schema;
import scenarioCreator.data.column.constraint.ColumnConstraintCheckNumerical;
import scenarioCreator.data.column.constraint.numerical.CheckConjunction;
import scenarioCreator.data.column.constraint.numerical.CheckExpression;
import scenarioCreator.data.column.context.ColumnContext;
import scenarioCreator.data.column.context.NumericalDistribution;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.identification.*;
import scenarioCreator.generation.heterogeneity.constraintBased.numerical.CheckExpressionEvaluation;
import scenarioCreator.generation.heterogeneity.constraintBased.numerical.Lagrange;
import scenarioCreator.generation.processing.integrity.IdentificationNumberCalculator;
import scenarioCreator.generation.processing.transformations.constraintBased.base.CheckNumericalManager;
import scenarioCreator.generation.processing.transformations.constraintBased.base.StepIntervall;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.SSet;
import scenarioCreator.utils.StreamExtensions;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class CheckNumericalBasedDistanceMeasure {

    private static final int SAMPLE_SIZE = 100;

    private CheckNumericalBasedDistanceMeasure() {
    }

    public static double calculateDistanceRelative(
            Schema schema1, Schema schema2
    ) {
        final var distanceAbsolute = calculateDistanceAbsolute(schema1, schema2);
        final var schema1Size = IdentificationNumberCalculator.getAllIds(schema1, false).count();
        final var schema2Size = IdentificationNumberCalculator.getAllIds(schema2, false).count();
        return (2.0 * distanceAbsolute) / (double) (schema1Size + schema2Size);
    }

    public static double calculateDistanceAbsolute(
            Schema schema1, Schema schema2
    ) {
        return aggregate(findCorrespondingTablePairs(schema1, schema2)).entrySet().stream()
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
        final var correspondenceList = correspondence.toList();
        final var aggregated1 = aggregate(correspondenceList, Pair::first, Pair::second);
        final var aggregated2 = aggregate(correspondenceList, Pair::second, Pair::first);
        return SSet.concat(aggregated1.keySet(), aggregated2.keySet()).stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        key -> aggregated1.getOrDefault(key, aggregated2.getOrDefault(key, SSet.of()))
                ));
    }

    private static <T> Map<Column, SortedSet<Column>> aggregate(
            List<T> correspondenceList, Function<T, Column> first, Function<T, Column> second
    ) {
        return correspondenceList.stream()
                .map(first)
                .distinct()
                .collect(Collectors.toMap(
                        Function.identity(),
                        column -> correspondenceList.stream()
                                .filter(corr -> column.id().equals(first.apply(corr).id()))
                                .map(second)
                                .collect(Collectors.toCollection(TreeSet::new)))
                );
    }

    private static double diffOfColumns(Column column, SortedSet<Column> columnSet) {
        if (!(column instanceof ColumnLeaf)) {
            return 0.0;
        }
        final var columnLeafSet = columnSet.stream()
                .filter(col -> col instanceof ColumnLeaf)
                .map(col -> (ColumnLeaf) col)
                .collect(Collectors.toCollection(TreeSet::new));
        if (columnLeafSet.isEmpty()) {
            return 0.0;
        }

        final var checkExpression1 = new CheckConjunction(columnToCheckExpression(column));
        final var checkExpression2 = new CheckConjunction(
                columnLeafSet.stream()
                        .map(CheckNumericalBasedDistanceMeasure::columnToCheckExpression)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toCollection(TreeSet::new))
        );

        final var ndOpt = StreamExtensions
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
        final var partialDistributionFunction = new Lagrange.PartialFunction(nd);
        final var fullDistributionFunction = Lagrange.polynomize(partialDistributionFunction);

        final var testValueSet = generateTestValues(nd);
        final var validSet1 = testValueSet.parallelStream()
                .filter(testValue -> CheckExpressionEvaluation.evaluate(checkExpression1, testValue))
                .collect(Collectors.toSet());
        final var validSet2 = testValueSet.parallelStream()
                .filter(testValue -> CheckExpressionEvaluation.evaluate(checkExpression2, testValue))
                .collect(Collectors.toSet());
        final var validSet12 = validSet1.stream().filter(validSet2::contains).collect(Collectors.toSet());
        final var fullDistributionFunctionMemo = SSet
                .concat(validSet1, validSet2).stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        fullDistributionFunction
                ));

        final var weight12 = validSet12.stream()
                .mapToDouble(fullDistributionFunctionMemo::get)
                .map(x -> Math.max(x, 0.0))
                .sum();
        final var weightUnion = fullDistributionFunctionMemo.values().parallelStream().mapToDouble(x -> x).sum();
        return 1.0 - weight12 / weightUnion;
    }

    private static SortedSet<Double> generateTestValues(NumericalDistribution nd) {
        final var extremePair = StepIntervall.extremes(nd);
        final var min = extremePair.first();
        final var max = extremePair.second();
        final var factor = 1.0;
        final var extendedLength = (max - min) * factor;
        final var lengthExtension = (extendedLength - (max - min)) / 2.0;
        final var extendedMin = min - lengthExtension;
        final var distBetweenTestValues = (extendedLength / (SAMPLE_SIZE - 1));
        return Stream
                .iterate(extendedMin, v -> v + distBetweenTestValues)
                .limit(SAMPLE_SIZE)
                .collect(Collectors.toCollection(TreeSet::new));
    }
}