package scenarioCreator.generation.processing.transformations.constraintBased.base;

import scenarioCreator.data.column.context.NumericalDistribution;
import scenarioCreator.utils.MMath;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.SSet;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class CheckNumericalManager {
    private CheckNumericalManager() {
    }

    public static NumericalDistribution merge(NumericalDistribution nd1, NumericalDistribution nd2) {
        if (Math.abs(nd1.stepSize()) < 0.001) {
            return nd2;
        }

        final var commonStepSize = findGoodCommonStepSize(nd1.stepSize(), nd2.stepSize());
        final var ndTrans1 = translateStepSize(nd1, commonStepSize);
        final var ndTrans2 = translateStepSize(nd2, commonStepSize);
        final var newStepToOccurences = SSet
                .concat(
                        ndTrans1.stepToOccurrences().keySet(),
                        ndTrans2.stepToOccurrences().keySet()
                )
                .stream()
                .distinct()
                .collect(Collectors.toMap(
                        Function.identity(),
                        step -> ndTrans1.stepToOccurrences().getOrDefault(step, 0.0)
                                + ndTrans2.stepToOccurrences().getOrDefault(step, 0.0)
                ));

        // normalization or blurring?
        return new NumericalDistribution(commonStepSize, newStepToOccurences);
    }

    private static double findGoodCommonStepSize(double ss1, double ss2) {
        // maybe we should set a lower limit...
        assert ss1 != 0.0;
        assert ss2 != 0.0;
        return MMath.gcd(ss1, ss2);
    }


    private static NumericalDistribution translateStepSize(NumericalDistribution nd, double newStepSize) {
        final var originalStepIntervallStream = StepIntervall
                .fromNumericalDistribution(nd)
                .collect(Collectors.toCollection(TreeSet::new));
        final var newStepIntervallStream = StepIntervall
                .fromNumericalDistribution(nd, newStepSize)
                .collect(Collectors.toCollection(TreeSet::new));

        final var newStepToOccurrences = newStepIntervallStream.stream()
                .filter(nsi -> originalStepIntervallStream.stream().anyMatch(osi -> StepIntervall.intersecting(nsi, osi)))
                .map(nsi -> {
                    final var intersectingIntervallsSet = originalStepIntervallStream.stream()
                            .filter(osi -> StepIntervall.intersecting(nsi, osi))
                            .collect(Collectors.toCollection(TreeSet::new));
                    return intervallInterpolation(nsi, intersectingIntervallsSet, nd);
                })
                .collect(Collectors.toMap(
                        pair -> pair.first().step(),
                        Pair::second
                ));
        return new NumericalDistribution(newStepSize, newStepToOccurrences);
    }

    private static Pair<StepIntervall, Double> intervallInterpolation(
            StepIntervall si, SortedSet<StepIntervall> osiSet, NumericalDistribution nd
    ) {
        final var weightedNumberStream = StepIntervall
                .fillHoles(osiSet, si.start(), si.end()).stream()
                .map(osi -> {
                    final var weight = StepIntervall.intersectionLength(osi, si);
                    final var originalValue = nd.stepToOccurrences().getOrDefault(osi.step(), 0.0);
                    return new MMath.WeightedNumber(weight, originalValue);
                });
        final var newOccurrences = MMath.avgWeighted(weightedNumberStream);
        return new Pair<>(si, newOccurrences);
    }

    public static NumericalDistribution normalize(NumericalDistribution nd) {
        final var sum = nd.stepToOccurrences().values().stream().mapToDouble(x -> x).sum();
        if (Math.abs(sum - 1.0) < 0.00001) {
            return nd;
        }
        final var newStepToOccurrences = nd.stepToOccurrences().keySet().stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        step -> {
                            final var occurrences = nd.stepToOccurrences().get(step);
                            return occurrences / sum;
                        }));
        return nd.withStepToOccurrences(newStepToOccurrences);
    }
}