package de.mrobohm.processing.transformations.constraintBased.base;

import de.mrobohm.data.column.context.NumericalDistribution;
import de.mrobohm.utils.MMath;
import de.mrobohm.utils.Pair;
import de.mrobohm.utils.SSet;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class CheckNumericalManager {
    private CheckNumericalManager() {
    }

    public static NumericalDistribution merge(NumericalDistribution nd1, NumericalDistribution nd2) {
        var commonStepSize = findGoodCommonStepSize(nd1.stepSize(), nd2.stepSize());
        var ndTrans1 = translateStepSize(nd1, commonStepSize);
        var ndTrans2 = translateStepSize(nd2, commonStepSize);
        var newStepToOccurences = SSet
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
        return MMath.gcd(ss1, ss2);
    }


    private static NumericalDistribution translateStepSize(NumericalDistribution nd, double newStepSize) {
        var originalStepIntervallStream = StepIntervall
                .fromNumericalDistribution(nd)
                .collect(Collectors.toCollection(TreeSet::new));
        var newStepIntervallStream = StepIntervall
                .fromNumericalDistribution(nd, newStepSize)
                .collect(Collectors.toCollection(TreeSet::new));

        var newStepToOccurrences = newStepIntervallStream.stream()
                .filter(nsi -> originalStepIntervallStream.stream().anyMatch(osi -> StepIntervall.intersecting(nsi, osi)))
                .map(nsi -> {
                    var intersectingIntervallsSet = originalStepIntervallStream.stream()
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
        var weightedNumberStream = StepIntervall
                .fillHoles(osiSet, si.start(), si.end()).stream()
                .map(osi -> {
                    var weight = StepIntervall.intersectionLength(osi, si);
                    var originalValue = nd.stepToOccurrences().getOrDefault(osi.step(), 0.0);
                    return new MMath.WeightedNumber(weight, originalValue);
                });
        var newOccurrences = MMath.avgWeighted(weightedNumberStream);
        return new Pair<>(si, newOccurrences);
    }

    public static NumericalDistribution normalize(NumericalDistribution nd) {
        var sum = nd.stepToOccurrences().values().stream().mapToDouble(x -> x).sum();
        if (Math.abs(sum - 1.0) < 0.00001) {
            return nd;
        }
        var newStepToOccurrences = nd.stepToOccurrences().keySet().stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        step -> {
                            var occurrences = nd.stepToOccurrences().get(step);
                            return occurrences / sum;
                        }));
        return nd.withStepToOccurrences(newStepToOccurrences);
    }
}