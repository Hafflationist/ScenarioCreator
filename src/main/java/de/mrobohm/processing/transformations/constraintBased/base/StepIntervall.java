package de.mrobohm.processing.transformations.constraintBased.base;

import de.mrobohm.data.column.context.NumericalDistribution;
import de.mrobohm.utils.MMath;
import de.mrobohm.utils.Pair;
import de.mrobohm.utils.SSet;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public record StepIntervall(int step, double start, double end) implements Comparable<StepIntervall> {

    public StepIntervall {
        if (start > end) {
            throw new IllegalArgumentException("start was greater than end!");
        }
    }

    public static Stream<StepIntervall> fromNumericalDistribution(NumericalDistribution nd) {
        return nd.stepToOccurrences().keySet().stream()
                .map(step -> {
                    final var shift = (step > 0) ? -1 : 1;
                    final var p1 = (step + shift) * nd.stepSize();
                    final var p2 = (step) * nd.stepSize();
                    return new StepIntervall(step, Math.min(p1, p2), Math.max(p1, p2));
                });
    }

    public static Pair<Double, Double> extremes(NumericalDistribution nd) {
        final var minStep = nd.stepToOccurrences().keySet().stream().mapToInt(x -> x).min().orElse(0);
        final var maxStep = nd.stepToOccurrences().keySet().stream().mapToInt(x -> x).max().orElse(0);
        final var globalStart = minStep * nd.stepSize();
        final var globalEnd = maxStep * nd.stepSize();
        return new Pair<>(globalStart, globalEnd);
    }

    public static Stream<StepIntervall> fromNumericalDistribution(NumericalDistribution nd, double stepSize) {
        final var extremePair = extremes(nd);
        final var globalStart = extremePair.first();
        final var globalEnd = extremePair.second();

        final var positiveIntervallStream = Stream
                .iterate(
                        new StepIntervall(0, 0.0, 0.0),
                        si -> si.start < globalEnd,
                        si -> new StepIntervall(si.step + 1, si.end, si.end + stepSize)
                );
        final var negativeIntervallStream = Stream
                .iterate(
                        new StepIntervall(0, 0.0, 0.0),
                        si -> si.end > globalStart,
                        si -> new StepIntervall(si.step - 1, si.start - stepSize, si.start)
                );
        return Stream
                .concat(positiveIntervallStream, negativeIntervallStream)
                .filter(si -> si.step != 0);
    }

    public static SortedSet<StepIntervall> fillHoles(SortedSet<StepIntervall> stepIntervallSet) {
        final var from = stepIntervallSet.stream().mapToDouble(is -> is.start).min().orElse(Double.POSITIVE_INFINITY);
        final var to = stepIntervallSet.stream().mapToDouble(is -> is.end).max().orElse(Double.NEGATIVE_INFINITY);
        return fillHoles(stepIntervallSet, from, to);
    }

    public static SortedSet<StepIntervall> fillHoles(SortedSet<StepIntervall> stepIntervallSet, double from, double to) {
        if (MMath.isApproxSame(from, to) || from > to) {
            return stepIntervallSet;
        }
        final var partition = StreamExtensions.partition(stepIntervallSet.stream(), si -> si.end < from);
        final var nextStepIntervallOpt = partition.no().min(Comparator.comparingDouble(a -> a.start));
        if (nextStepIntervallOpt.isEmpty()) {
            return SSet.prepend(new StepIntervall(Integer.MAX_VALUE, from, to), stepIntervallSet);
        }
        final var nextStepIntervall = nextStepIntervallOpt.get();
        final var prequel = partition.yes().collect(Collectors.toSet());
        final var newStepIntervall = (nextStepIntervall.start <= from * 1.001)
                ? SSet.<StepIntervall>of()
                : SSet.of(new StepIntervall(Integer.MAX_VALUE, from, nextStepIntervall.start));
        final var reducedStepIntervallSet = stepIntervallSet.stream()
                .filter(si -> !si.equals(nextStepIntervall) && !prequel.contains(si))
                .collect(Collectors.toCollection(TreeSet::new));
        final var additionalIntervallSet = fillHoles(reducedStepIntervallSet, nextStepIntervall.end, to);
        return SSet.concat(newStepIntervall, SSet.concat(stepIntervallSet, additionalIntervallSet));
    }

    public static boolean intersecting(StepIntervall si1, StepIntervall si2) {
        return si1.start < si2.end && si1.end > si2.start;
    }

    public static double intersectionLength(StepIntervall si1, StepIntervall si2) {
        if (!intersecting(si1, si2)) {
            return 0.0;
        }
        final var minEnd = Math.min(si1.end, si2.end);
        final var maxStart = Math.max(si1.start, si2.start);
        return Math.max(0.0, minEnd - maxStart);
    }

    @Override
    public int compareTo(@NotNull StepIntervall osi) {
        return this.step != osi.step
                ? Integer.compare(this.step, osi.step)
                : (this.start != osi.start
                ? Double.compare(this.start, osi.start)
                : Double.compare(this.end, osi.end));
    }
}