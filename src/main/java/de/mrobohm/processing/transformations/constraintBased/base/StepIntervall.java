package de.mrobohm.processing.transformations.constraintBased.base;

import de.mrobohm.data.column.context.NumericalDistribution;
import de.mrobohm.utils.MMath;
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
                    var shift = (step > 0) ? -1 : 1;
                    var p1 = (step + shift) * nd.stepSize();
                    var p2 = (step) * nd.stepSize();
                    return new StepIntervall(step, Math.min(p1, p2), Math.max(p1, p2));
                });
    }

    public static Stream<StepIntervall> fromNumericalDistribution(NumericalDistribution nd, double stepSize) {
        var minStep = nd.stepToOccurrences().keySet().stream().mapToInt(x -> x).min().orElse(0);
        var maxStep = nd.stepToOccurrences().keySet().stream().mapToInt(x -> x).max().orElse(0);
        var globalStart = minStep * nd.stepSize();
        var globalEnd = maxStep * nd.stepSize();

        var positiveIntervallStream = Stream
                .iterate(
                        new StepIntervall(0, 0.0, 0.0),
                        si -> si.start < globalEnd,
                        si -> new StepIntervall(si.step + 1, si.end, si.end + stepSize)
                );
        var negativeIntervallStream = Stream
                .iterate(
                        new StepIntervall(0, 0.0, 0.0),
                        si -> si.end > globalStart,
                        si -> new StepIntervall(si.step - 1, si.start - stepSize, si.start)
                );
        return Stream
                .concat(positiveIntervallStream, negativeIntervallStream)
                .filter(si -> si.step != 0);
    }

    public static SortedSet<StepIntervall> fillHoles(SortedSet<StepIntervall> stepIntervallSet, double from, double to) {
        if (MMath.isApproxSame(from, to) || from > to) {
            return stepIntervallSet;
        }
        var partition = StreamExtensions.partition(stepIntervallSet.stream(), si -> si.end < from);
        var nextStepIntervallOpt = partition.no().min(Comparator.comparingDouble(a -> a.start));
        if (nextStepIntervallOpt.isEmpty()) {
            return SSet.prepend(new StepIntervall(Integer.MAX_VALUE, from, to), stepIntervallSet);
        }
        var nextStepIntervall = nextStepIntervallOpt.get();
        var prequel = partition.yes().collect(Collectors.toSet());
        var newStepIntervall = (nextStepIntervall.start <= from * 1.001)
                ? SSet.<StepIntervall>of()
                : SSet.of(new StepIntervall(Integer.MAX_VALUE, from, nextStepIntervall.start));
        var reducedStepIntervallSet = stepIntervallSet.stream()
                .filter(si -> !si.equals(nextStepIntervall) && !prequel.contains(si))
                .collect(Collectors.toCollection(TreeSet::new));
        var additionalIntervallSet = fillHoles(reducedStepIntervallSet, nextStepIntervall.end, to);
        return SSet.concat(newStepIntervall, SSet.concat(stepIntervallSet, additionalIntervallSet));
    }

    public static boolean intersecting(StepIntervall si1, StepIntervall si2) {
        return si1.start < si2.end && si1.end > si2.start;
    }

    public static double intersectionLength(StepIntervall si1, StepIntervall si2) {
        if (!intersecting(si1, si2)) {
            return 0.0;
        }
        var minEnd = Math.min(si1.end, si2.end);
        var maxStart = Math.max(si1.start, si2.start);
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