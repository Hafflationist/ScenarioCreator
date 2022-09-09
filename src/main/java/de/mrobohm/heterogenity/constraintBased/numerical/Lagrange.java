package de.mrobohm.heterogenity.constraintBased.numerical;

import de.mrobohm.data.column.context.NumericalDistribution;
import de.mrobohm.processing.transformations.constraintBased.base.StepIntervall;

import java.util.Map;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class Lagrange {

    private Lagrange() {
    }

    public static Function<Double, Double> polynomize(PartialFunction partialFunction) {
        return xi -> partialFunction.func.keySet().stream()
                .mapToDouble(x -> {
                    final var term = partialFunction.func.get(x);
                    return term * partialFunction.func.keySet().stream()
                            .filter(x2 -> !x.equals(x2))
                            .mapToDouble(x2 -> (xi - x2) / (x - x2))
                            .reduce((a, b) -> a * b)
                            .orElse(0.0);
                })
                .sum();
    }

    public record PartialFunction(Map<Double, Double> func) {

        public PartialFunction(NumericalDistribution nd) {
            this(StepIntervall
                    .fillHoles(StepIntervall
                            .fromNumericalDistribution(nd)
                            .collect(Collectors.toCollection(TreeSet::new)),
                            1.25
                    ).stream()
                    .collect(Collectors.toMap(
                            is -> (is.start() + is.end()) / 2.0,
                            is -> nd.stepToOccurrences().getOrDefault(is.step(), 0.0)
                    ))
            );
        }
    }
}