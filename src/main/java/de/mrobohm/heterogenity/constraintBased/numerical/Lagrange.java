package de.mrobohm.heterogenity.constraintBased.numerical;

import java.util.Map;
import java.util.function.Function;

public class Lagrange {

    static Function<Double, Double> polynomize(Map<Double, Double> partialFunction) {
        return xi -> partialFunction.keySet().stream()
                .mapToDouble(x -> {
                    double term = partialFunction.get(x);
                    return term * partialFunction.keySet().stream()
                            .filter(x2 -> !x.equals(x2))
                            .mapToDouble(x2 -> (xi - x2) / (x - x2))
                            .reduce((a, b) -> a * b)
                            .orElse(0.0);
                })
                .sum();
    }
}