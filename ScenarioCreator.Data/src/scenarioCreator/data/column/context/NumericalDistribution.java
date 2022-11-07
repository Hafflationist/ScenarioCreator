package scenarioCreator.data.column.context;

import java.util.Map;

// step n ist defined as [(n-1)*stepsize .. n*stepsize] if n > 0
// else: [(n+1)*stepsize .. n*stepsize]
public record NumericalDistribution(double stepSize, Map<Integer, Double> stepToOccurrences) {
    public static NumericalDistribution getDefault() {
        return new NumericalDistribution(0.0, Map.of());
    }

    public NumericalDistribution withStepSize(double newStepSize) {
        return new NumericalDistribution(newStepSize, stepToOccurrences);
    }

    public NumericalDistribution withStepToOccurrences(Map<Integer, Double> newStepToOccurrences) {
        return new NumericalDistribution(stepSize, newStepToOccurrences);
    }
}