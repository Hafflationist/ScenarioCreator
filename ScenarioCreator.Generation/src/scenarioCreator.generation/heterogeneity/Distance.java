package scenarioCreator.generation.heterogeneity;

import java.util.List;
import java.util.function.Function;

public record Distance(double structural, double linguistic, double constraintBased, double contextual) {
    public static Distance add(Distance d1, Distance d2) {
        return new Distance(
                d1.structural + d2.structural,
                d1.linguistic + d2.structural,
                d1.constraintBased + d2.constraintBased,
                d1.constraintBased + d2.constraintBased
        );
    }

    public static Distance diff(Distance d1, Distance d2) {
        return new Distance(
                Math.abs(d1.structural - d2.structural),
                Math.abs(d1.linguistic - d2.linguistic),
                Math.abs(d1.constraintBased - d2.constraintBased),
                Math.abs(d1.contextual - d2.contextual)
        );
    }

    public static Distance scale(Distance d, double factor) {
        return new Distance(
                d.structural * factor,
                d.linguistic * factor,
                d.constraintBased * factor,
                d.contextual * factor
        );
    }
    public static Distance avg(List<Distance> distanceList) {
        return new Distance(
                avgDistance(distanceList, Distance::structural),
                avgDistance(distanceList, Distance::linguistic),
                avgDistance(distanceList, Distance::constraintBased),
                avgDistance(distanceList, Distance::contextual)
        );
    }

    private static double avgDistance(List<Distance> distanceList, Function<Distance, Double> reduceDistance) {
        return distanceList.stream()
                .mapToDouble(reduceDistance::apply)
                .average()
                .orElse(Double.NaN);
    }
}
