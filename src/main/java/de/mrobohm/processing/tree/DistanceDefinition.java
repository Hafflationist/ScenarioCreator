package de.mrobohm.processing.tree;

import de.mrobohm.heterogeneity.Distance;

public record DistanceDefinition(
        Target structural,
        Target linguistic,
        Target constraintBased,
        Target contextual
) {

    public static DistanceDefinition getDefault() {
        final var defaultTarget = new Target(0.0, 0.1, 1.0);
        return new DistanceDefinition(
                defaultTarget, defaultTarget, defaultTarget, defaultTarget
        );
    }

    public boolean isValid(Distance distance) {
        return structural.min <= distance.structural() && distance.structural() <= structural.max
                && linguistic.min <= distance.linguistic() && distance.linguistic() <= linguistic().max
                && constraintBased.min <= distance.constraintBased() && distance.constraintBased() <= constraintBased().max
                && contextual.min <= distance.contextual() && distance.contextual() <= contextual().max;
    }

    public Distance min() {
        return new Distance(
                structural.min,
                linguistic.min,
                constraintBased.min,
                contextual.min
        );
    }

    public Distance avg() {
        return new Distance(
                structural.avg,
                linguistic.avg,
                constraintBased.avg,
                contextual.avg
        );
    }

    public Distance max() {
        return new Distance(
                structural.max,
                linguistic.max,
                constraintBased.max,
                contextual.max
        );
    }

    public double diff(double min, double max, double value) {
        return Math.min(Math.abs(min - value), Math.abs(max - value));
    }

    public double diff(Distance distance) {
        return diff(structural.min, structural.max, distance.structural())
                + diff(linguistic.min, linguistic.max, distance.linguistic())
                + diff(constraintBased.min, constraintBased.max, distance.constraintBased())
                + diff(contextual.min, contextual.max, distance.contextual());
    }

    public record Target(double min, double avg, double max) {
        public Target {
            assert min <= avg || Double.isNaN(avg);
            assert avg <= max || Double.isNaN(avg);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Target target = (Target) o;
            return (Math.abs(target.min - min) <= (Math.min(Math.abs(target.min), Math.abs(min)) / 1e3) || Double.isNaN(target.min) && Double.isNaN(min))
                    && (Math.abs(target.avg - avg) <= (Math.min(Math.abs(target.avg), Math.abs(avg)) / 1e3) || Double.isNaN(target.avg) && Double.isNaN(avg))
                    && (Math.abs(target.max - max) <= (Math.min(Math.abs(target.max), Math.abs(max)) / 1e3) || Double.isNaN(target.max) && Double.isNaN(max));
        }
    }
}
