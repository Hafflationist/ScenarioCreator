package de.mrobohm.utils;

import java.util.stream.Stream;

public final class MMath {
    private MMath() {
    }


    public static boolean isApproxSame(double a, double b) {
        var diff = Math.abs(a - b);
        var epsilon = Math.max(Math.min(Math.abs(a), Math.abs(b)) * 0.0001, 1e-10);
        return diff < epsilon;
    }

    // recursive implementation
    public static double gcd(double p, double q) {
        var epsilon = 0.0001 * Math.min(p, q);
        return gcd(p, q, epsilon);
    }

    public static double gcd(double p, double q, double epsilon) {
        if (Math.abs(q) < epsilon) return p;
        else return gcd(q, p % q, epsilon);
    }

    // non-recursive implementation
//    public static double gcd2(double p, double q) {
//        while (Math.abs(q) >= 0.00001) {
//            var temp = q;
//            q = p % q;
//            p = temp;
//        }
//        return p;
//    }

    public static double avgWeighted(Stream<WeightedNumber> weightedNumberStream) {
        var weightedNumberList = weightedNumberStream.toList();
        var weightSum = weightedNumberList.stream().mapToDouble(WeightedNumber::weight).sum();
        return weightedNumberList.stream()
                .mapToDouble(weightedNumber -> weightedNumber.weight() * weightedNumber.number() / weightSum)
                .sum();
    }

    public record WeightedNumber(Double weight, Double number) {
    }
}