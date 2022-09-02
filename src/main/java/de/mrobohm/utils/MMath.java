package de.mrobohm.utils;

public final class MMath {
    private MMath() {
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
}