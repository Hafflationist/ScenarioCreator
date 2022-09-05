package de.mrobohm.heterogenity;

import org.jetbrains.annotations.Contract;

import java.util.stream.IntStream;

public class StringDistances {

    @Contract(pure = true)
    private static int zeroOne(String a, String b) {
        return a.equalsIgnoreCase(b) ? 0 : 1;
    }

    @Contract(pure = true)
    private static int levenshtein(String a, String b) {
        int[][] memMatrix = new int[a.length() + 1][b.length() + 1];

        for (int i = 0; i <= a.length(); i++) {
            for (int l = 0; l <= b.length(); l++) {
                if (i == 0) {
                    memMatrix[i][l] = l;
                } else if (l == 0) {
                    memMatrix[i][l] = i;
                } else {
                    final var replace = memMatrix[i - 1][l - 1] + (a.charAt(i - 1) == b.charAt(l - 1) ? 0 : 1);
                    final var delete = memMatrix[i - 1][l] + 1;
                    final var insert = memMatrix[i][l - 1] + 1;
                    memMatrix[i][l] = IntStream
                            .of(replace, delete, insert)
                            .min()
                            .orElse(Integer.MAX_VALUE);
                }
            }
        }
        return memMatrix[a.length()][b.length()];
    }

    @Contract(pure = true)
    public static double levenshteinNorm(String a, String b) {
        final var maxLength = (double) Math.max(a.length(), b.length());
        final var levenshteinDist = (double) levenshtein(a, b);
        return levenshteinDist / maxLength;
    }
}