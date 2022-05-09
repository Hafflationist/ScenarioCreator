package de.mrobohm.operations.linguistic;

import org.jetbrains.annotations.Contract;

import java.util.stream.IntStream;

public class LinguisticDistances {

    @Contract(pure = true)
    public static int zeroOne(String a, String b) {
        return a.equalsIgnoreCase(b) ? 0 : 1;
    }

    @Contract(pure = true)
    public static int levenshtein(String a, String b) {
        int[][] memMatrix = new int[a.length() + 1][b.length() + 1];

        for (int i = 0; i <= a.length(); i++) {
            for (int j = 0; j <= b.length(); j++) {
                if (i == 0) {
                    memMatrix[i][j] = j;
                } else if (j == 0) {
                    memMatrix[i][j] = i;
                } else {
                    var replace = memMatrix[i - 1][j - 1] + a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1;
                    var delete = memMatrix[i - 1][j] + 1;
                    var insert = memMatrix[i][j - 1] + 1;
                    memMatrix[i][j] = IntStream
                            .of(replace, delete, insert)
                            .min()
                            .orElse(Integer.MAX_VALUE);
                }
            }
        }
        return memMatrix[b.length()][b.length()];
    }

    // TODO: Vllt noch mehr implementiere?
    // Synonyme Wörter können eine hohe Levenshtein-Distanz aufweise, jedoch semantisch identisch sein... :thinking:

    // Man könnte semantische Netze wie GermaNet oder WordNet nutzen. Beide bieten Java-APIs an:


}
