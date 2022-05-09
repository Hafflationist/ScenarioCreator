package de.mrobohm.operations.linguistic;

import de.mrobohm.data.Language;

import java.util.Random;

public final class LinguisticUtils {

    public static Language merge(Language a, Language b) {
        if (a.equals(b))
            return Language.Mixed;
        else
            return a;
    }

    public static String merge(String a, String b, Language language, Random random) {
        var rnd = random.nextInt(0, 6);
        return switch (rnd) {
            case 0 -> a + b;
            case 1 -> a + capitalize(b);    // camelCase
            case 2 -> a + "_" + b;          // snake_case
            case 3 -> a + generateConjunction(language) + b;
            case 4 -> a + capitalize(generateConjunction(language)) + capitalize(b); // camelCase
            case 5 -> a + "_" + generateConjunction(language) + "_" + b; // snake_case
            default -> merge(a, b, language, random);
        };
    }

    private static String capitalize(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    private static String generateConjunction(Language language) {
        return switch (language) {
            case German -> "und";
            case English, Mixed -> "and";
        };
    }
}
