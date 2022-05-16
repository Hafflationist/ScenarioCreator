package de.mrobohm.operations.linguistic;

import de.mrobohm.data.Language;
import de.mrobohm.data.primitives.NamingConvention;
import de.mrobohm.data.primitives.StringPlus;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public final class LinguisticUtils {

    // https://link.springer.com/content/pdf/10.1007/978-3-642-41338-4_19.pdf
    @NotNull
    public static List<String> tokenize(StringPlus mergedTokens) {
        var namingConvention = mergedTokens.guessNamingConvention();
        var stringStream = switch (namingConvention) {
            // https://www.programcreek.com/2011/03/java-method-for-spliting-a-camelcase-string/
            case CAMELCASE, PASCALCASE ->
                    mergedTokens.rawString().split("(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])");
            case SNAKECASE, SCREAMINGSNAKECASE -> mergedTokens.rawString().split("_");
            case KEBABCASE, SCREAMINGKEBABCASE -> mergedTokens.rawString().split("-");
            case UNDEFINED, TECHNICAL -> new String[]{mergedTokens.rawString()};
        };
        return Arrays.stream(stringStream).map(String::toLowerCase).toList();
    }

    @NotNull
    public static String merge(NamingConvention nc, String... tokens) {
        var tokenStream = Arrays.stream(tokens);
        return switch (nc) {
            case CAMELCASE -> mergeCamelCase(tokens);
            case PASCALCASE -> mergePascalCase(tokens);
            case SNAKECASE -> tokenStream
                    .map(t -> capitalize(t.toLowerCase()))
                    .collect(Collectors.joining("_"));
            case SCREAMINGSNAKECASE -> tokenStream
                    .map(t -> capitalize(t.toUpperCase()))
                    .collect(Collectors.joining("_"));
            case KEBABCASE -> tokenStream
                    .map(t -> capitalize(t.toLowerCase()))
                    .collect(Collectors.joining("-"));
            case SCREAMINGKEBABCASE -> tokenStream
                    .map(t -> capitalize(t.toUpperCase()))
                    .collect(Collectors.joining("-"));
            case UNDEFINED -> String.join("", tokens);
            case TECHNICAL -> String.join("", tokens);
        };
    }

    private static String mergePascalCase(String... tokens) {
        return Arrays.stream(tokens)
                .map(t -> capitalize(t.toLowerCase()))
                .collect(Collectors.joining());
    }

    @NotNull
    private static String mergeCamelCase(String... tokens) {
        if (tokens.length <= 1) {
            return String.join("", tokens).toLowerCase();
        }

        var head = tokens[0];
        var tail = Arrays.stream(tokens).skip(1).toArray(String[]::new);
        return head.toLowerCase() + mergePascalCase(tail);
    }

    @NotNull
    public static Language merge(Language a, Language b) {
        if (a.equals(b))
            return Language.Mixed;
        else
            return a;
    }

    @NotNull
    public static NamingConvention merge(NamingConvention nc1, NamingConvention nc2, Random random) {
        if (nc1 == nc2) {
            return nc1;
        }
        if (nc1 == NamingConvention.UNDEFINED) {
            return nc2;
        }
        if (nc2 == NamingConvention.UNDEFINED) {
            return nc1;
        }
        if (nc1 == NamingConvention.TECHNICAL) {
            return nc2;
        }
        if (nc2 == NamingConvention.TECHNICAL) {
            return nc1;
        }
        if (nc1 == NamingConvention.CAMELCASE && nc2 == NamingConvention.PASCALCASE
                || nc1 == NamingConvention.PASCALCASE && nc2 == NamingConvention.CAMELCASE) {
            return NamingConvention.PASCALCASE;
        }
        if (nc1 == NamingConvention.SNAKECASE && nc2 == NamingConvention.SCREAMINGSNAKECASE
                || nc1 == NamingConvention.SCREAMINGSNAKECASE && nc2 == NamingConvention.SNAKECASE) {
            return NamingConvention.SCREAMINGSNAKECASE;
        }
        if (nc1 == NamingConvention.KEBABCASE && nc2 == NamingConvention.SCREAMINGKEBABCASE
                || nc1 == NamingConvention.SCREAMINGKEBABCASE && nc2 == NamingConvention.KEBABCASE) {
            return NamingConvention.SCREAMINGKEBABCASE;
        }
        return NamingConvention.getRandom(random);
    }

    @NotNull
    public static StringPlus merge(StringPlus aPlus, StringPlus bPlus, Random random) {
        var a = aPlus.rawString();
        var b = bPlus.rawString();
        var language = merge(aPlus.language(), bPlus.language());
        var namingConvention = merge(aPlus.guessNamingConvention(), bPlus.guessNamingConvention(), random);

        var useConjunction = random.nextInt() % 2 == 0;
        var rawString = useConjunction
                ? merge(namingConvention, a, generateConjunction(language), b)
                : merge(namingConvention, a, b);
        return new StringPlus(rawString, language);
    }

    @NotNull
    private static String capitalize(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    @NotNull
    private static String generateConjunction(Language language) {
        return switch (language) {
            case German -> "und";
            case English, Mixed -> "and";
            case Technical -> "et";
        };
    }
}
