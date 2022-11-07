package scenarioCreator.data.primitives;

import scenarioCreator.data.Language;

import java.util.function.BiFunction;


public sealed interface StringPlus extends Comparable<StringPlus> permits StringPlusNaked, StringPlusSemantical {
    String rawString(BiFunction<NamingConvention, String[], String> merge);
    Language language();


    default NamingConvention guessNamingConvention(BiFunction<NamingConvention, String[], String> merge) {
        final var isScreaming = capitalizationRatio(rawString(merge)) > 0.5;
        final var isNumerical = letterRatio(rawString(merge)) < 0.7;

        if (rawString(merge).equals("")) {
            return NamingConvention.UNDEFINED;
        } else if (isNumerical) {
            return NamingConvention.TECHNICAL;
        } else if (rawString(merge).contains("_")) {
            return isScreaming ? NamingConvention.SCREAMINGSNAKECASE : NamingConvention.SNAKECASE;
        } else if (rawString(merge).contains("-")) {
            return isScreaming ? NamingConvention.SCREAMINGKEBABCASE : NamingConvention.KEBABCASE;
        } else if (Character.isUpperCase(rawString(merge).charAt(0))) {
            return NamingConvention.PASCALCASE;
        } else {
            return NamingConvention.CAMELCASE;
        }
    }

    private double capitalizationRatio(String str) {
        final var relevantChars = str.chars()
                .filter(Character::isLetter)
                .boxed().toList();
        final var capitalizedChars = relevantChars.stream()
                .filter(Character::isUpperCase)
                .count();
        return ((double) capitalizedChars) / ((double) relevantChars.size());
    }

    private double letterRatio(String str) {
        final var letterChars = str.chars()
                .filter(Character::isLetter)
                .count();
        return ((double) letterChars) / ((double) str.length());
    }

    @Override
    default int compareTo(StringPlus sp) {
        return this.toString().compareTo(sp.toString());
    }
}
