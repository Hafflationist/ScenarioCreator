package de.mrobohm.data.primitives;

import de.mrobohm.data.Language;


public sealed interface StringPlus extends Comparable<StringPlus> permits StringPlusNaked, StringPlusSemantical {
    String rawString();
    Language language();


    default NamingConvention guessNamingConvention() {
        var isScreaming = capitalizationRatio(rawString()) > 0.5;
        var isNumerical = letterRatio(rawString()) < 0.7;

        if (rawString().equals("")) {
            return NamingConvention.UNDEFINED;
        } else if (isNumerical) {
            return NamingConvention.TECHNICAL;
        } else if (rawString().contains("_")) {
            return isScreaming ? NamingConvention.SCREAMINGSNAKECASE : NamingConvention.SNAKECASE;
        } else if (rawString().contains("-")) {
            return isScreaming ? NamingConvention.SCREAMINGKEBABCASE : NamingConvention.KEBABCASE;
        } else if (Character.isUpperCase(rawString().charAt(0))) {
            return NamingConvention.PASCALCASE;
        } else {
            return NamingConvention.CAMELCASE;
        }
    }

    private double capitalizationRatio(String str) {
        var relevantChars = str.chars()
                .filter(Character::isLetter)
                .boxed().toList();
        var capitalizedChars = relevantChars.stream()
                .filter(Character::isUpperCase)
                .count();
        return ((double) capitalizedChars) / ((double) relevantChars.size());
    }

    private double letterRatio(String str) {
        var letterChars = str.chars()
                .filter(Character::isLetter)
                .count();
        return ((double) letterChars) / ((double) str.length());
    }

    @Override
    default int compareTo(StringPlus sp) {
        return this.toString().compareTo(sp.toString());
    }
}
