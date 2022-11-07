package scenarioCreator.data.primitives;

import java.util.Random;

public enum NamingConvention {
    CAMELCASE,          // thisIsCamelCase
    PASCALCASE,         // ThisIsPascalCase
    SNAKECASE,          // this_is_snake_case
    SCREAMINGSNAKECASE, // THIS_IS_SCREAMING_SNAKE_CASE
    KEBABCASE,          // this-is-kebab-case
    SCREAMINGKEBABCASE, // THIS-IS-SCREAMING-KEBAB-CASE
    UNDEFINED,          // Something-LikeThis_MAYBE
    TECHNICAL;          // 92630gezukw-.f-

    public static NamingConvention getRandom(Random random) {
        final var rnd = random.nextInt(0, 6);
        return switch (rnd) {
            case 0 -> NamingConvention.CAMELCASE;
            case 1 -> NamingConvention.PASCALCASE;
            case 2 -> NamingConvention.SNAKECASE;
            case 3 -> NamingConvention.SCREAMINGSNAKECASE;
            case 4 -> NamingConvention.KEBABCASE;
            case 5 -> NamingConvention.SCREAMINGKEBABCASE;
            default -> throw new IllegalStateException("Unexpected value: " + rnd);
        };
    }
}
