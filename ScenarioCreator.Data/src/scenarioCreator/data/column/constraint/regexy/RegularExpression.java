package scenarioCreator.data.column.constraint.regexy;

import org.jetbrains.annotations.NotNull;

import java.util.stream.IntStream;
import java.util.stream.Stream;

public sealed interface RegularExpression extends Comparable<RegularExpression>
        permits
        RegularConcatenation,
        RegularKleene,
        RegularTerminal,
        RegularSum,
        RegularWildcard {
    default String toStringWithParentheses() {
        return switch (this) {
            case RegularTerminal ignore -> this.toString();
            case RegularWildcard ignore -> this.toString();
            default -> "(" + this + ")";
        };
    }

    static RegularExpression acceptsEverything() {
        return new RegularKleene(new RegularWildcard());
    }

    static Stream<Character> inputAlphabet() {
        var alphabet = Stream.concat(
                Stream.of('ä', 'ö', 'ü', 'ß', 'Ä', 'Ö', 'Ü', 'ẞ'),
                Stream.concat(
                        IntStream.rangeClosed('A', 'Z').mapToObj(var -> (char) var),
                        IntStream.rangeClosed('a', 'z').mapToObj(var -> (char) var)
                )
        );
        var digits = IntStream.rangeClosed('0', '9').mapToObj(var -> (char) var);
//        return Stream.concat(alphabet, digits).filter(NFA::isInputCharacter);
        return Stream.of('a', 'b', 'c');
    }

    @Override
    default int compareTo(@NotNull RegularExpression o) {
        return this.toString().compareTo(o.toString());
    }
}