package scenarioCreator.generation.processing.transformations.linguistic.helpers;

import scenarioCreator.data.primitives.StringPlus;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.data.primitives.StringPlusSemantical;
import scenarioCreator.data.primitives.StringPlusSemanticalSegment;
import scenarioCreator.utils.StreamExtensions;

import java.util.Random;

public final class CharBase {
    private CharBase() {
    }

    public static StringPlus introduceTypo(StringPlus cleanString, Random random) {
        return switch (cleanString) {
            case StringPlusNaked spn -> spn.withRawString(introduceTypo(spn.rawString(), random));
            case StringPlusSemantical sps -> {
                final var chosenSegmentOpt = StreamExtensions
                        .tryPickRandom(sps.segmentList().stream(), random);
                if (chosenSegmentOpt.isEmpty()) {
                    yield sps;
                }
                final var chosenSegment = chosenSegmentOpt.get();
                final var newSegment = new StringPlusSemanticalSegment(
                        introduceTypo(chosenSegment.token(), random), chosenSegment.gssSet()
                );
                final var newTokenToSynsetId = StreamExtensions
                        .replaceInStream(sps.segmentList().stream(), chosenSegment, newSegment)
                        .toList();
                yield sps.withSegmentList(newTokenToSynsetId);
            }
        };
    }

    public static String introduceTypo(String cleanString, Random random) {
        final var mode = random.nextInt(4);
        return switch (mode) {
            // Die Wurstfingervermutung ballert hier
            case 0, 3 -> randomInsertion(cleanString, random);
            case 1 -> randomSwap(cleanString, random);
            case 2 -> randomDeletion(cleanString, random);
            default -> throw new IllegalStateException("Unexpected value: " + mode);
        };
    }

    private static char generateAuthenticRandomChar(char contextChar, Random random) {
        // TODO: Hier böte sich die Möglichkeit, anhand von QWERTZ wahrscheinliche Kandidaten zu finden.
        final var startCharacter = Character.isUpperCase(contextChar) ? 'A' : 'a';
        return (char)(random.nextInt(26) + startCharacter);
    }

    public static String randomInsertion(String cleanString, Random random) {
        final var sb = new StringBuilder(cleanString);
        final var randomIdx = random.nextInt(sb.length());
        final var randomChar = generateAuthenticRandomChar(sb.charAt(randomIdx), random);
        return sb
                .insert(randomIdx, randomChar)
                .toString();
    }

    public static String randomSwap(String cleanString, Random random) {
        if (cleanString.length() < 2) return cleanString;
        final var sb = new StringBuilder(cleanString);
        final var firstIdx = random.nextInt(sb.length() - 1);
        final var pairSeq = sb.subSequence(firstIdx, firstIdx + 2);
        final var replacement = Character.toString(pairSeq.charAt(1)) + pairSeq.charAt(0);
        return sb
                .replace(firstIdx, firstIdx + 2, replacement)
                .toString();
    }

    public static String randomDeletion(String cleanString, Random random) {
        final var sb = new StringBuilder(cleanString);
        final var randomIdx = random.nextInt(sb.length());
        return sb
                .deleteCharAt(randomIdx)
                .toString();
    }
}