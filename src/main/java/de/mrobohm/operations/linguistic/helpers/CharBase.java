package de.mrobohm.operations.linguistic.helpers;

import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.primitives.StringPlusSemantical;
import de.mrobohm.data.primitives.StringPlusSemanticalSegment;
import de.mrobohm.utils.StreamExtensions;

import java.util.Random;

public final class CharBase {
    private CharBase() {
    }

    public static StringPlus introduceTypo(StringPlus cleanString, Random random) {
        return switch (cleanString) {
            case StringPlusNaked spn -> spn.withRawString(introduceTypo(spn.rawString(), random));
            case StringPlusSemantical sps -> {
                var chosenSegmentOpt = StreamExtensions
                        .tryPickRandom(sps.segmentList().stream(), random);
                if (chosenSegmentOpt.isEmpty()) {
                    yield sps;
                }
                var chosenSegment = chosenSegmentOpt.get();
                var newSegment = new StringPlusSemanticalSegment(
                        introduceTypo(chosenSegment.token(), random), chosenSegment.gssSet()
                );
                var newTokenToSynsetId = StreamExtensions
                        .replaceInStream(sps.segmentList().stream(), chosenSegment, newSegment)
                        .toList();
                yield sps.withTokenToSynsetId(newTokenToSynsetId);
            }
        };
    }

    public static String introduceTypo(String cleanString, Random random) {
        var mode = random.nextInt(4);
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
        var startCharacter = Character.isUpperCase(contextChar) ? 'A' : 'a';
        return (char)(random.nextInt(26) + startCharacter);
    }

    public static String randomInsertion(String cleanString, Random random) {
        var sb = new StringBuilder(cleanString);
        var randomIdx = random.nextInt(sb.length());
        var randomChar = generateAuthenticRandomChar(sb.charAt(randomIdx), random);
        return sb
                .insert(randomIdx, randomChar)
                .toString();
    }

    public static String randomSwap(String cleanString, Random random) {
        var sb = new StringBuilder(cleanString);
        var firstIdx = random.nextInt(sb.length() - 1);
        var pairSeq = sb.subSequence(firstIdx, firstIdx + 2);
        var replacement = Character.toString(pairSeq.charAt(1)) + pairSeq.charAt(0);
        return sb
                .replace(firstIdx, firstIdx + 2, replacement)
                .toString();
    }

    public static String randomDeletion(String cleanString, Random random) {
        var sb = new StringBuilder(cleanString);
        var randomIdx = random.nextInt(sb.length());
        return sb
                .deleteCharAt(randomIdx)
                .toString();
    }
}