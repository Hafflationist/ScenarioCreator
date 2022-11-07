package scenarioCreator.data.primitives;

import scenarioCreator.data.Language;
import scenarioCreator.data.primitives.synset.GlobalSynset;

import java.util.List;
import java.util.SortedSet;
import java.util.function.BiFunction;
import java.util.function.Function;

// Eigentlich müsste man das mal so machen, dass jedes Token eine eigene Sprache haben kann.
// Dies hat mEn ein zu hohen Aufwand/Außenwirkung-Verhätltnis.
public record StringPlusSemantical(List<StringPlusSemanticalSegment> segmentList,
                                   NamingConvention namingConvention) implements StringPlus {

    public static StringPlusSemantical of(
            StringPlus stringPlus,
            Function<String, SortedSet<GlobalSynset>> synsetFinder,
            Function<StringPlus, List<String>> tokenize,
            BiFunction<NamingConvention, String[], String> merge) {
            final var nc = stringPlus.guessNamingConvention(merge);
        final var tokens = tokenize.apply(stringPlus);
        final var segmentList = tokens.stream()
                .map(token -> new StringPlusSemanticalSegment(token, synsetFinder.apply(token)))
                .toList();

        return new StringPlusSemantical(segmentList, nc);
    }

    @Override
    public Language language() {
        final var languageList = segmentList.stream()
                .flatMap(segment -> segment.gssSet().stream())
                .map(GlobalSynset::language)
                .distinct()
                .toList();

        return switch (languageList.size()) {
            case 0 -> Language.Technical;
            case 1 -> languageList.get(0);
            default -> Language.Mixed;
        };
    }

    @Override
    public String rawString(BiFunction<NamingConvention, String[], String> merge) {
        final var tokenArray = segmentList.stream().map(StringPlusSemanticalSegment::token).toArray(String[]::new);
        return merge.apply(namingConvention(), tokenArray);
    }

    public StringPlusSemantical withSegmentList(List<StringPlusSemanticalSegment> newSegmentList) {
        return new StringPlusSemantical(newSegmentList, namingConvention);
    }
}