package de.mrobohm.data.primitives;

import de.mrobohm.data.Language;
import de.mrobohm.data.primitives.synset.GlobalSynset;
import de.mrobohm.processing.transformations.linguistic.helpers.LinguisticUtils;

import java.util.List;
import java.util.SortedSet;
import java.util.function.Function;

// Eigentlich müsste man das mal so machen, dass jedes Token eine eigene Sprache haben kann.
// Dies hat mEn ein zu hohen Aufwand/Außenwirkung-Verhätltnis.
public record StringPlusSemantical(List<StringPlusSemanticalSegment> segmentList,
                                   NamingConvention namingConvention) implements StringPlus {

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

    public static StringPlusSemantical of(StringPlus stringPlus, Function<String, SortedSet<GlobalSynset>> synsetFinder) {
        final var nc = stringPlus.guessNamingConvention();
        final var tokens = LinguisticUtils.tokenize(stringPlus);
        final var segmentList = tokens.stream()
                .map(token -> new StringPlusSemanticalSegment(token, synsetFinder.apply(token)))
                .toList();

        return new StringPlusSemantical(segmentList, nc);
    }

    @Override
    public String rawString() {
        final var tokenArray = segmentList.stream().map(StringPlusSemanticalSegment::token).toArray(String[]::new);
        return LinguisticUtils.merge(namingConvention(), tokenArray);
    }

    public StringPlusSemantical withSegmentList(List<StringPlusSemanticalSegment> newSegmentList) {
        return new StringPlusSemantical(newSegmentList, namingConvention);
    }
}