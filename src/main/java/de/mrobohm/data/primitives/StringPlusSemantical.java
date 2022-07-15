package de.mrobohm.data.primitives;

import de.mrobohm.data.Language;
import de.mrobohm.data.primitives.synset.GlobalSynset;
import de.mrobohm.transformations.linguistic.helpers.LinguisticUtils;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

// Eigentlich müsste man das mal so machen, dass jedes Token eine eigene Sprache haben kann.
// Dies hat mEn ein zu hohen Aufwand/Außenwirkung-Verhätltnis.
public record StringPlusSemantical(List<StringPlusSemanticalSegment> segmentList,
                                   NamingConvention namingConvention) implements StringPlus {

    @Override
    public Language language() {
        var languageList = segmentList.stream()
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

    public static StringPlusSemantical of(StringPlus stringPlus, Function<String, Set<GlobalSynset>> synsetFinder) {
        var nc = stringPlus.guessNamingConvention();
        var tokens = LinguisticUtils.tokenize(stringPlus);
        var segmentList = tokens.stream()
                .map(token -> new StringPlusSemanticalSegment(token, synsetFinder.apply(token)))
                .toList();

        return new StringPlusSemantical(segmentList, nc);
    }

    @Override
    public String rawString() {
        var tokenArray = segmentList.stream().map(StringPlusSemanticalSegment::token).toArray(String[]::new);
        return LinguisticUtils.merge(namingConvention(), tokenArray);
    }

    public StringPlusSemantical withSegmentList(List<StringPlusSemanticalSegment> newSegmentList) {
        return new StringPlusSemantical(newSegmentList, namingConvention);
    }
}