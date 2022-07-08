package de.mrobohm.data.primitives;

import de.mrobohm.data.Language;
import de.mrobohm.data.primitives.synset.GermanSynset;
import de.mrobohm.data.primitives.synset.GlobalSynset;
import de.mrobohm.operations.linguistic.helpers.LinguisticUtils;
import de.mrobohm.utils.Pair;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

// Eigentlich müsste man das mal so machen, dass jedes Token eine eigene Sprache haben kann.
// Dies hat mEn ein zu hohen Aufwand/Außenwirkung-Verhätltnis.
public record StringPlusSemantical(List<StringPlusSemanticalSegment> segmentList,
                                   Language language, // TODO: Calculate based on synsets
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

        return new StringPlusSemantical(segmentList, stringPlus.language(), nc);
    }

    @Override
    public String rawString() {
        var tokenArray = segmentList.stream().map(StringPlusSemanticalSegment::token).toArray(String[]::new);
        return LinguisticUtils.merge(namingConvention(), tokenArray);
    }

    public StringPlusSemantical withTokenToSynsetId(List<StringPlusSemanticalSegment> newSegmentList) {
        return new StringPlusSemantical(newSegmentList, language, namingConvention);
    }
}