package de.mrobohm.data.primitives;

import de.mrobohm.data.Language;
import de.mrobohm.data.primitives.synset.GlobalSynset;
import de.mrobohm.operations.linguistic.helpers.LinguisticUtils;
import de.mrobohm.utils.Pair;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

// Eigentlich müsste man das mal so machen, dass jedes Token eine eigene Sprache haben kann.
// Dies hat mEn ein zu hohen Aufwand/Außenwirkung-Verhätltnis.
public record StringPlusSemantical(List<Pair<String, Set<GlobalSynset>>> tokenToSynsetId,
                                   Language language, // TODO: Calculate based on synsets
                                   NamingConvention namingConvention) implements StringPlus {

    public static StringPlusSemantical of(StringPlus stringPlus, Function<String, Set<GlobalSynset>> synsetFinder) {
        var nc = stringPlus.guessNamingConvention();
        var tokens = LinguisticUtils.tokenize(stringPlus);
        var tokenToSynsetId = tokens.stream()
                .map(token -> new Pair<>(token, synsetFinder.apply(token)))
                .toList();

        return new StringPlusSemantical(tokenToSynsetId, stringPlus.language(), nc);
    }

    @Override
    public String rawString() {
        var tokenArray = tokenToSynsetId.stream().map(Pair::first).toArray(String[]::new);
        return LinguisticUtils.merge(namingConvention(), tokenArray);
    }

    public StringPlusSemantical withTokenToSynsetId(List<Pair<String, Set<GlobalSynset>>> newTokenToSynsetId) {
        return new StringPlusSemantical(newTokenToSynsetId, language, namingConvention);
    }
}