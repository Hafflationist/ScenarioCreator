package de.mrobohm.data.primitives;

import de.mrobohm.data.Language;
import de.mrobohm.operations.linguistic.helpers.LinguisticUtils;
import de.mrobohm.utils.Pair;

import java.util.List;
import java.util.Set;
import java.util.function.Function;


public record StringPlusSemantical(List<Pair<String, Set<Integer>>> tokenToSynsetId,
                                   Language language,
                                   NamingConvention namingConvention) implements StringPlus {

    public static StringPlusSemantical of(StringPlus stringPlus, Function<String, Set<Integer>> synsetFinder) {
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
}