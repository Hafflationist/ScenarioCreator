package de.mrobohm.operations.linguistic.helpers.biglingo;

import de.mrobohm.data.Language;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.operations.linguistic.helpers.LinguisticUtils;
import de.mrobohm.utils.Pair;
import de.mrobohm.utils.StreamExtensions;

import java.util.*;
import java.util.stream.Collectors;

public class UnifiedLanguageCorpus {

    private final Map<Language, LanguageCorpus> _corpora;

    public UnifiedLanguageCorpus(Map<Language, LanguageCorpus> corpora) {
        _corpora = corpora;
    }

    private Map<Language, Set<String>> synonymizeFully(String word) {
        return _corpora.keySet().stream()
                .collect(Collectors.toMap(
                        l -> l,
                        l -> _corpora.get(l).getSynonymes(word)));
    }


    private Optional<Pair<String, Language>> synonymize(String word, Random random) {
        var synonymesByLanguage = synonymizeFully(word);
        var synonymes = synonymesByLanguage.values().stream().flatMap(Collection::stream);
        try {
            var pickedSynonyme = StreamExtensions.pickRandomOrThrow(synonymes, new LocalException(), random);
            var newLanguage = synonymesByLanguage.keySet().stream()
                    .filter(language -> synonymesByLanguage.get(language).contains(pickedSynonyme))
                    .findFirst()
                    .orElse(Language.Technical);
            return Optional.of(new Pair<>(pickedSynonyme, newLanguage));
        } catch (LocalException e) {
            return Optional.empty();
        }
    }


    public Optional<StringPlus> synonymizeRandomToken(StringPlus word, Random random) {
        var nc = word.guessNamingConvention();
        var wordTokenList = LinguisticUtils.tokenize(word);
        try {
            var pickedToken = StreamExtensions.pickRandomOrThrow(wordTokenList.stream(), new LocalException(), random);
            var pairOptional = synonymize(pickedToken, random);
            if (pairOptional.isEmpty()) {
                return Optional.empty();
            }
            var pickedSynonyme = pairOptional.get().first();
            var introducedLanguage = pairOptional.get().second();
            var newLanguage = word.language() != introducedLanguage ? Language.Mixed : introducedLanguage;
            var newWordTokenStream = wordTokenList.stream()
                    .map(t -> (t.equals(pickedToken)) ? pickedSynonyme : t);
            var newWord = LinguisticUtils.merge(nc, newWordTokenStream.toArray(String[]::new));
            return Optional.of(new StringPlusNaked(newWord, newLanguage));
        } catch (LocalException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<Integer> estimateSynsetId(String word, Set<String> context) {
        return _corpora.keySet().stream()
                .map(language -> new Pair<>(language, _corpora.get(language).estimateSynset(word, context)))
                .max(Comparator.comparingInt(pair -> pair.second().size()))
                .map(Pair::second)
                .orElse(Set.of());
    }

    private static class LocalException extends Exception {
    }

}