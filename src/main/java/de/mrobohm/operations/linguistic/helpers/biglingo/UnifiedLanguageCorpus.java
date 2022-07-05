package de.mrobohm.operations.linguistic.helpers.biglingo;

import de.mrobohm.data.Language;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.primitives.StringPlusSemantical;
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
        return StreamExtensions
                .tryPickRandom(synonymes, random)
                .map(pickedSynonyme -> {
                    var newLanguage = synonymesByLanguage.keySet().stream()
                            .filter(language -> synonymesByLanguage.get(language).contains(pickedSynonyme))
                            .findFirst()
                            .orElse(Language.Technical);
                    return new Pair<>(pickedSynonyme, newLanguage);
                });
    }


    public Optional<StringPlus> synonymizeRandomToken(StringPlus word, Random random) {
        var nc = word.guessNamingConvention();
        var wordTokenList = LinguisticUtils.tokenize(word);
        var pickedToken = StreamExtensions
                .pickRandomOrThrow(wordTokenList.stream(), new RuntimeException(), random);
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
    }

    public Set<Integer> estimateSynsetId(String word, Set<String> context) {
        return _corpora.keySet().stream()
                .map(language -> new Pair<>(language, _corpora.get(language).estimateSynset(word, context)))
                .max(Comparator.comparingInt(pair -> pair.second().size()))
                .map(Pair::second)
                .orElse(Set.of());
    }

    public Set<Set<Integer>> stringPlusToSynsetIdSet(StringPlus word) {
        return switch (word) {
            case StringPlusSemantical sps -> sps.tokenToSynsetId().stream()
                    .map(Pair::second)
                    .collect(Collectors.toSet());
            case StringPlusNaked spn -> Set.of(_corpora
                    .get(spn.language())
                    .estimateSynset(spn.rawString(), Set.of())
            );
        };
    }

    public double semanticDiff(StringPlus word1, StringPlus word2) {
        var synsetIdSetSet1 = stringPlusToSynsetIdSet(word1);
        var synsetIdSetSet2 = stringPlusToSynsetIdSet(word2);

        if (word1.language().equals(word2.language())) {
            // hier kommt der entspannte Teil
            var language = word1.language();
            return semanticDiffInner(synsetIdSetSet1, synsetIdSetSet2, language);
        }
        var corpus1 = _corpora.get(word1.language());
        var corpus2 = _corpora.get(word2.language());

        var interLingoSetSet1 = synsetIdSetSet1.stream()
                .map(corpus1::word2InterLingoRecord)
                .map(interSet -> interSet.stream()
                        .map(InterLingoRecord::num)
                        .collect(Collectors.toSet()))
                .collect(Collectors.toSet());
        var interLingoSetSet2 = synsetIdSetSet2.stream()
                .map(corpus2::word2InterLingoRecord)
                .map(interSet -> interSet.stream()
                        .map(InterLingoRecord::num)
                        .collect(Collectors.toSet()))
                .collect(Collectors.toSet());
        var rawDist = semanticDiffInner(interLingoSetSet1, interLingoSetSet2, Language.English);
        return Math.sqrt(rawDist) * 0.8 + 0.2; // so schlau

    }

    private double semanticDiffInner(Set<Set<Integer>> synsetIdSetSet1,
                                     Set<Set<Integer>> synsetIdSetSet2,
                                     Language language) {
        // Wir müssten jetzt die Namen in ihrer tokenisierten Form haben und müssen die semantische Ähnlichkeit bestimmen.
        // Ansatz: Man schaut sich bei jedem Token an, mit welchen Token vom anderen Wort er am besten passt.
        //         Ich muss die KIS-Folien studieren -> monge-elkan distance
        var diffA = mongeElkanDiff(synsetIdSetSet1, synsetIdSetSet2, language);
        var diffB = mongeElkanDiff(synsetIdSetSet2, synsetIdSetSet1, language);
        return (diffA + diffB) / 2.0;
    }

    private double mongeElkanDiff(Set<Set<Integer>> synsetIdSetSet1,
                                  Set<Set<Integer>> synsetIdSetSet2,
                                  Language language) {
        return synsetIdSetSet1.stream()
                .mapToDouble(synsetIdSet1 -> synsetIdSetSet2.stream()
                        .mapToDouble(synsetIdSet2 -> semanticDiff(synsetIdSet1, synsetIdSet2, language))
                        .min()
                        .orElse(1.0))
                .average() // vllt doch Summe?
                .orElse(1.0);
    }

    private double semanticDiff(Set<Integer> synsetIdSet1, Set<Integer> synsetIdSet2, Language language) {
        return _corpora.get(language).lowestSemanticDistance(synsetIdSet1, synsetIdSet2);
    }

}