package de.mrobohm.processing.transformations.linguistic.helpers.biglingo;

import de.mrobohm.data.Language;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.primitives.StringPlusSemantical;
import de.mrobohm.data.primitives.StringPlusSemanticalSegment;
import de.mrobohm.data.primitives.synset.GlobalSynset;
import de.mrobohm.processing.transformations.linguistic.helpers.LinguisticUtils;
import de.mrobohm.utils.Pair;
import de.mrobohm.utils.SSet;
import de.mrobohm.utils.StreamExtensions;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

public class UnifiedLanguageCorpus {

    private final Map<Language, LanguageCorpus> _corpora;

    public UnifiedLanguageCorpus(Map<Language, LanguageCorpus> corpora) {
        _corpora = corpora;
    }

    private Map<Language, SortedSet<String>> synonymizeFully(String word) {
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

    public SortedSet<GlobalSynset> estimateSynsetId(String word, SortedSet<String> context) {
        return _corpora.keySet().stream()
                .map(language -> new Pair<>(language, _corpora.get(language).estimateSynset(word, context)))
                .max(Comparator.comparingInt(pair -> pair.second().size()))
                .map(Pair::second)
                .orElse(SSet.of());
    }

    public SortedSet<SortedSet<GlobalSynset>> stringPlusToSynsetIdSet(StringPlus word) {
        return switch (word) {
            case StringPlusSemantical sps -> sps.segmentList().stream()
                    .map(StringPlusSemanticalSegment::gssSet)
                    .collect(Collectors.toCollection(TreeSet::new));
            case StringPlusNaked spn -> SSet.of(_corpora
                    .get(spn.language())
                    .estimateSynset(spn.rawString(), SSet.of())
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

        var gssSetSet1 = synsetIdSetSet1.stream()
                .map(corpus1::word2EnglishSynset)
                .map(essSet -> essSet.stream()
                        .map(ess -> (GlobalSynset) ess)
                        .collect(Collectors.toCollection(() -> (SortedSet<GlobalSynset>) new TreeSet<GlobalSynset>())))
                .collect(Collectors.toCollection(TreeSet::new));
        var gssSetSet2 = synsetIdSetSet2.stream()
                .map(corpus2::word2EnglishSynset)
                .map(essSet -> essSet.stream()
                        .map(ess -> (GlobalSynset) ess)
                        .collect(Collectors.toCollection(() -> (SortedSet<GlobalSynset>) new TreeSet<GlobalSynset>())))
                .collect(Collectors.toCollection(TreeSet::new));
        var rawDist = semanticDiffInner(gssSetSet1, gssSetSet2, Language.English);
        return Math.sqrt(rawDist) * 0.8 + 0.2; // so schlau
    }

    private double semanticDiffInner(SortedSet<SortedSet<GlobalSynset>> gssSetSet1,
                                     SortedSet<SortedSet<GlobalSynset>> gssSetSet2,
                                     Language language) {
        // Wir müssten jetzt die Namen in ihrer tokenisierten Form haben und müssen die semantische Ähnlichkeit bestimmen.
        // Ansatz: Man schaut sich bei jedem Token an, mit welchen Token vom anderen Wort er am besten passt.
        //         Ich muss die KIS-Folien studieren -> monge-elkan distance
        var diffA = mongeElkanDiff(gssSetSet1, gssSetSet2, language);
        var diffB = mongeElkanDiff(gssSetSet2, gssSetSet1, language);
        return (diffA + diffB) / 2.0;
    }

    private double mongeElkanDiff(SortedSet<SortedSet<GlobalSynset>> gssSetSet1,
                                  SortedSet<SortedSet<GlobalSynset>> gssSetSet2,
                                  Language language) {
        return gssSetSet1.stream()
                .mapToDouble(gssSet1 -> gssSetSet2.stream()
                        .mapToDouble(gssSet2 -> semanticDiff(gssSet1, gssSet2, language))
                        .min()
                        .orElse(1.0))
                .average() // vllt doch Summe?
                .orElse(1.0);
    }

    private double semanticDiff(SortedSet<GlobalSynset> synsetIdSet1, SortedSet<GlobalSynset> synsetIdSet2, Language language) {
        return _corpora.get(language).lowestSemanticDistance(synsetIdSet1, synsetIdSet2);
    }

    public SortedSet<StringPlusSemanticalSegment> translate(StringPlusSemanticalSegment segment, Language targetLanguage) {
        assert _corpora.containsKey(targetLanguage) : "missing support for language!";
        var validGssMap = segment.gssSet().stream()
                .filter(gss -> gss.language() != targetLanguage)
                .collect(groupingBy(GlobalSynset::language));
        assert !validGssMap.values().isEmpty() : "gss already in target language!";
        return validGssMap.keySet().stream()
                .flatMap(lang -> {
                    var gssSet = new TreeSet<>(validGssMap.get(lang));
                    return _corpora.get(lang).word2EnglishSynset(gssSet).stream();
                })
                .map(_corpora.get(targetLanguage)::englishSynsetRecord2Word)
                .flatMap(translation -> translation.keySet().stream()
                        .map(orthForm -> new StringPlusSemanticalSegment(
                                StringPlusSemanticalSegment.normalizeToken(orthForm),
                                translation.get(orthForm)
                        )))
                .collect(Collectors.toCollection(TreeSet::new));
    }
}