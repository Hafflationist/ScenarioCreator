package scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo;

import scenarioCreator.data.Language;
import scenarioCreator.data.primitives.StringPlus;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.data.primitives.StringPlusSemantical;
import scenarioCreator.data.primitives.StringPlusSemanticalSegment;
import scenarioCreator.data.primitives.synset.EnglishSynset;
import scenarioCreator.data.primitives.synset.GlobalSynset;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.LinguisticUtils;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.SSet;
import scenarioCreator.utils.StreamExtensions;

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
        final var synonymesByLanguage = synonymizeFully(word);
        final var synonymes = synonymesByLanguage.values().stream().flatMap(Collection::stream);
        return StreamExtensions
                .tryPickRandom(synonymes, random)
                .map(pickedSynonyme -> {
                    final var newLanguage = synonymesByLanguage.keySet().stream()
                            .filter(language -> synonymesByLanguage.get(language).contains(pickedSynonyme))
                            .findFirst()
                            .orElse(Language.Technical);
                    return new Pair<>(pickedSynonyme, newLanguage);
                });
    }


    public Optional<StringPlus> synonymizeRandomToken(StringPlus word, Random random) {
        final var nc = word.guessNamingConvention(LinguisticUtils::merge);
        final var wordTokenList = LinguisticUtils.tokenize(word);
        final var pickedToken = StreamExtensions
                .pickRandomOrThrow(wordTokenList.stream(), new RuntimeException(), random);
        final var pairOptional = synonymize(pickedToken, random);
        if (pairOptional.isEmpty()) {
            return Optional.empty();
        }
        final var pickedSynonyme = pairOptional.get().first();
        final var introducedLanguage = pairOptional.get().second();
        final var newLanguage = word.language() != introducedLanguage ? Language.Mixed : introducedLanguage;
        final var newWordTokenStream = wordTokenList.stream()
                .map(t -> (t.equals(pickedToken)) ? pickedSynonyme : t);
        final var newWord = LinguisticUtils.merge(nc, newWordTokenStream.toArray(String[]::new));
        return Optional.of(new StringPlusNaked(newWord, newLanguage));
    }

    public SortedSet<GlobalSynset> estimateSynsetId(String word, SortedSet<String> context) {
        return _corpora.keySet().stream()
                .map(language -> new Pair<>(language, _corpora.get(language).estimateSynset(word, context)))
                .max(Comparator.comparingInt(pair -> pair.second().size()))
                .map(Pair::second)
                .orElse(SSet.of());
    }

    public List<SortedSet<GlobalSynset>> stringPlusToSynsetIdSet(StringPlus word) {
        return switch (word) {
            case StringPlusSemantical sps -> sps.segmentList().stream()
                     .map(StringPlusSemanticalSegment::gssSet)
                     .toList();
            case StringPlusNaked spn -> {
                if(!_corpora.containsKey(spn.language())){
                   yield List.of();
                }
                yield List.of(_corpora
                        .get(spn.language())
                        .estimateSynset(spn.rawString(), SSet.of())
                );
            }
        };
    }

    public double semanticDiff(StringPlus word1, StringPlus word2) {
        final var synsetIdSetSet1 = stringPlusToSynsetIdSet(word1);
        final var synsetIdSetSet2 = stringPlusToSynsetIdSet(word2);
        return semanticDiffInner(synsetIdSetSet1, synsetIdSetSet2);
    }

    private double semanticDiffInner(List<SortedSet<GlobalSynset>> gssSetList1,
                                     List<SortedSet<GlobalSynset>> gssSetList2) {
        // Wir müssten jetzt die Namen in ihrer tokenisierten Form haben und müssen die semantische Ähnlichkeit bestimmen.
        // Ansatz: Man schaut sich bei jedem Token an, mit welchen Token vom anderen Wort er am besten passt.
        //         Ich muss die KIS-Folien studieren -> monge-elkan distance
        final var diffA = mongeElkanDiff(gssSetList1, gssSetList2);
        final var diffB = mongeElkanDiff(gssSetList2, gssSetList1);
        return (diffA + diffB) / 2.0;
    }

    private double mongeElkanDiff(List<SortedSet<GlobalSynset>> gssSetList1,
                                  List<SortedSet<GlobalSynset>> gssSetList2) {
        return gssSetList1.stream()
                .mapToDouble(gssSet1 -> gssSetList2.stream()
                        .mapToDouble(gssSet2 -> semanticDiff(gssSet1, gssSet2))
                        .filter(x -> !Double.isNaN(x))
                        .min()
                        .orElse(1.0))
                .average() // vllt doch Summe?
                .orElse(1.0);
    }

    private double semanticDiff(SortedSet<GlobalSynset> synsetIdSet1, SortedSet<GlobalSynset> synsetIdSet2) {
        return synsetIdSet1.stream()
                .mapToDouble(gss1 -> synsetIdSet2.stream()
                        .mapToDouble(gss2 -> semanticDiff(gss1, gss2))
                        .filter(x -> !Double.isNaN(x))
                        .min()
                        .orElse(1.0))
                .min()
                .orElse(1.0);
    }

    private double semanticDiff(GlobalSynset gss1, GlobalSynset gss2) {
        if (gss1.language() == gss2.language()) {
            // Der entspannte Fall:
            assert _corpora.containsKey(gss1.language());
            return _corpora.get(gss1.language()).diff(gss1, gss2);
        }
        final var englishGss1 = _corpora.get(gss1.language()).word2EnglishSynset(SSet.of(gss1));
        final var englishGss2 = _corpora.get(gss2.language()).word2EnglishSynset(SSet.of(gss2));
        final var rawDist = minDiff(englishGss1, englishGss2);
        return Math.sqrt(rawDist) * 0.8 + 0.2; // so schlau
    }

    private double minDiff(SortedSet<EnglishSynset> essSet1,
                           SortedSet<EnglishSynset> essSet2) {
        return essSet1.stream()
                .mapToDouble(ess1 -> essSet2.stream()
                        .mapToDouble(ess2 -> _corpora.get(Language.English).diff(ess1, ess2))
                        .filter(x -> !Double.isNaN(x))
                        .min()
                        .orElse(1.0))
                .average() // vllt doch Summe?
                .orElse(1.0);
    }

    public SortedSet<StringPlusSemanticalSegment> translate(StringPlusSemanticalSegment segment, Language targetLanguage) {
        assert _corpora.containsKey(targetLanguage) : "missing support for language!";
        final var validGssMap = segment.gssSet().stream()
                .filter(gss -> gss.language() != targetLanguage)
                .collect(groupingBy(GlobalSynset::language));
        assert !validGssMap.values().isEmpty() : "gss already in target language!";
        return validGssMap.keySet().stream()
                .flatMap(lang -> {
                    final var gssSet = new TreeSet<>(validGssMap.get(lang));
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