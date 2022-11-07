package scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo;

import edu.mit.jwi.IRAMDictionary;
import edu.mit.jwi.RAMDictionary;
import edu.mit.jwi.data.ILoadPolicy;
import edu.mit.jwi.item.*;
import edu.uniba.di.lacam.kdde.lexical_db.MITWordNet;
import edu.uniba.di.lacam.kdde.lexical_db.data.Concept;
import edu.uniba.di.lacam.kdde.ws4j.similarity.Lin;
import edu.uniba.di.lacam.kdde.ws4j.util.WS4JConfiguration;
import scenarioCreator.data.primitives.synset.EnglishSynset;
import scenarioCreator.data.primitives.synset.GlobalSynset;
import scenarioCreator.data.primitives.synset.PartOfSpeech;
import scenarioCreator.utils.SSet;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class WordNetInterface implements LanguageCorpus {

    private static final String DICT_FOLDER = "ScenarioCreator.Generation/resources/wordnet";
    private final IRAMDictionary _dict;

    public WordNetInterface() throws IOException {
        final var path = DICT_FOLDER + File.separator + "dict";
        final var url = new URL("file", null, path);
        _dict = new RAMDictionary(url, ILoadPolicy.NO_LOAD);
        _dict.open();
    }

    private POS partOfSpeechToPos(PartOfSpeech pos) {
        return switch (pos) {
            case NOUN -> POS.NOUN;
            case VERB -> POS.VERB;
            case ADJECTIVE -> POS.ADJECTIVE;
        };
    }

    private PartOfSpeech posToPartOfSpeech(POS pos) {
        return switch (pos) {
            case NOUN -> PartOfSpeech.NOUN;
            case VERB -> PartOfSpeech.VERB;
            case ADJECTIVE -> PartOfSpeech.ADJECTIVE;
            case default -> throw new RuntimeException("Not implemented");
        };
    }

    public SortedSet<String> getSynonymes(String wordStr) {
        final var idxWord = _dict.getIndexWord(wordStr, POS.NOUN);
        if (idxWord == null) {
            return SSet.of();
        }
        return idxWord.getWordIDs().stream()
                .map(_dict::getWord)
                .flatMap(word -> word.getSynset().getWords().stream())
                .map(IWord::getLemma)
                .map(str -> str.replace("_", "")) // hot_dog -> hotdog
                .map(String::toLowerCase) // Canisfamiliaris -> canisfamiliaris
                .collect(Collectors.toCollection(TreeSet::new));
    }

    @Override
    public SortedSet<String> getSynonymes(SortedSet<GlobalSynset> synsetIdSet) {
        return synsetIdSet.stream()
                .filter(gss -> gss instanceof EnglishSynset)
                .map(gss -> (EnglishSynset) gss)
                .flatMap(ess -> _dict
                        .getSynset(new SynsetID(ess.offset(), partOfSpeechToPos(ess.partOfSpeech())))
                        .getWords().stream()
                        .map(IWord::getLemma)
                        .map(str -> str.replace("_", "")) // hot_dog -> hotdog
                        .map(String::toLowerCase) // Canisfamiliaris -> canisfamiliaris
                )
                .collect(Collectors.toCollection(TreeSet::new));
    }

    @Override
    public SortedSet<GlobalSynset> estimateSynset(String word, SortedSet<String> otherWordSet) {
        // TODO: Diese Methode funktioniert zurzeit nicht richtig.
        if (_dict.getIndexWord(word, POS.NOUN) == null) {
            return SSet.of();
        }

        final var possibleSynsets = _dict
                .getIndexWord(word, POS.NOUN)
                .getWordIDs().stream()
                .map(_dict::getWord)
                .map(IWord::getSynset)
                .collect(Collectors.toSet());
        final var otherSynsets = otherWordSet.stream()
                .filter(w -> _dict.getIndexWord(w, POS.NOUN) != null)
                .flatMap(w -> _dict
                        .getIndexWord(w, POS.NOUN)
                        .getWordIDs().stream()
                        .map(_dict::getWord)
                        .map(IWord::getSynset))
                .collect(Collectors.toSet());

        final var distanceArray = possibleSynsets.stream().mapToDouble(ss -> avgDistance(ss, otherSynsets)).toArray();
        final var distanceMin = Arrays.stream(distanceArray).min().orElse(Double.POSITIVE_INFINITY);
        final var distanceMax = Arrays.stream(distanceArray).max().orElse(Double.NEGATIVE_INFINITY);

        return possibleSynsets.stream()
                .filter(ss -> avgDistance(ss, otherSynsets, distanceMin, distanceMax) < 0.1)
                .map(ss -> new EnglishSynset(ss.getOffset(), posToPartOfSpeech(ss.getPOS())))
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private double avgDistance(ISynset synset, Set<ISynset> otherSynsets, double min, double max) {
        return (min != max)
                ? (avgDistance(synset, otherSynsets) - min) / (max - min)
                : 0.0;
    }

    private double avgDistance(ISynset synset, Set<ISynset> otherSynsets) {
        WS4JConfiguration.getInstance().setMemoryDB(false);
        WS4JConfiguration.getInstance().setMFS(true);
        final var db = new MITWordNet(_dict);
        final var relatednessCalculator = new Lin(db);   // Resulting values are normalized [0, 1]
        final var concept = essToConcept(synset.getID());
        return otherSynsets.stream()
                .map(IItem::getID)
                .map(this::essToConcept)
                .mapToDouble(con -> 1.0 - relatednessCalculator.calcRelatednessOfSynsets(concept, con).getScore())
                .average()
                .orElse(2.0);
    }

    @Override
    public Map<String, SortedSet<GlobalSynset>> englishSynsetRecord2Word(EnglishSynset ess) {
        final var pos = partOfSpeechToPos(ess.partOfSpeech());
        return _dict.getSynset(new SynsetID(ess.offset(), pos)).getWords().stream()
                .map(IWord::getLemma)
                .collect(Collectors.toMap(
                        Function.identity(),
                        ignore -> SSet.of(ess)
                ));
    }

    @Override
    public SortedSet<EnglishSynset> word2EnglishSynset(SortedSet<GlobalSynset> gssSet) {
        return gssSet.stream()
                .filter(gss -> gss instanceof EnglishSynset)
                .map(gss -> (EnglishSynset) gss)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private Concept essToConcept(ISynsetID synsetId) {
        final var pos = edu.uniba.di.lacam.kdde.lexical_db.item.POS.getPOS(synsetId.toString().toLowerCase().charAt(13));
        return new Concept(synsetId.toString(), pos);
    }

    public double lowestSemanticDistance(SortedSet<GlobalSynset> synsetIdSet1, SortedSet<GlobalSynset> synsetIdSet2) {
        WS4JConfiguration.getInstance().setMemoryDB(false);
        WS4JConfiguration.getInstance().setMFS(true);
        final var db = new MITWordNet(_dict);
        final var relatednessCalculator = new Lin(db);   // Resulting values are normalized [0, 1]
        return synsetIdSet1.stream()
                .filter(gss -> gss instanceof EnglishSynset)
                .map(gss -> (EnglishSynset) gss)
                .mapToDouble(ess1 -> synsetIdSet2.stream()
                        .filter(gss -> gss instanceof EnglishSynset)
                        .map(gss -> (EnglishSynset) gss)
                        .mapToDouble(ess2 -> {
                            final var synsetId1 = new SynsetID(ess1.offset(), partOfSpeechToPos(ess1.partOfSpeech()));
                            final var synsetId2 = new SynsetID(ess2.offset(), partOfSpeechToPos(ess2.partOfSpeech()));
                            final var relatedness = relatednessCalculator
                                    .calcRelatednessOfSynsets(
                                            essToConcept(synsetId1),
                                            essToConcept(synsetId2))
                                    .getScore();
                            assert 0.0 <= relatedness;
                            assert relatedness <= 1.0;
                            return 1.0 - relatedness;
                        })
                        .min()
                        .orElse(1.0))
                .min()
                .orElse(1.0);
    }
}