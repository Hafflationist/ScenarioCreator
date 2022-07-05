package de.mrobohm.operations.linguistic.helpers.biglingo;

import edu.mit.jwi.IRAMDictionary;
import edu.mit.jwi.RAMDictionary;
import edu.mit.jwi.data.ILoadPolicy;
import edu.mit.jwi.item.*;
import edu.uniba.di.lacam.kdde.lexical_db.MITWordNet;
import edu.uniba.di.lacam.kdde.lexical_db.data.Concept;
import edu.uniba.di.lacam.kdde.ws4j.similarity.WuPalmer;
import edu.uniba.di.lacam.kdde.ws4j.util.WS4JConfiguration;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class WordNetInterface implements LanguageCorpus {

    private static final String DICT_FOLDER = "src/main/resources/wordnet/";
    private final IRAMDictionary _dict;

    public WordNetInterface() throws IOException {
        var path = DICT_FOLDER + File.separator + "dict";
        var url = new URL("file", null, path);
        _dict = new RAMDictionary(url, ILoadPolicy.NO_LOAD);
        _dict.open();
    }

    public Set<String> getSynonymes(String wordStr) {
        var idxWord = _dict.getIndexWord(wordStr, POS.NOUN);
        return idxWord.getWordIDs().stream()
                .map(_dict::getWord)
                .flatMap(word -> word.getSynset().getWords().stream())
                .map(IWord::getLemma)
                .map(str -> str.replace("_", "")) // hot_dog -> hotdog
                .map(String::toLowerCase) // Canisfamiliaris -> canisfamiliaris
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getSynonymes(Set<Integer> synsetIdSet) {
        return synsetIdSet.stream()
                .flatMap(synsetId -> _dict
                        .getSynset(new SynsetID(synsetId, POS.NOUN))
                        .getWords().stream()
                        .map(IWord::getLemma)
                        .map(str -> str.replace("_", "")) // hot_dog -> hotdog
                        .map(String::toLowerCase) // Canisfamiliaris -> canisfamiliaris
                )
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Integer> estimateSynset(String word, Set<String> otherWordSet) {
        // TODO: Diese Methode funktioniert zurzeit nicht richtig.
        if (_dict.getIndexWord(word, POS.NOUN) == null) {
            return Set.of();
        }


        var possibleSynsets = _dict
                .getIndexWord(word, POS.NOUN)
                .getWordIDs().stream()
                .map(_dict::getWord)
                .map(IWord::getSynset)
                .collect(Collectors.toSet());
        var otherSynsets = otherWordSet.stream()
                .filter(w -> _dict.getIndexWord(w, POS.NOUN) != null)
                .flatMap(w -> _dict
                        .getIndexWord(w, POS.NOUN)
                        .getWordIDs().stream()
                        .map(_dict::getWord)
                        .map(IWord::getSynset))
                .collect(Collectors.toSet());

        var distanceArray = possibleSynsets.stream().mapToDouble(ss -> avgDistance(ss, otherSynsets)).toArray();
        var distanceMin = Arrays.stream(distanceArray).min().orElse(Double.POSITIVE_INFINITY);
        var distanceMax = Arrays.stream(distanceArray).max().orElse(Double.NEGATIVE_INFINITY);

        return possibleSynsets.stream()
                .filter(ss -> avgDistance(ss, otherSynsets, distanceMin, distanceMax) < 0.1)
                .map(ISynset::getOffset)
                .collect(Collectors.toSet());
    }

    private double avgDistance(ISynset synset, Set<ISynset> otherSynsets, double min, double max) {
        return (min != max)
                ? (avgDistance(synset, otherSynsets) - min) / (max - min)
                : 0.0;
    }

    private double avgDistance(ISynset synset, Set<ISynset> otherSynsets) {
        WS4JConfiguration.getInstance().setMemoryDB(false);
        WS4JConfiguration.getInstance().setMFS(true);
        var db = new MITWordNet(_dict);
        var relatednessCalculator = new WuPalmer(db);   // Resulting values are normalized [0, 1]
        var concept = new Concept(synset.toString());
        return otherSynsets.stream()
                .map(ss -> new Concept(ss.toString()))
                .mapToDouble(con -> 1 - relatednessCalculator.calcRelatednessOfSynsets(concept, con).getScore())
                .average()
                .orElse(2.0);
    }

    @Override
    public Set<String> interLingoRecord2Word(InterLingoRecord interLingoRecord) {
        var pos = switch (interLingoRecord.partOfSpeech()) {
            case NOUN -> POS.NOUN;
            case VERB -> POS.VERB;
            case ADJECTIVE -> POS.ADJECTIVE;
        };
        return _dict.getSynset(new SynsetID(interLingoRecord.num(), pos)).getWords().stream()
                .map(IWord::getLemma)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<InterLingoRecord> word2InterLingoRecord(Set<Integer> synsetIdSet) {
        return synsetIdSet.stream()
                .map(ss -> new InterLingoRecord(ss, InterLingoRecord.PartOfSpeech.NOUN))
                .collect(Collectors.toSet());
    }

    public double lowestSemanticDistance(Set<Integer> synsetIdSet1, Set<Integer> synsetIdSet2) {
        var posSet = Set.of(POS.NOUN, POS.VERB, POS.ADJECTIVE);
        WS4JConfiguration.getInstance().setMemoryDB(false);
        WS4JConfiguration.getInstance().setMFS(true);
        var db = new MITWordNet(_dict);
        var relatednessCalculator = new WuPalmer(db);   // Resulting values are normalized [0, 1]
        return synsetIdSet1.stream()
                .mapToDouble(synsetIdNum1 -> synsetIdSet2.stream()
                        .mapToDouble(synsetIdNum2 -> posSet.stream()
                                .mapToDouble(pos1 -> posSet.stream()
                                        .mapToDouble(pos2 -> {
                                            var synset1 = _dict.getSynset(new SynsetID(synsetIdNum1, pos1));
                                            var synset2 = _dict.getSynset(new SynsetID(synsetIdNum2, pos2));
                                            return relatednessCalculator
                                                    .calcRelatednessOfSynsets(
                                                            new Concept(synset1.toString()),
                                                            new Concept(synset2.toString()))
                                                    .getScore();
                                        })
                                        .min()
                                        .orElse(1.0))
                                .min()
                                .orElse(1.0))
                        .min()
                        .orElse(1.0))
                .min()
                .orElse(1.0);
    }
}