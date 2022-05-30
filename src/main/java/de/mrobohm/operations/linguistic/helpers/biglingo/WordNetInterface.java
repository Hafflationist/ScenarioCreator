package de.mrobohm.operations.linguistic.helpers.biglingo;

import edu.mit.jwi.IRAMDictionary;
import edu.mit.jwi.RAMDictionary;
import edu.mit.jwi.data.ILoadPolicy;
import edu.mit.jwi.item.ISynset;
import edu.mit.jwi.item.IWord;
import edu.mit.jwi.item.POS;
import edu.mit.jwi.item.SynsetID;
import edu.uniba.di.lacam.kdde.lexical_db.MITWordNet;
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
        // Weshalb man nicht zwischen Synsets die semantische Ähnlichkeit bestimmen kann, verstehen ich nicht.
        if (_dict.getIndexWord(word, POS.NOUN) == null)
        {
            return Set.of();
        }

        var possibleSynsets = _dict
                .getIndexWord(word, POS.NOUN)
                .getWordIDs().stream()
                .map(_dict::getWord)
                .map(IWord::getLemma)
                .collect(Collectors.toSet());
        var otherSynsets = otherWordSet.stream()
                .filter(w -> _dict.getIndexWord(w, POS.NOUN) != null)
                .flatMap(w -> _dict
                        .getIndexWord(w, POS.NOUN)
                        .getWordIDs().stream()
                        .map(_dict::getWord)
                        .map(IWord::getLemma))
                .collect(Collectors.toSet());

        var distanceArray = possibleSynsets.stream().mapToDouble(ss -> avgDistance(ss, otherSynsets)).toArray();
        var distanceMin = Arrays.stream(distanceArray).min().orElse(Double.POSITIVE_INFINITY);
        var distanceMax = Arrays.stream(distanceArray).max().orElse(Double.NEGATIVE_INFINITY);

        return possibleSynsets.stream()
                .filter(ss -> avgDistance(ss, otherSynsets, distanceMin, distanceMax) < 0.1)
                .flatMap(w -> _dict
                        .getIndexWord(w, POS.NOUN)
                        .getWordIDs().stream())
                .map(_dict::getWord)
                .map(IWord::getSynset)
                .map(ISynset::getOffset)
                .collect(Collectors.toSet());
    }

    private double avgDistance(String synset, Set<String> otherSynsets, double min, double max) {
        return (min != max)
                ? (avgDistance(synset, otherSynsets) - min) / (max - min)
                : 0.0;
    }

    private double avgDistance(String synset, Set<String> otherSynsets) {
        WS4JConfiguration.getInstance().setMemoryDB(false);
        WS4JConfiguration.getInstance().setMFS(true);
        var db = new MITWordNet(_dict);
        var relatednessCalculator = new WuPalmer(db);
        return otherSynsets.stream()
                .mapToDouble(ss -> 1 - relatednessCalculator.calcRelatednessOfWords(synset, ss))
                .average()
                .orElse(2.0);
    }
}