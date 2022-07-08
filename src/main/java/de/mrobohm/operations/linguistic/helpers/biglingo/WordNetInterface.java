package de.mrobohm.operations.linguistic.helpers.biglingo;

import de.mrobohm.data.primitives.synset.EnglishSynset;
import de.mrobohm.data.primitives.synset.GlobalSynset;
import de.mrobohm.data.primitives.synset.PartOfSpeech;
import edu.mit.jwi.IRAMDictionary;
import edu.mit.jwi.RAMDictionary;
import edu.mit.jwi.data.ILoadPolicy;
import edu.mit.jwi.item.ISynset;
import edu.mit.jwi.item.IWord;
import edu.mit.jwi.item.POS;
import edu.mit.jwi.item.SynsetID;
import edu.uniba.di.lacam.kdde.lexical_db.MITWordNet;
import edu.uniba.di.lacam.kdde.lexical_db.data.Concept;
import edu.uniba.di.lacam.kdde.ws4j.similarity.Lin;
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
    public Set<String> getSynonymes(Set<GlobalSynset> synsetIdSet) {
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
                .collect(Collectors.toSet());
    }

    @Override
    public Set<GlobalSynset> estimateSynset(String word, Set<String> otherWordSet) {
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
                .map(ss -> new EnglishSynset(ss.getOffset(), posToPartOfSpeech(ss.getPOS())))
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
                .mapToDouble(con -> 1.0 - relatednessCalculator.calcRelatednessOfSynsets(concept, con).getScore())
                .average()
                .orElse(2.0);
    }

    @Override
    public Set<String> englishSynsetRecord2Word(EnglishSynset ess) {
        var pos = partOfSpeechToPos(ess.partOfSpeech());
        return _dict.getSynset(new SynsetID(ess.offset(), pos)).getWords().stream()
                .map(IWord::getLemma)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<EnglishSynset> word2EnglishSynset(Set<GlobalSynset> gssSet) {
        return gssSet.stream()
                .filter(gss -> gss instanceof EnglishSynset)
                .map(gss -> (EnglishSynset) gss)
                .collect(Collectors.toSet());
    }

    public double lowestSemanticDistance(Set<GlobalSynset> synsetIdSet1, Set<GlobalSynset> synsetIdSet2) {
        WS4JConfiguration.getInstance().setMemoryDB(false);
        WS4JConfiguration.getInstance().setMFS(true);
        var db = new MITWordNet(_dict);
        var relatednessCalculator = new Lin(db);   // Resulting values are normalized [0, 1]
        return synsetIdSet1.stream()
                .filter(gss -> gss instanceof EnglishSynset)
                .map(gss -> (EnglishSynset) gss)
                .mapToDouble(ess1 -> synsetIdSet2.stream()
                        .filter(gss -> gss instanceof EnglishSynset)
                        .map(gss -> (EnglishSynset) gss)
                        .mapToDouble(ess2 -> {
                            var synsetId1 = new SynsetID(ess1.offset(), partOfSpeechToPos(ess1.partOfSpeech()));
                            var synsetId2 = new SynsetID(ess2.offset(), partOfSpeechToPos(ess2.partOfSpeech()));
                            var pos1 = edu.uniba.di.lacam.kdde.lexical_db.item.POS.getPOS(synsetId1.toString().toLowerCase().charAt(13));
                            var pos2 = edu.uniba.di.lacam.kdde.lexical_db.item.POS.getPOS(synsetId2.toString().toLowerCase().charAt(13));
                            var relatedness = relatednessCalculator
                                    .calcRelatednessOfSynsets(
                                            new Concept(synsetId1.toString(), pos1),
                                            new Concept(synsetId2.toString(), pos2))
                                    .getScore();
                            if (relatedness > 1.0) {
                                System.out.println(synsetId1);
                                System.out.println(synsetId2);
                            }
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