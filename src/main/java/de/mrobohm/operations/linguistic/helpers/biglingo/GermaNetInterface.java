package de.mrobohm.operations.linguistic.helpers.biglingo;

import de.tuebingen.uni.sfs.germanet.api.GermaNet;
import de.tuebingen.uni.sfs.germanet.api.IliRecord;
import de.tuebingen.uni.sfs.germanet.api.SemRelMeasure;
import de.tuebingen.uni.sfs.germanet.api.Synset;
import edu.mit.jwi.item.POS;
import edu.mit.jwi.item.SynsetID;
import edu.uniba.di.lacam.kdde.lexical_db.MITWordNet;
import edu.uniba.di.lacam.kdde.lexical_db.data.Concept;
import edu.uniba.di.lacam.kdde.ws4j.similarity.WuPalmer;
import edu.uniba.di.lacam.kdde.ws4j.util.WS4JConfiguration;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class GermaNetInterface implements LanguageCorpus {

    private static final SemRelMeasure SEM_REL_MEASURE = SemRelMeasure.WuAndPalmer;
    private static final String GERMANET_FOLDER = "src/main/resources/germanet/";
    private final GermaNet _germanet;
    private final GermaNet _germanetFreq;

    public GermaNetInterface() throws XMLStreamException, IOException {
        var data_path = GERMANET_FOLDER + "GN_V160/GN_V160_XML";
        var freqListPath = GERMANET_FOLDER + "GN_V160-FreqLists/";
        var nounFreqListPath = freqListPath + "noun_freqs_decow14_16.txt";
        var verbFreqListPath = freqListPath + "verb_freqs_decow14_16.txt";
        var adjFreqListPath = freqListPath + "adj_freqs_decow14_16.txt";
        System.out.println(data_path);
        _germanet = new GermaNet(data_path, true);
        _germanetFreq = new GermaNet(data_path, nounFreqListPath, verbFreqListPath, adjFreqListPath);
        System.out.println("Germanet loaded.");
    }

    private static String interLingoRecordToString(InterLingoRecord interLingoRecord) {
        var posString = switch (interLingoRecord.partOfSpeech()) {
            case NOUN -> "n";
            case VERB -> "v";
            case ADJECTIVE -> "a";
        };
        return ("eng30-" + String.format("%07d", interLingoRecord.num()) + "-" + posString).toLowerCase();
    }

    private static InterLingoRecord stringToInterLingoRecord(String str) {
        var strPartArray = str.split("-");
        assert strPartArray.length == 3 : "Invalid interlingorecord string!";
        var num = Integer.parseInt(strPartArray[1]);
        var pos = switch (strPartArray[2].toLowerCase()) {
            case "n" -> InterLingoRecord.PartOfSpeech.NOUN;
            case "v" -> InterLingoRecord.PartOfSpeech.VERB;
            case "a" -> InterLingoRecord.PartOfSpeech.ADJECTIVE;
            default -> throw new IllegalStateException("Unexpected value: " + strPartArray[2].toLowerCase());
        };
        return new InterLingoRecord(num, pos);
    }

    public Set<String> getSynonymes(String word) {
        var synonymes = _germanet.getSynsets(word).stream().flatMap(ss -> ss.getAllOrthForms().stream());
        return synonymes.collect(Collectors.toSet());
    }

    public Set<String> getSynonymes(Set<Integer> synsetIdSet) {
        return synsetIdSet.stream()
                .flatMap(synsetId -> _germanet
                        .getSynsetByID(synsetId)
                        .getAllOrthForms()
                        .stream())
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Integer> estimateSynset(String word, Set<String> otherWordSet) {
        var possibleSynsets = _germanet.getSynsets(word);
        var otherSynsets = otherWordSet.stream()
                .flatMap(w -> _germanet.getSynsets(w).stream())
                .collect(Collectors.toSet());

        var distanceArray = possibleSynsets.stream().mapToDouble(ss -> avgDistance(ss, otherSynsets)).toArray();
        var distanceMin = Arrays.stream(distanceArray).min().orElse(Double.POSITIVE_INFINITY);
        var distanceMax = Arrays.stream(distanceArray).max().orElse(Double.NEGATIVE_INFINITY);

        return possibleSynsets.stream()
                .filter(ss -> avgDistance(ss, otherSynsets, distanceMin, distanceMax) < 0.1)
                .map(Synset::getId)
                .collect(Collectors.toSet());
    }

    private double avgDistance(Synset synset, Set<Synset> otherSynsets, double min, double max) {
        return (min != max)
                ? (avgDistance(synset, otherSynsets) - min) / (max - min)
                : 0.0;
    }

    private double avgDistance(Synset synset, Set<Synset> otherSynsets) {
        try {
            var semanticUtils = _germanetFreq.getSemanticUtils();
            return otherSynsets.stream()
                    .mapToDouble(ss -> 1 - Optional
                            .ofNullable(semanticUtils.getSimilarity(SEM_REL_MEASURE, synset, ss, 1))
                            .orElse(0.0))
                    .average()
                    .orElse(2.0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<String> interLingoRecord2Word(InterLingoRecord interLingoRecord) {
        var ilId = interLingoRecordToString(interLingoRecord);
        return _germanet.getIliRecords().stream()
                .filter(ili -> ili.getPwn30Id().toLowerCase().equals(ilId))
                .findFirst()
                .map(ili -> {
                    var synset = _germanet.getLexUnitByID(ili.getLexUnitId()).getSynset();
                    return (Set<String>) new HashSet<>(synset.getAllOrthForms());
                }).orElse(Set.of());
    }

    @Override
    public Set<InterLingoRecord> word2InterLingoRecord(Set<Integer> synsetIdSet) {
        return synsetIdSet.stream()
                .map(_germanet::getSynsetByID)
                .flatMap(ss -> ss.getIliRecords().stream())
                .map(IliRecord::getPwn30Id)
                .map(GermaNetInterface::stringToInterLingoRecord)
                .collect(Collectors.toSet());
    }

    public double lowestSemanticDistance(Set<Integer> synsetIdSet1, Set<Integer> synsetIdSet2) {
        try {
            var semanticUtils = _germanetFreq.getSemanticUtils();
            return synsetIdSet1.stream()
                    .map(_germanet::getSynsetByID)
                    .mapToDouble(synset1 -> synsetIdSet2.stream()
                            .map(_germanet::getSynsetByID)
                            .mapToDouble(synset2 -> semanticUtils.getSimilarity(
                                    SEM_REL_MEASURE, synset1, synset2, 1
                            ))
                            .min()
                            .orElse(1.0))
                    .min()
                    .orElse(1.0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}