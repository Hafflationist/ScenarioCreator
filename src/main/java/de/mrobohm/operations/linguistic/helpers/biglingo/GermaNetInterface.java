package de.mrobohm.operations.linguistic.helpers.biglingo;

import de.tuebingen.uni.sfs.germanet.api.GermaNet;
import de.tuebingen.uni.sfs.germanet.api.SemRelMeasure;
import de.tuebingen.uni.sfs.germanet.api.Synset;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class GermaNetInterface implements LanguageCorpus {

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

    public Set<String> getSynonymes(String word) {
        // TODO: Man köntne durch andere Namen der Tabelle eingrenzen, welches Synset genau die richtige Bedeutung hat.
        // Sonst können Wörter wie "Bank" (Sitzgelegenheit vs Finanzinstitut?) komplett falsch synonymisiert werde!
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
        var otherSynsets = otherWordSet.stream()
                .flatMap(w -> _germanet.getSynsets(w).stream())
                .collect(Collectors.toSet());
        var possibleSynsets = _germanet.getSynsets(word);

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
                            .ofNullable(semanticUtils.getSimilarity(SemRelMeasure.WuAndPalmer, synset, ss, 1))
                            .orElse(0.0))
                    .average()
                    .orElse(2.0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}