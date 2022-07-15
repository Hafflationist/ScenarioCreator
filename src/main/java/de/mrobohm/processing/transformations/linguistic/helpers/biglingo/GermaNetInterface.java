package de.mrobohm.processing.transformations.linguistic.helpers.biglingo;

import de.mrobohm.data.primitives.synset.EnglishSynset;
import de.mrobohm.data.primitives.synset.GermanSynset;
import de.mrobohm.data.primitives.synset.GlobalSynset;
import de.mrobohm.data.primitives.synset.PartOfSpeech;
import de.mrobohm.utils.Pair;
import de.mrobohm.utils.StreamExtensions;
import de.tuebingen.uni.sfs.germanet.api.GermaNet;
import de.tuebingen.uni.sfs.germanet.api.IliRecord;
import de.tuebingen.uni.sfs.germanet.api.SemRelMeasure;
import de.tuebingen.uni.sfs.germanet.api.Synset;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

public class GermaNetInterface implements LanguageCorpus {

    private static final SemRelMeasure SEM_REL_MEASURE = SemRelMeasure.Lin;
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

    private static String englishSynsetToString(EnglishSynset ess) {
        var posString = switch (ess.partOfSpeech()) {
            case NOUN -> "n";
            case VERB -> "v";
            case ADJECTIVE -> "a";
        };
        return ("eng30-" + String.format("%08d", ess.offset()) + "-" + posString).toLowerCase();
    }

    private static EnglishSynset stringToEnglishSynset(String str) {
        var strPartArray = str.split("-");
        assert strPartArray.length == 3 : "Invalid interlingorecord string!";
        var offset = Integer.parseInt(strPartArray[1]);
        var pos = switch (strPartArray[2].toLowerCase()) {
            case "n" -> PartOfSpeech.NOUN;
            case "v" -> PartOfSpeech.VERB;
            case "a" -> PartOfSpeech.ADJECTIVE;
            default -> throw new IllegalStateException("Unexpected value: " + strPartArray[2].toLowerCase());
        };
        return new EnglishSynset(offset, pos);
    }

    public Set<String> getSynonymes(String word) {
        var synonymes = _germanet.getSynsets(word).stream().flatMap(ss -> ss.getAllOrthForms().stream());
        return synonymes.collect(Collectors.toSet());
    }

    public Set<String> getSynonymes(Set<GlobalSynset> synsetIdSet) {
        return synsetIdSet.stream()
                .filter(gss -> gss instanceof GermanSynset)
                .map(gss -> (GermanSynset) gss)
                .flatMap(synsetId -> _germanet
                        .getSynsetByID(synsetId.id())
                        .getAllOrthForms()
                        .stream())
                .collect(Collectors.toSet());
    }

    @Override
    public Set<GlobalSynset> estimateSynset(String word, Set<String> otherWordSet) {
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
                .map(GermanSynset::new)
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

    public Map<String, Set<GlobalSynset>> englishSynsetRecord2Word(EnglishSynset ess) {
        var ilId = englishSynsetToString(ess);
        var preMap = _germanet.getIliRecords().stream()
                .filter(ili -> ili.getPwn30Id().toLowerCase().equals(ilId))
                .flatMap(ili -> {
                    var synset = _germanet.getLexUnitByID(ili.getLexUnitId()).getSynset();
                    var gss = (GlobalSynset) new GermanSynset(synset.getId());
                    return synset.getAllOrthForms().stream().map(orthForm -> new Pair<>(orthForm, gss));
                })
                .collect(groupingBy(Pair::first));
        return preMap.keySet().stream().collect(Collectors.toMap(
                Function.identity(),
                orthForm -> preMap.get(orthForm).stream().map(Pair::second).collect(Collectors.toSet())
        ));
    }

    @Override
    public Set<EnglishSynset> word2EnglishSynset(Set<GlobalSynset> synsetIdSet) {
        return synsetIdSet.stream()
                .filter(gss -> gss instanceof GermanSynset)
                .map(gss -> ((GermanSynset) gss).id())
                .map(_germanet::getSynsetByID)
                .flatMap(ss -> ss.getIliRecords().stream())
                .map(IliRecord::getPwn30Id)
                .map(GermaNetInterface::stringToEnglishSynset)
                .collect(Collectors.toSet());
    }

    public double lowestSemanticDistance(Set<GlobalSynset> synsetIdSet1, Set<GlobalSynset> synsetIdSet2) {
        try {
            var semanticUtils = _germanetFreq.getSemanticUtils();
            return synsetIdSet1.stream()
                    .filter(gss -> gss instanceof GermanSynset)
                    .map(gss -> ((GermanSynset) gss).id())
                    .map(_germanet::getSynsetByID)
                    .mapToDouble(synset1 -> synsetIdSet2.stream()
                            .filter(gss -> gss instanceof GermanSynset)
                            .map(gss -> ((GermanSynset) gss).id())
                            .map(_germanet::getSynsetByID)
                            .mapToDouble(synset2 -> 1.0 - semanticUtils.getSimilarity(
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

    public String pickRandomEnglishWord(Random random) {
        var rte = new RuntimeException("REEE");
        return StreamExtensions
                .pickRandomOrThrowMultiple(
                        _germanet.getIliRecords().stream().filter(ili -> !ili.getPwnWord().isBlank()), 1, rte, random
                )
                .map(IliRecord::getPwnWord)
                .toList()
                .get(0);
    }
}