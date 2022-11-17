package scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo;

import de.tuebingen.uni.sfs.germanet.api.GermaNet;
import de.tuebingen.uni.sfs.germanet.api.IliRecord;
import de.tuebingen.uni.sfs.germanet.api.SemRelMeasure;
import de.tuebingen.uni.sfs.germanet.api.Synset;
import scenarioCreator.data.primitives.synset.EnglishSynset;
import scenarioCreator.data.primitives.synset.GermanSynset;
import scenarioCreator.data.primitives.synset.GlobalSynset;
import scenarioCreator.data.primitives.synset.PartOfSpeech;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.StreamExtensions;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

public class GermaNetInterface implements LanguageCorpus {

    private static final SemRelMeasure SEM_REL_MEASURE = SemRelMeasure.Lin;
    private static final String GERMANET_FOLDER = "ScenarioCreator.Generation/resources/germanet/";
    private final GermaNet _germanet;
    private final GermaNet _germanetFreq;

    public GermaNetInterface() throws XMLStreamException, IOException {
        final var data_path = GERMANET_FOLDER + "GN_V160/GN_V160_XML";
        final var freqListPath = GERMANET_FOLDER + "GN_V160-FreqLists/";
        final var nounFreqListPath = freqListPath + "noun_freqs_decow14_16.txt";
        final var verbFreqListPath = freqListPath + "verb_freqs_decow14_16.txt";
        final var adjFreqListPath = freqListPath + "adj_freqs_decow14_16.txt";
        System.out.println(data_path);
        _germanet = new GermaNet(data_path, true);
        _germanetFreq = new GermaNet(data_path, nounFreqListPath, verbFreqListPath, adjFreqListPath);
        System.out.println("Germanet loaded.");
    }

    private static String englishSynsetToString(EnglishSynset ess) {
        final var posString = switch (ess.partOfSpeech()) {
            case NOUN -> "n";
            case VERB -> "v";
            case ADJECTIVE -> "a";
        };
        return ("eng30-" + String.format("%08d", ess.offset()) + "-" + posString).toLowerCase();
    }

    private static EnglishSynset stringToEnglishSynset(String str) {
        final var strPartArray = str.split("-");
        assert strPartArray.length == 3 : "Invalid interlingorecord string!";
        final var offset = Integer.parseInt(strPartArray[1]);
        final var pos = switch (strPartArray[2].toLowerCase()) {
            case "n" -> PartOfSpeech.NOUN;
            case "v" -> PartOfSpeech.VERB;
            case "a" -> PartOfSpeech.ADJECTIVE;
            default -> throw new IllegalStateException("Unexpected value: " + strPartArray[2].toLowerCase());
        };
        return new EnglishSynset(offset, pos);
    }

    public SortedSet<String> getSynonymes(String word) {
        final var synonymes = _germanet.getSynsets(word).stream().flatMap(ss -> ss.getAllOrthForms().stream());
        return synonymes.collect(Collectors.toCollection(TreeSet::new));
    }

    public SortedSet<String> getSynonymes(SortedSet<GlobalSynset> synsetIdSet) {
        return synsetIdSet.stream()
                .filter(gss -> gss instanceof GermanSynset)
                .map(gss -> (GermanSynset) gss)
                .flatMap(synsetId -> _germanet
                        .getSynsetByID(synsetId.id())
                        .getAllOrthForms()
                        .stream())
                .collect(Collectors.toCollection(TreeSet::new));
    }

    @Override
    public SortedSet<GlobalSynset> estimateSynset(String word, SortedSet<String> otherWordSet) {
        final var possibleSynsets = _germanet.getSynsets(word);
        final var otherSynsets = otherWordSet.stream()
                .flatMap(w -> _germanet.getSynsets(w).stream())
                .collect(Collectors.toCollection(TreeSet::new));

        final var distanceArray = possibleSynsets.stream().mapToDouble(ss -> avgDistance(ss, otherSynsets)).toArray();
        final var distanceMin = Arrays.stream(distanceArray).min().orElse(Double.POSITIVE_INFINITY);
        final var distanceMax = Arrays.stream(distanceArray).max().orElse(Double.NEGATIVE_INFINITY);

        return possibleSynsets.stream()
                .filter(ss -> avgDistance(ss, otherSynsets, distanceMin, distanceMax) < 0.1)
                .map(Synset::getId)
                .map(GermanSynset::new)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private double avgDistance(Synset synset, SortedSet<Synset> otherSynsets, double min, double max) {
        return (min != max)
                ? (avgDistance(synset, otherSynsets) - min) / (max - min)
                : 0.0;
    }

    private double avgDistance(Synset synset, SortedSet<Synset> otherSynsets) {
        try {
            final var semanticUtils = _germanetFreq.getSemanticUtils();
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

    public Map<String, SortedSet<GlobalSynset>> englishSynsetRecord2Word(EnglishSynset ess) {
        final var ilId = englishSynsetToString(ess);
        final var preMap = _germanet.getIliRecords().stream()
                .filter(ili -> ili.getPwn30Id().toLowerCase().equals(ilId))
                .flatMap(ili -> {
                    final var synset = _germanet.getLexUnitByID(ili.getLexUnitId()).getSynset();
                    final var gss = (GlobalSynset) new GermanSynset(synset.getId());
                    return synset.getAllOrthForms().stream().map(orthForm -> new Pair<>(orthForm, gss));
                })
                .collect(groupingBy(Pair::first));
        return preMap.keySet().stream().collect(Collectors.toMap(
                Function.identity(),
                orthForm -> preMap.get(orthForm).stream().map(Pair::second).collect(Collectors.toCollection(TreeSet::new))
        ));
    }

    @Override
    public SortedSet<EnglishSynset> word2EnglishSynset(SortedSet<GlobalSynset> synsetIdSet) {
        return synsetIdSet.stream()
                .filter(gss -> gss instanceof GermanSynset)
                .map(gss -> ((GermanSynset) gss).id())
                .map(_germanet::getSynsetByID)
                .flatMap(ss -> ss.getIliRecords().stream())
                .map(IliRecord::getPwn30Id)
                .map(GermaNetInterface::stringToEnglishSynset)
                .collect(Collectors.toCollection(TreeSet::new));
    }


    public double lowestSemanticDistance(SortedSet<GlobalSynset> synsetIdSet1, SortedSet<GlobalSynset> synsetIdSet2) {
        return synsetIdSet1.stream()
                .mapToDouble(gss1 -> synsetIdSet2.stream()
                        .mapToDouble(gss2 -> diff(gss1, gss2))
                        .filter(x -> !Double.isNaN(x))
                        .min()
                        .orElse(1.0))
                .min()
                .orElse(1.0);
    }

    @Override
    public double diff(GlobalSynset gss1, GlobalSynset gss2) {
        try {
            assert gss1 instanceof GermanSynset;
            assert gss2 instanceof GermanSynset;
            final var semanticUtils = _germanetFreq.getSemanticUtils();
            final var synset1 = _germanet.getSynsetByID(((GermanSynset) gss1).id());
            final var synset2 = _germanet.getSynsetByID(((GermanSynset) gss2).id());
            return 1.0 - Optional.ofNullable(semanticUtils.getSimilarity(
                    SEM_REL_MEASURE, synset1, synset2, 1
            )).orElse(0.0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String pickRandomEnglishWord(Random random) {
        final var rte = new RuntimeException("REEE");
        final var validIliStream = _germanet.getIliRecords().stream()
                .filter(ili -> !ili.getPwnWord().isBlank());
        return StreamExtensions
                .pickRandomOrThrowMultiple(
                        validIliStream, 1, rte, random
                )
                .map(IliRecord::getPwnWord)
                .toList()
                .get(0);
    }
}