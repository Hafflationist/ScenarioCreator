package de.mrobohm.operations.linguistic.helpers.biglingo;

import de.tuebingen.uni.sfs.germanet.api.GermaNet;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

class GermaNetInterface implements LanguageCorpus {

    private final GermaNet _germanet;

    private static final String GERMANET_FOLDER = "src/main/resources/germanet/";

    public GermaNetInterface() throws XMLStreamException, IOException {
        var data_path = GERMANET_FOLDER + "GN_V150/GN_V150_XML";
        var freqListPath = GERMANET_FOLDER + "GN_V170-FreqLists/";
        var nounFreqListPath = freqListPath + "/noun_freqs_decow14_16.txt";
        var verbFreqListPath = freqListPath + "/verb_freqs_decow14_16.txt";
        var adjFreqListPath = freqListPath + "/adj_freqs_decow14_16.txt";
        System.out.println(data_path);
        _germanet = new GermaNet(data_path, nounFreqListPath, verbFreqListPath, adjFreqListPath);
        System.out.println("Germanet loaded.");
    }

    public Set<String> getSynonymes(String word) {
        // TODO: Man köntne durch andere Namen der Tabelle eingrenzen, welches Synset genau die richtige Bedeutung hat.
        // Sonst können Wörter wie "Bank" (Sitzgelegenheit vs Finanzinstitut?) komplett falsch synonymisiert werde!
        var synonymes = _germanet.getSynsets(word).stream().flatMap(ss -> ss.getAllOrthForms().stream());
        return synonymes.collect(Collectors.toSet());
    }


}