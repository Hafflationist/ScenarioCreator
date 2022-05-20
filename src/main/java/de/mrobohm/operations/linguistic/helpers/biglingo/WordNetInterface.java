package de.mrobohm.operations.linguistic.helpers.biglingo;

import edu.mit.jwi.IDictionary;
import edu.mit.jwi.RAMDictionary;
import edu.mit.jwi.data.ILoadPolicy;
import edu.mit.jwi.item.IWord;
import edu.mit.jwi.item.POS;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Set;
import java.util.stream.Collectors;

public class WordNetInterface implements LanguageCorpus {

    private static final String DICT_FOLDER = "src/main/resources/wordnet/";
    private final IDictionary _dict;

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
                .collect(Collectors.toSet());
    }
}