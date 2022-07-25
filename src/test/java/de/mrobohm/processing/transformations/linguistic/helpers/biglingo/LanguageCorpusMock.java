package de.mrobohm.processing.transformations.linguistic.helpers.biglingo;

import de.mrobohm.data.primitives.synset.EnglishSynset;
import de.mrobohm.data.primitives.synset.GlobalSynset;
import de.mrobohm.processing.transformations.linguistic.helpers.biglingo.LanguageCorpus;

import java.util.Map;
import java.util.SortedSet;

public class LanguageCorpusMock implements LanguageCorpus {


    private final Map<String, SortedSet<GlobalSynset>> _englishSynsetRecord2WordReturn;
    private final SortedSet<EnglishSynset> _word2EnglishSynsetReturn;

    public LanguageCorpusMock(Map<String, SortedSet<GlobalSynset>> englishSynsetRecord2WordReturn,
                              SortedSet<EnglishSynset> word2EnglishSynsetReturn) {
        _englishSynsetRecord2WordReturn = englishSynsetRecord2WordReturn;
        _word2EnglishSynsetReturn = word2EnglishSynsetReturn;
    }

    @Override
    public SortedSet<String> getSynonymes(String word) {
        return null;
    }

    @Override
    public SortedSet<String> getSynonymes(SortedSet<GlobalSynset> gssSet) {
        return null;
    }

    @Override
    public SortedSet<GlobalSynset> estimateSynset(String word, SortedSet<String> otherWordSet) {
        return null;
    }

    @Override
    public Map<String, SortedSet<GlobalSynset>> englishSynsetRecord2Word(EnglishSynset ess) {
        return _englishSynsetRecord2WordReturn;
    }

    @Override
    public SortedSet<EnglishSynset> word2EnglishSynset(SortedSet<GlobalSynset> gssSet) {
        return _word2EnglishSynsetReturn;
    }

    @Override
    public double lowestSemanticDistance(SortedSet<GlobalSynset> gssSet1, SortedSet<GlobalSynset> gssSet2) {
        return 0;
    }
}
