package de.mrobohm.transformations.linguistic.helpers.biglingo;

import de.mrobohm.data.primitives.synset.EnglishSynset;
import de.mrobohm.data.primitives.synset.GlobalSynset;
import de.mrobohm.processing.transformations.linguistic.helpers.biglingo.LanguageCorpus;

import java.util.Map;
import java.util.Set;

public class LanguageCorpusMock implements LanguageCorpus {


    private final Map<String, Set<GlobalSynset>> _englishSynsetRecord2WordReturn;
    private final Set<EnglishSynset> _word2EnglishSynsetReturn;

    public LanguageCorpusMock(Map<String, Set<GlobalSynset>> englishSynsetRecord2WordReturn,
                              Set<EnglishSynset> word2EnglishSynsetReturn) {
        _englishSynsetRecord2WordReturn = englishSynsetRecord2WordReturn;
        _word2EnglishSynsetReturn = word2EnglishSynsetReturn;
    }

    @Override
    public Set<String> getSynonymes(String word) {
        return null;
    }

    @Override
    public Set<String> getSynonymes(Set<GlobalSynset> gssSet) {
        return null;
    }

    @Override
    public Set<GlobalSynset> estimateSynset(String word, Set<String> otherWordSet) {
        return null;
    }

    @Override
    public Map<String, Set<GlobalSynset>> englishSynsetRecord2Word(EnglishSynset ess) {
        return _englishSynsetRecord2WordReturn;
    }

    @Override
    public Set<EnglishSynset> word2EnglishSynset(Set<GlobalSynset> gssSet) {
        return _word2EnglishSynsetReturn;
    }

    @Override
    public double lowestSemanticDistance(Set<GlobalSynset> gssSet1, Set<GlobalSynset> gssSet2) {
        return 0;
    }
}
