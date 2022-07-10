package de.mrobohm.operations.linguistic.helpers.biglingo;

import de.mrobohm.data.primitives.synset.EnglishSynset;
import de.mrobohm.data.primitives.synset.GlobalSynset;

import java.util.Map;
import java.util.Set;

public class LanguageCorpusMock implements LanguageCorpus {

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
        return null;
    }

    @Override
    public Set<EnglishSynset> word2EnglishSynset(Set<GlobalSynset> gssSet) {
        return null;
    }

    @Override
    public double lowestSemanticDistance(Set<GlobalSynset> gssSet1, Set<GlobalSynset> gssSet2) {
        return 0;
    }
}
