package de.mrobohm.processing.transformations.linguistic.helpers.biglingo;

import de.mrobohm.data.primitives.synset.EnglishSynset;
import de.mrobohm.data.primitives.synset.GlobalSynset;

import java.util.Map;
import java.util.Set;

public interface LanguageCorpus {
    Set<String> getSynonymes(String word);

    Set<String> getSynonymes(Set<GlobalSynset> gssSet);

    Set<GlobalSynset> estimateSynset(String word, Set<String> otherWordSet);

    Map<String, Set<GlobalSynset>> englishSynsetRecord2Word(EnglishSynset ess);

    Set<EnglishSynset> word2EnglishSynset(Set<GlobalSynset> gssSet);

    double lowestSemanticDistance(Set<GlobalSynset> gssSet1, Set<GlobalSynset> gssSet2);
}