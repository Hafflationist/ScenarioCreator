package de.mrobohm.processing.transformations.linguistic.helpers.biglingo;

import de.mrobohm.data.primitives.synset.EnglishSynset;
import de.mrobohm.data.primitives.synset.GlobalSynset;

import java.util.Map;
import java.util.SortedSet;

public interface LanguageCorpus {
    SortedSet<String> getSynonymes(String word);

    SortedSet<String> getSynonymes(SortedSet<GlobalSynset> gssSet);

    SortedSet<GlobalSynset> estimateSynset(String word, SortedSet<String> otherWordSet);

    Map<String, SortedSet<GlobalSynset>> englishSynsetRecord2Word(EnglishSynset ess);

    SortedSet<EnglishSynset> word2EnglishSynset(SortedSet<GlobalSynset> gssSet);

    double lowestSemanticDistance(SortedSet<GlobalSynset> gssSet1, SortedSet<GlobalSynset> gssSet2);
}