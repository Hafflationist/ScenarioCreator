package scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo;

import scenarioCreator.data.primitives.synset.EnglishSynset;
import scenarioCreator.data.primitives.synset.GlobalSynset;

import java.util.Map;
import java.util.SortedSet;

public interface LanguageCorpus {
    SortedSet<String> getSynonymes(String word);

    SortedSet<String> getSynonymes(SortedSet<GlobalSynset> gssSet);

    SortedSet<GlobalSynset> estimateSynset(String word, SortedSet<String> otherWordSet);

    Map<String, SortedSet<GlobalSynset>> englishSynsetRecord2Word(EnglishSynset ess);

    SortedSet<EnglishSynset> word2EnglishSynset(SortedSet<GlobalSynset> gssSet);

    double diff(GlobalSynset gss1, GlobalSynset gss2);
}