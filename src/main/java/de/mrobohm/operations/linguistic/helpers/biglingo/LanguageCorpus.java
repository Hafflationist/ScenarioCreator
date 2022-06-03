package de.mrobohm.operations.linguistic.helpers.biglingo;

import java.util.Set;

public interface LanguageCorpus {
    Set<String> getSynonymes(String word);

    Set<String> getSynonymes(Set<Integer> synsetIdSet);

    Set<Integer> estimateSynset(String word, Set<String> otherWordSet);

    Set<String> interLingoRecord2Word(InterLingoRecord interLingoRecord);

    Set<InterLingoRecord> word2InterLingoRecord(String word, Set<String> context);
}