package de.mrobohm.data.primitives;

import de.mrobohm.data.Language;

import java.util.Set;


public record StringPlusSemantical(String rawString, Language language, Set<Integer> estimatedSynsetIdSet)
        implements StringPlus {

    public StringPlusSemantical(StringPlus stringPlus, Set<Integer> estimatedSynsetIdSet) {
        this(stringPlus.rawString(), stringPlus.language(), estimatedSynsetIdSet);
    }

}