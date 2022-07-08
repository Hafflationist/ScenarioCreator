package de.mrobohm.data.primitives;

import de.mrobohm.data.primitives.synset.GlobalSynset;

import java.util.Set;

public record StringPlusSemanticalSegment(String token, Set<GlobalSynset> gssSet) {

    public StringPlusSemanticalSegment withToken(String newToken) {
        return new StringPlusSemanticalSegment(newToken, gssSet);
    }
}