package de.mrobohm.data.primitives;

import de.mrobohm.data.primitives.synset.GlobalSynset;

import java.util.Set;

public record StringPlusSemanticalSegment(String token, Set<GlobalSynset> gssSet) {

    public StringPlusSemanticalSegment(String token, Set<GlobalSynset> gssSet) {
        this.token = normalizeToken(token);
        this.gssSet = gssSet;
    }

    public StringPlusSemanticalSegment withToken(String newToken) {
        return new StringPlusSemanticalSegment(newToken, gssSet);
    }


    public static String normalizeToken(String orthForm) {
        return orthForm
                .replace("_", "")
                .replace("-", "")
                .replace(" ", "")
                .toLowerCase();
    }
}