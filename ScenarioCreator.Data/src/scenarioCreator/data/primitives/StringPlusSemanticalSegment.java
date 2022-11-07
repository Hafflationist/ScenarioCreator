package scenarioCreator.data.primitives;

import scenarioCreator.data.primitives.synset.GlobalSynset;
import org.jetbrains.annotations.NotNull;

import java.util.SortedSet;

public record StringPlusSemanticalSegment(String token, SortedSet<GlobalSynset> gssSet)
        implements Comparable<StringPlusSemanticalSegment> {

    public StringPlusSemanticalSegment(String token, SortedSet<GlobalSynset> gssSet) {
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

    @Override
    public int compareTo(@NotNull StringPlusSemanticalSegment spss) {
        return this.toString().compareTo(spss.toString());
    }
}