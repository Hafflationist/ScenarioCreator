package scenarioCreator.data.primitives.synset;

import scenarioCreator.data.Language;

public sealed interface GlobalSynset extends Comparable<GlobalSynset> permits GermanSynset, EnglishSynset {
    Language language();

    @Override
    default int compareTo(GlobalSynset gss) {
        return this.toString().compareTo(gss.toString());
    }
}
