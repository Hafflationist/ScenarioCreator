package de.mrobohm.data.primitives.synset;

import de.mrobohm.data.Language;

public sealed interface GlobalSynset permits GermanSynset, EnglishSynset {
    Language language();
}
