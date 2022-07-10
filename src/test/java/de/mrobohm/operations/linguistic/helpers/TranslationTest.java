package de.mrobohm.operations.linguistic.helpers;

import de.mrobohm.data.Language;
import de.mrobohm.data.primitives.NamingConvention;
import de.mrobohm.data.primitives.StringPlusSemantical;
import de.mrobohm.data.primitives.StringPlusSemanticalSegment;
import de.mrobohm.data.primitives.synset.GermanSynset;
import de.mrobohm.operations.linguistic.helpers.biglingo.LanguageCorpusMock;
import de.mrobohm.operations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

class TranslationTest {

    private Translation getTranslation() {
        var languageCorpusMock = new LanguageCorpusMock();
        var ulc = new UnifiedLanguageCorpus(
                Map.of(Language.German, languageCorpusMock, Language.English, languageCorpusMock)
        );
        return new Translation(ulc);
    }

    // TODO: MORE TESTS!!!

    @Test
    void canBeTranslatedFalseEmptySps() {
        // --- Arrange
        var stringPlus = new StringPlusSemantical(List.of(), NamingConvention.CAMELCASE);
        var translation = getTranslation();

        // --- Act
        var canBeTranslated = translation.canBeTranslated(stringPlus);

        // --- Assert
        Assertions.assertFalse(canBeTranslated);
    }

    @Test
    void canBeTranslatedFalseInvalidSps() {
        // --- Arrange
        var stringPlus = new StringPlusSemantical(List.of(
                new StringPlusSemanticalSegment("hugo", Set.of()),
                new StringPlusSemanticalSegment("hugo", Set.of())
        ), NamingConvention.CAMELCASE);
        var translation = getTranslation();

        // --- Act
        var canBeTranslated = translation.canBeTranslated(stringPlus);

        // --- Assert
        Assertions.assertFalse(canBeTranslated);
    }

    @Test
    void canBeTranslatedTrue() {
        // --- Arrange
        var stringPlus = new StringPlusSemantical(List.of(
                new StringPlusSemanticalSegment("katze", Set.of(new GermanSynset(1))),
                new StringPlusSemanticalSegment("hugo", Set.of())
        ), NamingConvention.CAMELCASE);
        var translation = getTranslation();

        // --- Act
        var canBeTranslated = translation.canBeTranslated(stringPlus);

        // --- Assert
        Assertions.assertTrue(canBeTranslated);
    }
}