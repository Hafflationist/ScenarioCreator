package de.mrobohm.processing.transformations.linguistic.helpers;

import de.mrobohm.data.Language;
import de.mrobohm.data.primitives.NamingConvention;
import de.mrobohm.data.primitives.StringPlusSemantical;
import de.mrobohm.data.primitives.StringPlusSemanticalSegment;
import de.mrobohm.data.primitives.synset.EnglishSynset;
import de.mrobohm.data.primitives.synset.GermanSynset;
import de.mrobohm.data.primitives.synset.GlobalSynset;
import de.mrobohm.data.primitives.synset.PartOfSpeech;
import de.mrobohm.processing.transformations.linguistic.helpers.biglingo.LanguageCorpusMock;
import de.mrobohm.processing.transformations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import de.mrobohm.utils.SSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;

class TranslationTest {

    private Translation getTranslation(Map<String, SortedSet<GlobalSynset>> englishSynsetRecord2WordReturn,
                                       SortedSet<EnglishSynset> word2EnglishSynsetReturn) {
        final var languageCorpusMock = new LanguageCorpusMock(englishSynsetRecord2WordReturn, word2EnglishSynsetReturn);
        final var ulc = new UnifiedLanguageCorpus(
                Map.of(Language.German, languageCorpusMock, Language.English, languageCorpusMock)
        );
        return new Translation(ulc);
    }

    private Translation getTranslation() {
        final var languageCorpusMock = new LanguageCorpusMock(null, null);
        final var ulc = new UnifiedLanguageCorpus(
                Map.of(Language.German, languageCorpusMock, Language.English, languageCorpusMock)
        );
        return new Translation(ulc);
    }

    @Test
    void translate() {
        // --- Arrange
        final var validSegment = new StringPlusSemanticalSegment("Katze", SSet.of(new GermanSynset(1)));
        final var invalidSegment = new StringPlusSemanticalSegment("Unsinnnnn", SSet.of());
        final var expectedSegment = new StringPlusSemanticalSegment(
                "Cat", SSet.of(new EnglishSynset(2, PartOfSpeech.NOUN))
        );
        final var stringPlus = new StringPlusSemantical(List.of(
                validSegment, invalidSegment
        ), NamingConvention.CAMELCASE);

        final var englishSynsetRecord2WordReturn =
                Map.of(expectedSegment.token(), expectedSegment.gssSet());
        SortedSet<EnglishSynset> word2EnglishSynsetReturn = SSet.of(new EnglishSynset(-1, PartOfSpeech.NOUN));
        final var translation = getTranslation(englishSynsetRecord2WordReturn, word2EnglishSynsetReturn);

        // --- Act
        final var newStringPlusOpt = translation.translate(stringPlus, new Random());

        // --- Assert
        Assertions.assertTrue(newStringPlusOpt.isPresent());
        final var newStringPlus = newStringPlusOpt.get();
        Assertions.assertNotEquals(stringPlus, newStringPlus);
        Assertions.assertTrue(newStringPlus instanceof StringPlusSemantical);
        final var newSps = (StringPlusSemantical) newStringPlus;
        Assertions.assertEquals(stringPlus.namingConvention(), newSps.namingConvention());
        Assertions.assertEquals(stringPlus.segmentList().size(), newSps.segmentList().size());
        Assertions.assertEquals(stringPlus.segmentList().get(1), newSps.segmentList().get(1));
        Assertions.assertNotEquals(stringPlus.segmentList().get(0), newSps.segmentList().get(0));
        Assertions.assertEquals(expectedSegment, newSps.segmentList().get(0));
    }

    @Test
    void canBeTranslatedFalseEmptySps() {
        // --- Arrange
        final var stringPlus = new StringPlusSemantical(List.of(), NamingConvention.CAMELCASE);
        final var translation = getTranslation();

        // --- Act
        final var canBeTranslated = translation.canBeTranslated(stringPlus);

        // --- Assert
        Assertions.assertFalse(canBeTranslated);
    }

    @Test
    void canBeTranslatedFalseInvalidSps() {
        // --- Arrange
        final var stringPlus = new StringPlusSemantical(List.of(
                new StringPlusSemanticalSegment("808", SSet.of()),
                new StringPlusSemanticalSegment("111", SSet.of())
        ), NamingConvention.CAMELCASE);
        final var translation = getTranslation();

        // --- Act
        final var canBeTranslated = translation.canBeTranslated(stringPlus);

        // --- Assert
        Assertions.assertFalse(canBeTranslated);
    }

    @Test
    void canBeTranslatedTrue() {
        // --- Arrange
        final var stringPlus = new StringPlusSemantical(List.of(
                new StringPlusSemanticalSegment("katze", SSet.of(new GermanSynset(1))),
                new StringPlusSemanticalSegment("hugo", SSet.of())
        ), NamingConvention.CAMELCASE);
        final var translation = getTranslation();

        // --- Act
        final var canBeTranslated = translation.canBeTranslated(stringPlus);

        // --- Assert
        Assertions.assertTrue(canBeTranslated);
    }
}