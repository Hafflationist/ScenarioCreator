package de.mrobohm.processing.transformations.linguistic.helpers;

import de.mrobohm.data.Language;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.primitives.StringPlusSemantical;
import de.mrobohm.data.primitives.StringPlusSemanticalSegment;
import de.mrobohm.data.primitives.synset.GlobalSynset;
import de.mrobohm.processing.transformations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import de.mrobohm.utils.SSet;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

public class Translation {

    private final UnifiedLanguageCorpus _corpus;

    public Translation(UnifiedLanguageCorpus corpus) {
        _corpus = corpus;
    }

    private Language chooseDifferentLanguage(Language exception, Random random) {
        final var rte = new RuntimeException("Fatal error! Not enough languages defined!");
        final var newLanguageStream = Arrays.stream(Language.values())
                .filter(lang -> !Set.of(Language.Technical, Language.Mixed, exception).contains(lang));
        final var newLanguage = StreamExtensions.pickRandomOrThrow(newLanguageStream, rte, random);
        assert !newLanguage.equals(exception) : "Local bug found!";
        return newLanguage;
    }

    @NotNull
    private Optional<StringPlusSemanticalSegment> translate(StringPlusSemanticalSegment segment, Random random) {
        if (segment.gssSet().isEmpty()) {
            return Optional.empty();
        }
        final var rte = new RuntimeException("Should not happen.");
        final var randomGss = StreamExtensions.pickRandomOrThrow(segment.gssSet().stream(), rte, random);
        final var targetLanguage = chooseDifferentLanguage(randomGss.language(), random);
        final var translationPossibilitySet = _corpus.translate(segment, targetLanguage);
        return StreamExtensions.tryPickRandom(translationPossibilitySet.stream(), random);
    }

    @NotNull
    public Optional<StringPlus> translate(StringPlus name, Random random) {
        return translateInner(name, random, 0, 10);
    }

    public Optional<StringPlus> translateInner(StringPlus name, Random random, int acc, int max) {
        if (acc == max) {
            return Optional.empty();
        }
        return switch (name) {
            case StringPlusNaked spn -> Optional.of(translateNaked(spn, random));
            case StringPlusSemantical sps -> {
                final var rte = new RuntimeException("StringPlus without segments are invalid");
                final var validSegmentStream = sps.segmentList().stream()
                        .filter(segment -> segment.gssSet().size() > 0
                                && !segment.gssSet().stream()
                                .map(GlobalSynset::language)
                                .allMatch(SSet.of(Language.Mixed, Language.Technical)::contains));
                final var chosenSegment = StreamExtensions.pickRandomOrThrow(
                        validSegmentStream, rte, random
                );
                final var newSegmentOpt = translate(chosenSegment, random);
                if (newSegmentOpt.isEmpty()) {
//                    throw new RuntimeException("No translation found!");
//                    System.out.println("I'll fucking do it again!");
                    yield translateInner(name, random, acc + 1, max); // https://i.kym-cdn.com/entries/icons/original/000/030/952/goofy.jpg
                }
                final var newSegmentList = StreamExtensions.replaceInStream(
                        sps.segmentList().stream(), chosenSegment, newSegmentOpt.get()
                ).toList();
                yield Optional.of(sps.withSegmentList(newSegmentList));
            }
        };
    }

    @NotNull
    private StringPlus translateNaked(StringPlusNaked name, Random random) {
        return switch (name.language()) {
            case English:
                final var germanRawString = translate(name.rawString(), Language.German, random);
                // TODO: get translation
                yield new StringPlusNaked(germanRawString, Language.German);

            case German:
                final var englishRawString = translate(name.rawString(), Language.English, random);
                // TODO: get translation
                yield new StringPlusNaked(englishRawString, Language.English);

            case Mixed:
                final var newLanguage = (random.nextInt() % 2 == 0) ? Language.German : Language.English;
                final var newRawString = name.rawString();
                // TODO: get translation
                yield new StringPlusNaked(newRawString, newLanguage);

            default:
                throw new IllegalStateException("Unexpected value: " + name.language());
        };
    }

    @NotNull
    public String translate(String string, Language targetLanguage, Random random) {
        // TODO: Use a lib
        return string;
    }

    public boolean canBeTranslated(StringPlus stringPlus) {
        return switch (stringPlus) {
            case StringPlusNaked ignore -> false;
//            case StringPlusNaked spn -> !SSet.of(Language.Mixed, Language.Technical).contains(spn.language());
            case StringPlusSemantical sps -> sps.language() != Language.Technical;
        };
    }
}