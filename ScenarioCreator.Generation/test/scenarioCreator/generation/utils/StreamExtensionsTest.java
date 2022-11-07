package scenarioCreator.generation.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.stream.Stream;

class StreamExtensionsTest {

    @Test
    void pickRandomOrThrow() {
        final var stream = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9);
        final var random = StreamExtensions.pickRandomOrThrow(stream, new RuntimeException(), new Random());

        Assertions.assertThrows(IllegalArgumentException.class, () ->
                StreamExtensions.pickRandomOrThrow(Stream.of(), new IllegalArgumentException(), new Random()));
    }
}