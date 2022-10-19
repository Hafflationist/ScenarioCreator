package de.mrobohm.heterogeneity.constraintBased.regexy;

import de.mrobohm.utils.Pair;
import de.mrobohm.utils.StreamExtensions;

import java.util.Optional;
import java.util.Random;

public final class DfaToRandomString {

    private DfaToRandomString() {
    }

    public static String generate(DFA dfa, Random random) {
        final var allStateSet = dfa.stateSet();
        if (allStateSet.isEmpty()) {
            return "";
        }
        final var initState = dfa.initState();
        return generate(dfa, initState, "", "", random);
    }

    private static String generate(
            DFA dfa, StateDet state, String acc, String lastAcceptedString, Random random
    ) {
        if (state.isAcceptState() && random.nextInt(12) == 7) {
            // accepting current string
            return acc;
        }
        final var nextTransitionOpt = chooseTransition(dfa, state, random);
        if (nextTransitionOpt.isEmpty() || acc.length() > 128) {
            // CANCEL -> return last accepted string
            return lastAcceptedString;
        }
        final var newAcc = acc + nextTransitionOpt.get().first();
        final var newState = nextTransitionOpt.get().second();
        final var newLastAcceptedString = newState.isAcceptState() ? newAcc : lastAcceptedString;
        return generate(dfa, newState, newAcc, newLastAcceptedString, random);
    }

    private static Optional<Pair<Character, StateDet>> chooseTransition(
            DFA dfa, StateDet state, Random random
    ) {
        final var transitionStream = state.transitionMap().entrySet().stream()
                .map(entry -> dfa.stateSet().stream()
                        .filter(s -> s.id() == entry.getValue()).findFirst()
                        .map(nextState -> new Pair<>(entry.getKey(), nextState)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(pair -> !isDeadEnd(dfa, pair.second()));

        return StreamExtensions.tryPickRandom(transitionStream, random);
    }

    private static boolean isDeadEnd(DFA dfa, StateDet state) {
        return dfa.getClosure(state)
                .noneMatch(StateDet::isAcceptState);
    }
}