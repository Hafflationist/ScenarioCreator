package scenarioCreator.generation.heterogeneity.constraintBased.regexy.minimizer;

import scenarioCreator.generation.heterogeneity.constraintBased.regexy.DFA;
import scenarioCreator.generation.heterogeneity.constraintBased.regexy.StateDet;

import java.util.*;
import java.util.stream.Collectors;

public class DfaMinimization {

    public static DFA minimize(DFA preDfa) {
        final var dfa = removeUnreachableStates(preDfa);

        final var stateList = dfa.stateSet();
        var groups = (List<Group>) new ArrayList<Group>();

        // stores the group of non-final states
        final var nonFinalStates = new Group(1);
        final var finalStates = new Group(stateList.stream().filter(StateDet::isAcceptState).toList(), 0);

        for (var s : stateList) {
            if (!finalStates.contains(s))
                nonFinalStates.add(s);
        }

        //add final and the non-final group in the list of groups
        groups.add(finalStates);
        groups.add(nonFinalStates);

        //initial set of groups
        System.out.println("initially - \n" + groups);

        //set of groups after removing unreachable states
        System.out.println("removed unreachable - \n" + groups);
        System.out.println();

        //minimize all the groups
        groups = minimize(groups);

        //set of minimized groups
        System.out.println("minimized - \n" + groups);


        //map all different groups to a specific integer that is their id
//        Map<Integer, Integer> map = new HashMap<>();
//        int idx = 0;
//        for (Group g : groups) {
//            for (StateDet s : g) {
//                map.put(s.id(), idx);
//            }
//            idx++;
//        }


        //store all the set of transitions after minimization
//        Set<String> trans = new HashSet<>();
//        for (Group g : groups) {
//            for (StateDet s : g) {
//                for (Map.Entry<Character, Integer> e : s.transitionMap().entrySet()) {
//                    trans.add(map.get(s.id()) + " " + e.getKey() + " " + map.get(e.getValue()));
//                }
//            }
//        }

        final var newStateList = groups.stream()
                .flatMap(Collection::stream)
                .toList();

        return new DFA(newStateList.get(0), new TreeSet<>(newStateList));
    }

    public static DFA removeUnreachableStates(DFA dfa) {
        final var closureSet = dfa.getClosure(dfa.initState())
                .collect(Collectors.toCollection(TreeSet::new));
        return new DFA(dfa.initState(), closureSet);
    }

    /*
     * Recursive function  to minimize the number of states until they can be divided
     *
     * returns a list of groups of states that are similar and can be showed as a single state
     */
    private static List<Group> minimize(List<Group> groups) {
        System.out.println(groups);

        //list of groups after division of groups
        List<Group> result = new ArrayList<>();

        int idx = 0;

        for (Group g : groups) {
            if (g.size() > 1) {
                for (int i = 0; i < g.size(); i++) {

                    // if the result group already contains the current group then we don't need to check it
                    if (containsState(result, g.get(i))) {
                        continue;
                    }

                    // a new group to store states that are not unique among the current group g
                    Group newGroup = new Group(idx++);
                    newGroup.add(g.get(i));

                    for (int j = i + 1; j < g.size(); j++) {

                        StateDet state1 = g.get(i);
                        StateDet state2 = g.get(j);

                        if (!areStatesUnique(groups, state1, state2)) {
                            newGroup.add(state2);
                        }
                    }

                    result.add(newGroup);
                }
            } else {
                result.add(new Group(g, idx++));
            }
        }

        if (groups.size() == result.size()) return result;

        // minimize further
        return minimize(result);
    }

    private static boolean areStatesUnique(List<Group> groups, StateDet state1, StateDet state2) {
        for (Map.Entry<Character, Integer> e : state1.transitionMap().entrySet()) {
            if (!containedBySameGroup(groups, e.getValue(), state2.transitionMap().get(e.getKey()))) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsState(List<Group> groups, StateDet state) {
        return groups.stream().anyMatch(g -> g.contains(state));
    }

    private static boolean containedBySameGroup(List<Group> groups, int stateId1, int stateId2) {
        return groups.stream().anyMatch(group ->
        {
            var contains1 = group.stream().anyMatch(s -> s.id() == stateId1);
            var contains2 = group.stream().anyMatch(s -> s.id() == stateId2);
            return contains1 && contains2;
        });
    }
}