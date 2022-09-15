package de.mrobohm.heterogenity.constraintBased.regexy;

import de.mrobohm.utils.Pair;
import de.mrobohm.utils.SSet;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DirectedGraph {
    private final Set<Pair<Integer, Integer>> _edgeSet;

    public DirectedGraph(Set<Pair<Integer, Integer>> edgeSet) {
        final var vertexSet = edgesToVertices(edgeSet);
        if (vertexSet.stream().mapToInt(x -> x).min().orElse(0) < 0) {
            throw new IllegalArgumentException(
                    "Number of vertices in a DirectedGraph must be non-negative"
            );
        }
        _edgeSet = edgeSet;
    }

    private static SortedSet<Integer> edgesToVertices(Set<Pair<Integer, Integer>> edgeSet) {
        return edgeSet.stream()
                .flatMap(pair -> Stream.of(pair.first(), pair.second()))
                .collect(Collectors.toCollection(TreeSet::new));
    }

    public int vertices() {
        return edgesToVertices(_edgeSet).stream().mapToInt(x -> x).max().orElse(0);
    }

    public int edges() {
        return _edgeSet.size();
    }

    public Stream<Integer> adj(int v) {
        return _edgeSet.stream().filter(pair -> pair.first() == v).map(Pair::second);
    }

    public String toString() {
        final var adjacencyMatrixStr = Stream.iterate(0, x -> x + 1).limit(vertices()).map(v -> {
            var vertexStr = String.format("%d: ", v);
            var adjacentVerticesStr = adj(v).map(w -> String.format("%d ", w)).collect(Collectors.joining());
            return vertexStr + adjacentVerticesStr + "\n";
        }).collect(Collectors.joining());
        return vertices() + " vertices, " + edges() + " edges " + "\n" + adjacencyMatrixStr;
    }

    public SortedSet<Integer> depthFirstSearch(int v) {
        return depthFirstSearch(v, SSet.of());
    }

    private SortedSet<Integer> depthFirstSearch(int v, SortedSet<Integer> markedVertexSet) {
        final var newMarkedVertexSet = SSet.prepend(v, markedVertexSet);
        final var addedMarkedVertexSet = adj(v).filter(w -> !newMarkedVertexSet.contains(w)).flatMap(w -> depthFirstSearch(w, newMarkedVertexSet).stream()).collect(Collectors.toCollection(TreeSet::new));
        return SSet.prepend(v, addedMarkedVertexSet);
    }
}