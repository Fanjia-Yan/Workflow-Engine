import java.io.Serializable;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Stack;
import java.util.HashSet;

public class Graph implements Serializable, Iterable<Integer>  {

    private LinkedList<Edge>[] adjLists;
    private int vertexCount;

    /** Initializes a graph with NUMVERTICES vertices and no Edges. */
    public Graph(int numVertices) {
        adjLists = (LinkedList<Edge>[]) new LinkedList[numVertices];
        for (int k = 0; k < numVertices; k++) {
            adjLists[k] = new LinkedList<Edge>();
        }
        vertexCount = numVertices;
    }

    /** Adds a directed Edge (V1, V2) to the graph. That is, adds an edge
       in ONE directions, from v1 to v2. */
    public void addEdge(int v1, int v2) {
        addEdge(v1, v2, 0);
    }


    /** Adds a directed Edge (V1, V2) to the graph with weight WEIGHT. If the
       Edge already exists, replaces the current Edge with a new Edge with
       weight WEIGHT. */
    private void addEdge(int v1, int v2, int weight) {
        Edge e = new Edge(v1, v2, weight);
        if (adjLists[v1].contains(e)){
            adjLists[v1].remove(e);
        }
        adjLists[v1].add(e);
    }

    /** Returns the number of incoming Edges for vertex V. */
    public int inDegree(int v) {
        int count = 0;
            for (LinkedList<Edge> list : adjLists){
                for(Edge e : list){
                    if(e.to == v){
                        count = count + 1;
                    }
                }
            }
        return count;
    }
    /** Returns the number of outgoing Edges for vertex V. */
    public int outDegree(int v){
        return adjLists[v].size();
    }

    /**Returns the vertices that depend on vertex V */
    public ArrayList<Integer> dependencies(int v){
        ArrayList<Integer> ret = new ArrayList<>();
        for (LinkedList<Edge> list : adjLists){
            for(Edge e : list){
                if(e.to == v){
                    ret.add(e.from);
                }
            }
        }
        return ret;
    }
    /** Returns an Iterator that outputs the vertices of the graph in topological
       sorted order. */
    public Iterator<Integer> iterator() {
        return new TopologicalIterator();
    }

    public List<Integer> topologicalSort() {
        ArrayList<Integer> result = new ArrayList<Integer>();
        Iterator<Integer> iter = new TopologicalIterator();
        while (iter.hasNext()) {
            result.add(iter.next());
        }
        return result;
    }

    private class TopologicalIterator implements Iterator<Integer> {

        private Stack<Integer> fringe;
        private Integer[] currentInDegree;
        private Integer currentVertex;
        private HashSet<Integer> visited;

        TopologicalIterator() {
            fringe = new Stack<Integer>();
            visited = new HashSet<Integer>();
            currentInDegree = new Integer[adjLists.length];
            for (int i = 0; i < adjLists.length; i++) {
                currentInDegree[i] = inDegree(i);
                if (inDegree(i) == 0) {
                    fringe.push(i);
                }
            }
        }

        public boolean hasNext() {
            return !fringe.isEmpty();
        }

        public Integer next() {
            currentVertex = fringe.pop();
            for (Edge e: adjLists[currentVertex]) {
                currentInDegree[e.to]--;
            }
            visited.add(currentVertex);
            for (int i = 0; i < adjLists.length; i++) {
                if (!visited.contains(i) && !fringe.contains(i) && currentInDegree[i] == 0) {
                    fringe.push(i);
                }
            }
            return currentVertex;
        }
    }

    private class Edge implements Serializable{

        private int from;
        private int to;
        private int weight;

        Edge(int from, int to, int weight) {
            this.from = from;
            this.to = to;
            this.weight = weight;
        }
        public String toString() {
            return "(" + from + ", " + to + ", weight = " + weight + ")";
        }
    }
}