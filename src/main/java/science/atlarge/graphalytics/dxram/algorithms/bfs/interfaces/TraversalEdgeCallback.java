package science.atlarge.graphalytics.dxram.algorithms.bfs.interfaces;

import de.hhu.bsinfo.dxgraph.data.Edge;

public interface TraversalEdgeCallback {
    // return false to terminate the traversal (because result found, error, ...), true to continue
    boolean evaluateEdge(Edge p_edge, int p_depth);
}
