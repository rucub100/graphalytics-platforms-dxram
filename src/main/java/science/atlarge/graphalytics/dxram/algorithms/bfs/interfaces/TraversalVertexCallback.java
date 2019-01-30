package science.atlarge.graphalytics.dxram.algorithms.bfs.interfaces;

import de.hhu.bsinfo.dxgraph.data.Vertex;

public interface TraversalVertexCallback {
    // return false to terminate the traversal (because result found, error, ...), true to continue
    boolean evaluateVertex(Vertex p_vertex, int p_depth);
}
