/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package science.atlarge.graphalytics.dxram.graph.load;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import science.atlarge.graphalytics.dxram.graph.data.GraphPartitionIndex;
import science.atlarge.graphalytics.dxram.graph.data.Vertex;
import science.atlarge.graphalytics.dxram.graph.load.oel.GraphalyticsOrderedEdgeList;
import science.atlarge.graphalytics.dxram.graph.load.oel.OrderedEdgeList;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

import static de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil.*;

/**
 * TaskScript to load a graph from a partitioned ordered edge list.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class GraphLoadOrderedEdgeListTask implements Task {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoadOrderedEdgeListTask.class.getSimpleName());

    @Expose
    private String m_vertexPath = "";
    @Expose
    private String m_edgePath = "";
    @Expose
    private int m_vertexBatchSize = 100;
    @Expose
    private boolean m_filterDupEdges;
    @Expose
    private boolean m_filterSelfLoops;

    private TaskContext m_ctx;
    private ChunkService m_chunkService;
    private ChunkLocalService m_chunkLocalService;

    /**
     * Default constructor
     */
    public GraphLoadOrderedEdgeListTask() {

    }

    /**
     * Constructor
     *
     * @param p_path
     *         Path containing the graph data to load
     * @param p_vertexBatchSize
     *         Size of a vertex batch for the loading process
     * @param p_filterDupEdges
     *         Check for and filter duplicate edges per vertex
     * @param p_filterSelfLoops
     *         Check for and filter self loops per vertex
     */
    public GraphLoadOrderedEdgeListTask(
    		final String p_vertexPath,
    		final String p_edgePath,
    		final int p_vertexBatchSize,
    		final boolean p_filterDupEdges,
    		final boolean p_filterSelfLoops) {
        m_vertexPath = p_vertexPath;
        m_edgePath = p_edgePath;
        m_vertexBatchSize = p_vertexBatchSize;
        m_filterDupEdges = p_filterDupEdges;
        m_filterSelfLoops = p_filterSelfLoops;
    }

    /**
     * Set the number of vertices to buffer with one load call.
     *
     * @param p_batchSize
     *         Number of vertices to buffer.
     */
    public void setLoadVertexBatchSize(final int p_batchSize) {
        m_vertexBatchSize = p_batchSize;
    }

    /**
     * Set the file path that contains the vertex graph data.
     *
     * @param p_path
     *         Path to the vertex graph data file.
     */
    public void setLoadVertexPath(final String p_path) {
        m_vertexPath = p_path;
    }

    /**
     * Set the file path that contains the edge graph data.
     *
     * @param p_path
     *         Path to the edge graph data file.
     */
    public void setLoadEdgePath(final String p_path) {
        m_edgePath = p_path;
    }

    @Override
    public int execute(final TaskContext p_ctx) {
        m_ctx = p_ctx;
        m_chunkService = m_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        m_chunkLocalService = m_ctx.getDXRAMServiceAccessor().getService(ChunkLocalService.class);
        NameserviceService nameserviceService = m_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);

        // look for the graph partitioned index of the current compute group
        long chunkIdPartitionIndex =
                nameserviceService.getChunkID(GraphLoadPartitionIndexTask.MS_PART_INDEX_IDENT + m_ctx.getCtxData().getComputeGroupId(), 5000);
        if (chunkIdPartitionIndex == ChunkID.INVALID_ID) {
            // #if LOGGER >= ERROR
            LOGGER.error("Could not find partition index for current compute group %d", m_ctx.getCtxData().getComputeGroupId());
            // #endif /* LOGGER >= ERROR */
            return -1;
        }

        GraphPartitionIndex graphPartitionIndex = new GraphPartitionIndex();
        graphPartitionIndex.setID(chunkIdPartitionIndex);

        // get the index
        if (!m_chunkService.get().get(graphPartitionIndex)) {
            // #if LOGGER >= ERROR
            LOGGER.error("Getting partition index from temporary memory failed");
            // #endif /* LOGGER >= ERROR */
            return -2;
        }

        OrderedEdgeList graphPartitionOel = setupOrderedEdgeListForCurrentSlave(
        		m_vertexPath,
        		m_edgePath,
        		graphPartitionIndex);

        if (graphPartitionOel == null) {
            // #if LOGGER >= ERROR
            LOGGER.error("Setting up graph partition for current slave failed");
            // #endif /* LOGGER >= ERROR */
            return -3;
        }

        // #if LOGGER >= INFO
        LOGGER.info("Chunkservice status BEFORE load:\n%s", m_chunkService.status().getStatus());
        // #endif /* LOGGER >= INFO */

        if (!loadGraphPartition(graphPartitionOel, graphPartitionIndex)) {
            // #if LOGGER >= ERROR
            LOGGER.error("Loading graph partition failed");
            // #endif /* LOGGER >= ERROR */
            return -4;
        }

        // #if LOGGER >= INFO
        LOGGER.info("Chunkservice status AFTER load:\n%s", m_chunkService.status().getStatus());
        // #endif /* LOGGER >= INFO */

        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {
        switch (p_signal) {
            case SIGNAL_ABORT: {
                // ignore signal here
                break;
            }
            default:
                break;
        }
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
    	p_exporter.writeString(m_vertexPath);
    	p_exporter.writeString(m_edgePath);
        p_exporter.writeInt(m_vertexBatchSize);
        p_exporter.writeBoolean(m_filterDupEdges);
        p_exporter.writeBoolean(m_filterSelfLoops);
    }

    @Override
    public void importObject(final Importer p_importer) {
    	m_vertexPath = p_importer.readString(m_vertexPath);
    	m_edgePath = p_importer.readString(m_edgePath);
        m_vertexBatchSize = p_importer.readInt(m_vertexBatchSize);
        m_filterDupEdges = p_importer.readBoolean(m_filterDupEdges);
        m_filterSelfLoops = p_importer.readBoolean(m_filterSelfLoops);
    }

    @Override
    public int sizeofObject() {
        return sizeofString(m_vertexPath) + // m_vertexPath
        		sizeofString(m_edgePath) +  // m_edgePath
        		Integer.BYTES +             // m_vertexBatchSize
        		sizeofBoolean() +           // m_filterDupEdges
        		sizeofBoolean();            // m_filterSelfLoops
    }

    /**
     * Setup an edge list instance for the current slave node.
     *
     * @param TODO p_path
     *         Path with indexed graph data partitions.
     * @param p_graphPartitionIndex
     *         Loaded partition index of the graph
     * @return OrderedEdgeList instance giving access to the list found for this slave or null on error.
     */
    private OrderedEdgeList setupOrderedEdgeListForCurrentSlave(
    		final String p_vertexPath,
    		final String p_edgePath,
    		final GraphPartitionIndex p_graphPartitionIndex) {
        // check if file exists
        File vFile = new File(p_vertexPath);
        File eFile = new File(p_edgePath);

        if (!vFile.exists() | !eFile.exists()) {
            // #if LOGGER >= ERROR
            LOGGER.error("Cannot setup edge lists, path does not exist:\nvertexPath: %s \nedgePath: %s", p_vertexPath, p_edgePath);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        long startOffset = p_graphPartitionIndex.getPartitionIndex(m_ctx.getCtxData().getSlaveId()).getFileStartOffset();
        long endOffset;

        // last partition
        if (m_ctx.getCtxData().getSlaveId() + 1 >= p_graphPartitionIndex.getTotalPartitionCount()) {
            endOffset = Long.MAX_VALUE;
        } else {
            endOffset = p_graphPartitionIndex.getPartitionIndex(m_ctx.getCtxData().getSlaveId() + 1).getFileStartOffset();
        }

        // #if LOGGER >= INFO
        LOGGER.info("Partition for slave %dgraph data file: start %d, end %d", m_ctx.getCtxData().getSlaveId(), startOffset, endOffset);
        // #endif /* LOGGER >= INFO */

        // get the first vertex id of the partition to load
        long startVertexId = 0;
        for (int i = 0; i < m_ctx.getCtxData().getSlaveId(); i++) {
            startVertexId += p_graphPartitionIndex.getPartitionIndex(m_ctx.getCtxData().getSlaveId()).getVertexCount();
        }


        return new GraphalyticsOrderedEdgeList(m_vertexPath, m_edgePath, startOffset, endOffset, startVertexId);
    }

    /**
     * Load a graph partition (single threaded).
     *
     * @param p_orderedEdgeList
     *         Graph partition to load.
     * @param p_graphPartitionIndex
     *         Index for all partitions to rebase vertex ids to current node.
     * @return True if loading successful, false on error.
     */

    private boolean loadGraphPartition(final OrderedEdgeList p_orderedEdgeList, final GraphPartitionIndex p_graphPartitionIndex) {
        Vertex[] vertexBuffer = new Vertex[m_vertexBatchSize];
        int readCount;

        GraphPartitionIndex.Entry currentPartitionIndexEntry = p_graphPartitionIndex.getPartitionIndex(m_ctx.getCtxData().getSlaveId());
        if (currentPartitionIndexEntry == null) {
            // #if LOGGER >= ERROR
            LOGGER.error("Cannot load graph, missing partition index entry for partition %d", m_ctx.getCtxData().getSlaveId());
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        float previousProgress = 0.0f;

        long totalVerticesLoaded = 0;
        long totalEdgesLoaded = 0;

        // #if LOGGER >= INFO
        LOGGER.info("Loading started, target vertex/edge count of partition %d: %d/%d", currentPartitionIndexEntry.getPartitionId(),
                currentPartitionIndexEntry.getVertexCount(), currentPartitionIndexEntry.getEdgeCount());
        // #endif /* LOGGER >= INFO */

        GraphalyticsOrderedEdgeList.VERTEX_ID_TO_CID.clear();
        while (true) {
            readCount = 0;
            while (readCount < vertexBuffer.length) {
                Vertex vertex = p_orderedEdgeList.readVertex();
                if (vertex == null) {
                	break;
                }

                long vid = GraphalyticsOrderedEdgeList.CID_TO_VERTEX_ID[(int)vertex.getID()];
                vertex.setID(ChunkID.getChunkID(currentPartitionIndexEntry.getNodeId(), vertex.getID() + 1));
                GraphalyticsOrderedEdgeList.VERTEX_ID_TO_CID.put(vid, vertex.getID());

                // re-basing of neighbors needed for multiple files
                // offset tells us how much to add
                // also add current node ID
                long[] neighbours = vertex.getNeighbors();
                if (!p_graphPartitionIndex.rebaseGlobalVertexIdToLocalPartitionVertexId(neighbours)) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Rebasing of neighbors of %s failed, out of vertex id range of graph: %s", vertex, Arrays.toString(neighbours));
                    // #endif /* LOGGER >= ERROR */
                }

                // for now: check if we exceed the max number of neighbors that fit into a chunk
                // this needs to be changed later to split the neighbor list and have a linked list
                // we don't get this very often, so there aren't any real performance issues
                if (neighbours.length > 134217660) {
                    // #if LOGGER >= WARNING
                    LOGGER.warn("Neighbor count of vertex %s exceeds total number of neighbors that fit into a " + "single vertex; will be truncated", vertex);
                    // #endif /* LOGGER >= WARNING */
                }

                vertexBuffer[readCount] = vertex;
                readCount++;
                totalEdgesLoaded += neighbours.length;
            }

            if (readCount == 0) {
                break;
            }

            // trim array on unused elements
            if (readCount < vertexBuffer.length) {
                vertexBuffer = Arrays.copyOf(vertexBuffer, readCount);
            }

            // TODO: createReservedLocal!
            m_chunkLocalService.createReservedLocal().create((AbstractChunk[]) vertexBuffer);

            int count = m_chunkService.put().put((AbstractChunk[]) vertexBuffer);
            
            if (count != readCount) {
                // #if LOGGER >= ERROR
                LOGGER.error("Putting vertex data for chunks failed: %d != %d", count, readCount);
                // #endif /* LOGGER >= ERROR */
                // return false;
            }

            totalVerticesLoaded += readCount;

            float curProgress = (float) totalVerticesLoaded / currentPartitionIndexEntry.getVertexCount();
            if (curProgress - previousProgress > 0.01) {
                previousProgress = curProgress;
                // #if LOGGER >= INFO
                LOGGER.info("Loading progress: %d", (int) (curProgress * 100));
                // #endif /* LOGGER >= INFO */
            }
        }

        // #if LOGGER >= INFO
        LOGGER.info("Loading done, vertex/edge count: %d/%d", totalVerticesLoaded, totalEdgesLoaded);
        // #endif /* LOGGER >= INFO */

        // filtering removes edges, so this would always fail
        if (!m_filterSelfLoops && !m_filterDupEdges) {
            if (currentPartitionIndexEntry.getVertexCount() != totalVerticesLoaded || currentPartitionIndexEntry.getEdgeCount() != totalEdgesLoaded) {
                // #if LOGGER >= ERROR
                LOGGER.error("Loading failed, vertex/edge count (%d/%d) does not match data in graph partition " + "index (%d/%d)", totalVerticesLoaded,
                        totalEdgesLoaded, currentPartitionIndexEntry.getVertexCount(), currentPartitionIndexEntry.getEdgeCount());
                // #endif /* LOGGER >= ERROR */
                return false;
            }
        } else {
            // #if LOGGER >= INFO
            LOGGER.info("Graph was filtered during loadin: duplicate edges %b, self loops %b", m_filterDupEdges, m_filterSelfLoops);
            // #endif /* LOGGER >= INFO */
        }

        return true;
    }
}
