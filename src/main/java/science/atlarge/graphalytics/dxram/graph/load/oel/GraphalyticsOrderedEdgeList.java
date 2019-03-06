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
package science.atlarge.graphalytics.dxram.graph.load.oel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import science.atlarge.graphalytics.dxram.graph.data.Vertex;

/**
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, 14.02.2019
 *
 */
public class GraphalyticsOrderedEdgeList implements OrderedEdgeList {

	public static long[] CID_TO_VERTEX_ID = null;
	public static Map<Long, Long> VERTEX_ID_TO_CID = new HashMap<Long, Long>();
	

	private final String m_vertexPath;
	private final String m_edgePath;
	// TODO: private final int m_bufferSize;
	private final long m_startOffset;
	private final long m_endOffset;
	private final long m_startVertexId;

	private final LinkedList<Vertex> m_oel = new LinkedList<Vertex>();
	private boolean m_loaded;

	public GraphalyticsOrderedEdgeList(
			final String p_vertexPath,
			final String p_edgePath,
			final long p_startOffset,
			final long p_endOffset,
			final long p_startVertexId) {
		m_vertexPath = p_vertexPath;
		m_edgePath = p_edgePath;
		m_startOffset = p_startOffset;
		m_endOffset = p_endOffset;
		m_startVertexId = p_startVertexId;

		m_loaded = false;
	}

	private void loadFromVertexEdgeFile() {
		// read vertex file
		final LinkedList<Long> vertexList = new LinkedList<Long>();
		try (Stream<String> stream = Files.lines(Paths.get(m_vertexPath), StandardCharsets.US_ASCII)) {
			stream.forEach(new Consumer<String>() {
				@Override
				public void accept(String line) {
					String[] tmp = line.split("\\s");
					long vertexId = Long.parseLong(tmp[0]);
					vertexList.add(vertexId);
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}

		final int totalVertexCnt = vertexList.size();
		CID_TO_VERTEX_ID = new long[totalVertexCnt];
		// read edge file
		try (Stream<String> stream = Files.lines(Paths.get(m_edgePath), StandardCharsets.US_ASCII)) {
			final AtomicLong currentVertexId = new AtomicLong(vertexList.pop());
			final AtomicInteger cnt = new AtomicInteger(0);
			final LinkedList<Long> neighbors = new LinkedList<Long>();
			stream.forEach(new Consumer<String>() {
				@Override
				public void accept(String line) {
					String[] tmp = line.split("\\s");
					long srcId = Long.parseLong(tmp[0]);
					long dstId = Long.parseLong(tmp[1]);

					// add to oel
					while (srcId > currentVertexId.get()) {
						VERTEX_ID_TO_CID.put(currentVertexId.get(), m_startVertexId + cnt.get());
						CID_TO_VERTEX_ID[cnt.get()] = currentVertexId.getAndSet(vertexList.pop());
						Vertex v = new Vertex();
						v.setID(m_startVertexId + cnt.getAndIncrement());
						v.setNeighbors(neighbors.stream().mapToLong(Long::longValue).toArray());
						neighbors.clear();
						m_oel.add(v);
					}

					neighbors.add(dstId);
				}
			});

			// add remaining vertices (last from edge list + vertices without neighbors at the end)
			{
				VERTEX_ID_TO_CID.put(currentVertexId.get(), m_startVertexId + cnt.get());
				CID_TO_VERTEX_ID[cnt.get()] = currentVertexId.get();
				Vertex v = new Vertex();
				v.setID(m_startVertexId + cnt.getAndIncrement());
				v.setNeighbors(neighbors.stream().mapToLong(Long::longValue).toArray());
				neighbors.clear();
				m_oel.add(v);				
			}
			while (!vertexList.isEmpty()) {
				currentVertexId.set(vertexList.pop());
				VERTEX_ID_TO_CID.put(currentVertexId.get(), m_startVertexId + cnt.get());
				CID_TO_VERTEX_ID[cnt.get()] = currentVertexId.get();
				Vertex v = new Vertex();
				v.setID(m_startVertexId + cnt.getAndIncrement());
				v.setNeighbors(neighbors.stream().mapToLong(Long::longValue).toArray());
				neighbors.clear();
				m_oel.add(v);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		// convert neighbors
		for (Vertex v : m_oel) {
			for (int i = 0; i < v.getNeighbors().length; i++) {
				v.getNeighbors()[i] = VERTEX_ID_TO_CID.get(v.getNeighbors()[i]);
			}
		}
	}

	@Override
	public Vertex readVertex() {
		if (!m_loaded) {
			m_loaded = true;
			loadFromVertexEdgeFile();
		}

		return m_oel.poll();
	}
}
