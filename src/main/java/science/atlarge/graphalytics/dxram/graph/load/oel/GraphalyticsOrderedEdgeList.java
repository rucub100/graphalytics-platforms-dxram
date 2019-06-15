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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
	

	private final String m_boelPath;
	private final int m_numberOfVertices;

	private final LinkedList<Vertex> m_oel = new LinkedList<Vertex>();
	private boolean m_loaded;

	public GraphalyticsOrderedEdgeList(
			final String p_boelPath,
			final int p_numberOfVertices) {
	    m_boelPath = p_boelPath;
	    m_numberOfVertices = p_numberOfVertices;
		m_loaded = false;
	}

	private void loadFromBoelFile() {
	    try (DataInputStream dis = new DataInputStream(
	            new BufferedInputStream(
	                    Files.newInputStream(
	                            Paths.get(m_boelPath),
	                            StandardOpenOption.READ),
	                    1000000))) {
	        CID_TO_VERTEX_ID = new long[m_numberOfVertices];
	        for (int i = 0; i < m_numberOfVertices; i++) {
	            CID_TO_VERTEX_ID[i] = dis.readLong();
	            VERTEX_ID_TO_CID.put(CID_TO_VERTEX_ID[i], (long) i);
	            int n = dis.readInt();
	            long[] neighbors = new long[n];
	            for (int j = 0; j < n; j++) {
	                neighbors[j] = dis.readLong();
	            }
	            Vertex vertex = new Vertex();
	            vertex.setID(i);
	            vertex.setNeighbors(neighbors);
	            m_oel.add(vertex);
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
			loadFromBoelFile();
		}

		return m_oel.poll();
	}
}
