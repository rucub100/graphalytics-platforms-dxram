/* 
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, 
 * Institute of Computer Science, Department Operating Systems
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package science.atlarge.graphalytics.dxram.graph;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import de.hhu.bsinfo.dxmem.data.ChunkID;

/**
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, Jan 1, 2019
 *
 */
public class Graph {

	public static Graph CONSTRUCTED_GRAPH = null;

	private final Map<Long, Long> vertexIdToCID = new HashMap<Long, Long>();
	private boolean built = false;
	private long[] vertices = null;

	public long getVertexCID(int index) {
		return this.vertices[index];
	}

	public long getVertexCID(long vertexId) {
		return vertexIdToCID.getOrDefault(vertexId, ChunkID.INVALID_ID);
	}

	public long[] getVertexCIDs() {
		return this.vertices;
	}

	public void putVertexCID(long vertexId, long cid) {
		if (!built) {
			vertexIdToCID.put(vertexId, cid);
		}
	}

	public void build() {
		if (!built) {
			built = true;
			final long[] tmp_vertices = new long[this.vertexIdToCID.size()];
			final AtomicInteger index = new AtomicInteger(0);
			this.vertexIdToCID
				.values()
				.parallelStream()
				.forEach(new Consumer<Long>() {
					@Override
					public void accept(Long cid) {
						tmp_vertices[index.getAndIncrement()] = cid;
					}
				});
			this.vertices = tmp_vertices;
		}
	}

	public int getNumberOfVertices() {
		return this.vertices.length;
	}
}
