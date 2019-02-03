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
package science.atlarge.graphalytics.dxram.job;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.commons.lang.ArrayUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import science.atlarge.graphalytics.dxram.DxramConfiguration;
import science.atlarge.graphalytics.dxram.graph.BFSVertex;
import science.atlarge.graphalytics.dxram.graph.Graph;

/**
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, December 27, 2018
 *
 */
public class LoadGraphJob extends GraphalyticsAbstractJob {
	private static final Logger LOGGER = LogManager.getFormatterLogger(LoadGraphJob.class.getSimpleName());

	public static final short TYPE_ID = 1;

	// Needed to be serializable/deserializable?
	public LoadGraphJob() { }

	public LoadGraphJob(
			String jobId,
			String logPath,
			String vertexPath,
			String edgePath,
			String outputPath,
			DxramConfiguration platformConfig) {
		super(jobId, logPath, vertexPath, edgePath, outputPath, platformConfig);
	}
	
	@Override
	public short getTypeID() {
		return TYPE_ID;
	}

	@Override
	public void execute() {
		final ChunkLocalService chunkLocalService = getService(ChunkLocalService.class);
		final ChunkService chunkService = getService(ChunkService.class);

		LOGGER.info("Construct a graph");
		// prepare graph metadata for construction
		final Graph graph = new Graph();

		LOGGER.info(String.format("Read the vertex file: %s", vertexPath));
		// read vertex file
		final List<Long> vertexList = new ArrayList<Long>();
		try (Stream<String> stream = Files.lines(Paths.get(vertexPath), StandardCharsets.US_ASCII)) {
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
			LOGGER.error("IOException: " + e.getMessage());
		}

		LOGGER.info(String.format("Read edge file [%s] and build the graph", edgePath));
		// read edge file & build graph
		try (Stream<String> stream = Files.lines(Paths.get(edgePath), StandardCharsets.US_ASCII)) {
			final List<Long> vertexAndNeighbors = new ArrayList<Long>();
			stream.sequential().forEach(new Consumer<String>() {
				@Override
				public void accept(String line) {
					String[] tmp = line.split("\\s");
					long srcId = Long.parseLong(tmp[0]);
					long dstId = Long.parseLong(tmp[1]);

					if (vertexAndNeighbors.isEmpty()) {
						vertexAndNeighbors.add(srcId);
						vertexAndNeighbors.add(dstId);
					} else if (vertexAndNeighbors.contains(srcId)) {
						vertexAndNeighbors.add(dstId);
					} else {
						final long vertexId = vertexAndNeighbors.remove(0);
						vertexList.remove(vertexId);
						BFSVertex bfsVertex = new BFSVertex(vertexId);
						bfsVertex.setNeighbors(
								ArrayUtils.toPrimitive(
										vertexAndNeighbors.toArray(new Long[vertexAndNeighbors.size()])));
						chunkLocalService.createLocal().create(bfsVertex);
						chunkService.put().put(bfsVertex);
						graph.putVertexCID(vertexId, bfsVertex.getID());
						vertexAndNeighbors.clear();
						vertexAndNeighbors.add(srcId);
						vertexAndNeighbors.add(dstId);
					}
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.error("IOException: " + e.getMessage());
		}

		LOGGER.info("Handle vertices without neighbors");
		// handle vertices without neighbors
		for (long vertexId : vertexList) {
			BFSVertex bfsVertex = new BFSVertex(vertexId);
			chunkLocalService.createLocal().create(bfsVertex);
			chunkService.put().put(bfsVertex);
			graph.putVertexCID(vertexId, bfsVertex.getID());
		}
		vertexList.clear();

		LOGGER.error("Build the graph");
		graph.build();
		Graph.CONSTRUCTED_GRAPH = graph;
		LOGGER.info(String.format("Graph: %d vertices", graph.getNumberOfVertices()));
	}
}
