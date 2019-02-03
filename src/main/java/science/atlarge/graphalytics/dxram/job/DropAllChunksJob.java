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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import science.atlarge.graphalytics.dxram.DxramConfiguration;
import science.atlarge.graphalytics.dxram.graph.Graph;

/**
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, December 27, 2018
 *
 */
public class DropAllChunksJob extends GraphalyticsAbstractJob {
	private static final Logger LOGGER = LogManager.getFormatterLogger(DropAllChunksJob.class.getSimpleName());

	public static final short TYPE_ID = 2;

	// Needed to be serializable/deserializable?
	public DropAllChunksJob() { }

	public DropAllChunksJob(
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
		ChunkService chunkService = getService(ChunkService.class);
		LOGGER.info("Cleanup: remove all vertices from constructed graph");
		chunkService.remove().remove(
				Graph.CONSTRUCTED_GRAPH.getVertexCIDs(),
				0,
				Graph.CONSTRUCTED_GRAPH.getNumberOfVertices());
	}
}
