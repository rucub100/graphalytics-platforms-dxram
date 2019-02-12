/*
 * Copyright 2015 Delft University of Technology
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
package science.atlarge.graphalytics.dxram.algorithms.bfs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.ms.TaskScript;
import science.atlarge.graphalytics.domain.algorithms.AlgorithmParameters;
import science.atlarge.graphalytics.domain.algorithms.BreadthFirstSearchParameters;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.dxram.DxramConfiguration;
import science.atlarge.graphalytics.dxram.job.DxramJob;

/**
 * DXRAM implementation of the Breadth-first Search algorithm.
 *
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, December 27, 2018
 */
public final class BreadthFirstSearchJob extends DxramJob {

	public static final short TYPE_ID = 10;

	private static final Logger LOG = LogManager.getLogger();

	private final long sourceVertex;
	private GraphAlgorithmBFSTask bfsTask;

	/**
	 * Creates a new BreadthFirstSearchJob object with all mandatory parameters specified.
	 *
	 * @param platformConfig the platform configuration.
	 * @param inputPath the path to the input graph.
	 */
	public BreadthFirstSearchJob(RunSpecification runSpecification, DxramConfiguration platformConfig) {
		super(runSpecification, platformConfig);

		AlgorithmParameters parameters = runSpecification.getBenchmarkRun().getAlgorithmParameters();
		this.sourceVertex = ((BreadthFirstSearchParameters)parameters).getSourceVertex();
	}

	@Override
	public short getTypeID() {
		return TYPE_ID;
	}

	private void submitBFSTask() {
		MasterSlaveComputeService ms = getService(MasterSlaveComputeService.class);
		bfsTask = new GraphAlgorithmBFSTask();
		TaskScript taskScript = new TaskScript(bfsTask);
		ms.submitTaskScript(taskScript, (short)0);
	}

	@Override
	protected void run() {
		LOG.error("TODO: Implement distributed BFS algorithm...");
		// reserve local ids (CIDs)
		ChunkLocalService cls = getService(ChunkLocalService.class);
		cls.reserveLocal().reserve(500000000);
		// TODO: load graph into runner DXRAM instance
		// define runner as slave to run the task locally (debugging)
		// run the BFSTask
		submitBFSTask();
		// gather the results and create "expected" output
	}
}
