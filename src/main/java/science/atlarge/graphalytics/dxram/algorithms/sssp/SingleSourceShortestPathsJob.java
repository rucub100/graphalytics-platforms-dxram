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
package science.atlarge.graphalytics.dxram.algorithms.sssp;

import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.domain.algorithms.AlgorithmParameters;
import science.atlarge.graphalytics.domain.algorithms.SingleSourceShortestPathsParameters;
import science.atlarge.graphalytics.dxram.DxramJob;
import science.atlarge.graphalytics.dxram.DxramConfiguration;

/**
 * DXRAM implementation of the Single Source Shortest Paths algorithm.
 *
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, December 27, 2018
 */
public final class SingleSourceShortestPathsJob extends DxramJob {

	public static final short TYPE_ID = 14;

	private final long sourceVertex;

	/**
	 * Creates a new BreadthFirstSearchJob object with all mandatory parameters specified.
	 *
	 * @param platformConfig the platform configuration.
	 * @param inputPath the path to the input graph.
	 */
	public SingleSourceShortestPathsJob(RunSpecification runSpecification, DxramConfiguration platformConfig) {
		super(runSpecification, platformConfig);

		AlgorithmParameters parameters = runSpecification.getBenchmarkRun().getAlgorithmParameters();
		this.sourceVertex = ((SingleSourceShortestPathsParameters)parameters).getSourceVertex();
	}

	@Override
	protected void run() throws Exception {
		throw new UnsupportedOperationException("SSSP not implemented");
	}

	@Override
	public short getTypeID() {
		return TYPE_ID;
	}

	@Override
	protected void execute(short p_nodeID, long[] p_chunkIDs) {
		try {
			execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
