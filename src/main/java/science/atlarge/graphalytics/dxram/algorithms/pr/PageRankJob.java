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
package science.atlarge.graphalytics.dxram.algorithms.pr;

import science.atlarge.graphalytics.domain.algorithms.AlgorithmParameters;
import science.atlarge.graphalytics.domain.algorithms.PageRankParameters;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.dxram.DxramConfiguration;
import science.atlarge.graphalytics.dxram.job.DxramJob;

/**
 * DXRAM implementation of the Page Rank algorithm.
 *
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, December 27, 2018
 */
public final class PageRankJob extends DxramJob {

	public static final short TYPE_ID = 13;

	private final long iteration;
	private final float dampingFactor;

	/**
	 * Creates a new BreadthFirstSearchJob object with all mandatory parameters specified.
	 *
	 * @param platformConfig the platform configuration.
	 * @param inputPath the path to the loaded graph.
	 */
	public PageRankJob(RunSpecification runSpecification, DxramConfiguration platformConfig) {
		super(runSpecification, platformConfig);

		AlgorithmParameters parameters = runSpecification.getBenchmarkRun().getAlgorithmParameters();
		this.iteration = ((PageRankParameters)parameters).getNumberOfIterations();
		this.dampingFactor = ((PageRankParameters)parameters).getDampingFactor();
	}

	@Override
	public short getTypeID() {
		return TYPE_ID;
	}

	@Override
	protected void run() {
		System.err.println("PR not implemented");
	}
}
