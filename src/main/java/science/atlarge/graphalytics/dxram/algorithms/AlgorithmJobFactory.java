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
package science.atlarge.graphalytics.dxram.algorithms;

import science.atlarge.graphalytics.domain.algorithms.Algorithm;
import science.atlarge.graphalytics.dxram.DxramConfiguration;
import science.atlarge.graphalytics.dxram.algorithms.bfs.BreadthFirstSearchJob;
import science.atlarge.graphalytics.dxram.algorithms.cdlp.CommunityDetectionLPJob;
import science.atlarge.graphalytics.dxram.algorithms.lcc.LocalClusteringCoefficientJob;
import science.atlarge.graphalytics.dxram.algorithms.pr.PageRankJob;
import science.atlarge.graphalytics.dxram.algorithms.sssp.SingleSourceShortestPathsJob;
import science.atlarge.graphalytics.dxram.algorithms.wcc.WeaklyConnectedComponentsJob;
import science.atlarge.graphalytics.dxram.job.DxramJob;
import science.atlarge.graphalytics.execution.PlatformExecutionException;
import science.atlarge.graphalytics.execution.RunSpecification;

/**
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, Jan 1, 2019
 *
 */
public final class AlgorithmJobFactory {

	public static final DxramJob newJob(
			RunSpecification runSpecification, 
			DxramConfiguration platformConfig) throws PlatformExecutionException {
		Algorithm algorithm = runSpecification.getBenchmarkRun().getAlgorithm();

		DxramJob job;
		switch (algorithm) {
		case BFS:
			job = new BreadthFirstSearchJob(runSpecification, platformConfig);
			break;
		case CDLP:
			job = new CommunityDetectionLPJob(runSpecification, platformConfig);
			break;
		case LCC:
			job = new LocalClusteringCoefficientJob(runSpecification, platformConfig);
			break;
		case PR:
			job = new PageRankJob(runSpecification, platformConfig);
			break;
		case WCC:
			job = new WeaklyConnectedComponentsJob(runSpecification, platformConfig);
			break;
		case SSSP:
			job = new SingleSourceShortestPathsJob(runSpecification, platformConfig);
			break;
		default:
			throw new PlatformExecutionException("Failed to load algorithm implementation.");
		}

		return job;
	}
}
