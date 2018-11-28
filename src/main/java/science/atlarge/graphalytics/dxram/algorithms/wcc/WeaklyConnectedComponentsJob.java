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
package science.atlarge.graphalytics.dxram.algorithms.wcc;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.domain.benchmark.BenchmarkRun;
import science.atlarge.graphalytics.dxram.DxramJob;
import science.atlarge.graphalytics.dxram.DxramConfiguration;

/**
 * Weakly Connected Components job implementation for Dxram. This class is responsible for formatting WCC-specific
 * arguments to be passed to the platform executable, and does not include the implementation of the algorithm.
 *
 * @author Ruslan Curbanov
 */
public final class WeaklyConnectedComponentsJob extends DxramJob {

	/**
	 * Creates a new ConnectedComponentsJob object with all mandatory parameters specified.
	 *
	 * @param platformConfig the platform configuration.
	 * @param inputPath the path to the input graph.
	 */
	public WeaklyConnectedComponentsJob(RunSpecification runSpecification, DxramConfiguration platformConfig,
										String inputPath, String outputPath) {
		super(runSpecification, platformConfig, inputPath, outputPath);

	}

	@Override
	protected void appendAlgorithmParameters() {
		commandLine.addArgument("--algorithm");
		commandLine.addArgument("wcc");
	}

}
