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
package science.atlarge.graphalytics.dxram;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMConfigBuilderException;
import de.hhu.bsinfo.dxram.engine.DXRAMConfigBuilderJVMArgs;
import de.hhu.bsinfo.dxram.engine.DXRAMConfigBuilderJsonFile2;
import de.hhu.bsinfo.dxram.job.JobService;
import science.atlarge.graphalytics.dxram.algorithms.bfs.BreadthFirstSearchJob;
import science.atlarge.graphalytics.dxram.algorithms.cdlp.CommunityDetectionLPJob;
import science.atlarge.graphalytics.dxram.algorithms.lcc.LocalClusteringCoefficientJob;
import science.atlarge.graphalytics.dxram.algorithms.pr.PageRankJob;
import science.atlarge.graphalytics.dxram.algorithms.sssp.SingleSourceShortestPathsJob;
import science.atlarge.graphalytics.dxram.algorithms.wcc.WeaklyConnectedComponentsJob;
import science.atlarge.graphalytics.dxram.job.DropAllChunksJob;
import science.atlarge.graphalytics.dxram.job.DxramJob;
import science.atlarge.graphalytics.dxram.job.GraphalyticsAbstractJob;
import science.atlarge.graphalytics.dxram.job.LoadGraphJob;

/**
 * Main entry point for running a standalone DXRAM instance.
 *
 *
 */
public final class DXRAMMain {
    /**
     * Main entry point
     *
     * @param p_args
     *         Program arguments.
     */
    public static void main(final String[] p_args) {
        printJVMArgs();
        printCmdArgs(p_args);
        System.out.println();

        DXRAM dxram = new DXRAM();

        System.out.println("Starting DXRAM, version " + dxram.getVersion());

        DXRAMConfig config = bootstrapConfig(dxram);

        if (!dxram.initialize(config, true)) {
            System.out.println("Initializing DXRAM failed.");
            System.exit(-1);
        }
        
        registerJobTypes(dxram);

        while (dxram.update()) {
            // run
        }

        System.exit(0);
    }
    
    /**
	 * Register all job types.
	 * 
	 * @param p_dxram DXRAM instance
	 */
	private static void registerJobTypes(final DXRAM p_dxram) {
		final JobService jobService = p_dxram.getService(JobService.class);

		// abstract types
		jobService.registerJobType(GraphalyticsAbstractJob.TYPE_ID, GraphalyticsAbstractJob.class);
		jobService.registerJobType(DxramJob.TYPE_ID, DxramJob.class);

		// load and unload
		jobService.registerJobType(LoadGraphJob.TYPE_ID, LoadGraphJob.class);
		jobService.registerJobType(DropAllChunksJob.TYPE_ID, DropAllChunksJob.class);

		// algorithms
		jobService.registerJobType(BreadthFirstSearchJob.TYPE_ID, BreadthFirstSearchJob.class);
		jobService.registerJobType(CommunityDetectionLPJob.TYPE_ID, CommunityDetectionLPJob.class);
		jobService.registerJobType(LocalClusteringCoefficientJob.TYPE_ID, LocalClusteringCoefficientJob.class);
		jobService.registerJobType(PageRankJob.TYPE_ID, PageRankJob.class);
		jobService.registerJobType(SingleSourceShortestPathsJob.TYPE_ID, SingleSourceShortestPathsJob.class);
		jobService.registerJobType(WeaklyConnectedComponentsJob.TYPE_ID, WeaklyConnectedComponentsJob.class);
	}

    /**
     * Bootstrap configuration loading/creation
     *
     * @param p_dxram
     *          DXRAM instance
     * @return Configuration instance to use to initialize DXRAM
     */
    private static DXRAMConfig bootstrapConfig(final DXRAM p_dxram) {
        DXRAMConfig config = p_dxram.createDefaultConfigInstance();

        DXRAMConfigBuilderJsonFile2 configBuilderFile = new DXRAMConfigBuilderJsonFile2();
        DXRAMConfigBuilderJVMArgs configBuilderJvmArgs = new DXRAMConfigBuilderJVMArgs();

        // JVM args override any default and/or config values loaded from file
        try {
            config = configBuilderJvmArgs.build(configBuilderFile.build(config));
        } catch (final DXRAMConfigBuilderException e) {
            System.out.println("Bootstrapping configuration failed: " + e.getMessage());
            System.exit(-1);
        }

        return config;
    }

    /**
     * Print all cmd args specified on startup
     *
     * @param p_args
     *         Main arguments
     */
    private static void printCmdArgs(final String[] p_args) {
        StringBuilder builder = new StringBuilder();
        builder.append("Cmd arguments: ");

        for (String arg : p_args) {
            builder.append(arg);
            builder.append(' ');
        }

        System.out.println(builder);
        System.out.println();
    }

    /**
     * Print all JVM args specified on startup
     */
    private static void printJVMArgs() {
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        List<String> args = runtimeMxBean.getInputArguments();

        StringBuilder builder = new StringBuilder();
        builder.append("JVM arguments: ");

        for (String arg : args) {
            builder.append(arg);
            builder.append(' ');
        }

        System.out.println(builder);
        System.out.println();
    }
}
