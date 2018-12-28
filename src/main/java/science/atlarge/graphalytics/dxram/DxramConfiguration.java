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
package science.atlarge.graphalytics.dxram;

import org.apache.commons.configuration.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import science.atlarge.graphalytics.configuration.ConfigurationUtil;
import science.atlarge.graphalytics.configuration.GraphalyticsExecutionException;

/**
 * Collection of configurable platform options.
 *
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, December 27, 2018
 */
public final class DxramConfiguration implements Importable, Exportable {

	protected static final Logger LOG = LogManager.getLogger();

	private static final String BENCHMARK_PROPERTIES_FILE = "benchmark.properties";
	private static final String HOME_PATH_KEY = "platform.dxram.home";
	private static final String NUM_MACHINES_KEY = "platform.dxram.num-machines";
	private static final String NUM_THREADS_KEY = "platform.dxram.num-threads";

	private String homePath;
	private int numMachines = 1;
	private int numThreads = 1;

	/**
	 * Creates a new DxramConfiguration object to capture all platform parameters that are not specific to any algorithm.
	 */
	public DxramConfiguration(){
	}

	/**
	 * @return the home directory
	 */
	public String getHomePath() {
		return homePath;
	}

	/**
	 * @param homePath the home directory
	 */
	public void setHomePath(String homePath) {
		this.homePath = homePath;
	}

	/**
	 * @return the number of machines
	 */
	public int getNumMachines() {
		return numMachines;
	}

	/**
	 * @param numMachines the number of machines
	 */
	public void setNumMachines(int numMachines) {
		this.numMachines = numMachines;
	}

	/**
	 * @param numThreads the number of threads to use on each machine
	 */
	public void setNumThreads(int numThreads) {
		this.numThreads = numThreads;
	}

	/**
	 * @return the number of threads to use on each machine
	 */
	public int getNumThreads() {
		return numThreads;
	}


	public static DxramConfiguration parsePropertiesFile() {

		DxramConfiguration platformConfig = new DxramConfiguration();

		Configuration configuration = null;
		try {
			configuration = ConfigurationUtil.loadConfiguration(BENCHMARK_PROPERTIES_FILE);
		} catch (Exception e) {
			LOG.warn(String.format("Failed to load configuration from %s", BENCHMARK_PROPERTIES_FILE));
			throw new GraphalyticsExecutionException("Failed to load configuration. Benchmark run aborted.", e);
		}

		String homePath = configuration.getString(HOME_PATH_KEY, null);
		if (homePath != null) {
			platformConfig.setHomePath(homePath);
		}

		Integer numMachines = configuration.getInteger(NUM_MACHINES_KEY, null);
		if (numMachines != null) {
			platformConfig.setNumMachines(numMachines);
		} else {
			platformConfig.setNumMachines(1);
		}

		Integer numThreads = configuration.getInteger(NUM_THREADS_KEY, null);
		if (numThreads != null) {
			platformConfig.setNumThreads(numThreads);
		} else {
			platformConfig.setNumThreads(1);
		}

		return platformConfig;
	}

	@Override
	public int sizeofObject() {
		return ObjectSizeUtil.sizeofString(this.homePath) + (2 * Integer.BYTES);
	}

	@Override
	public void exportObject(Exporter p_exporter) {
		p_exporter.writeString(this.homePath);
		p_exporter.writeInt(this.numMachines);
		p_exporter.writeInt(this.numThreads);
	}

	@Override
	public void importObject(Importer p_importer) {
		this.homePath = p_importer.readString(this.homePath);
		this.numMachines = p_importer.readInt(this.numMachines);
		this.numMachines = p_importer.readInt(this.numThreads);
	}

}
