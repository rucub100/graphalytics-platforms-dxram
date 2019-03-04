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

import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import science.atlarge.graphalytics.dxram.DxramConfiguration;

/**
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, December 27, 2018
 *
 */
public abstract class GraphalyticsAbstractJob extends AbstractJob {

	protected String jobId;
	protected String logPath;
	protected String vertexPath;
	protected String edgePath;
	protected String outputPath;

	protected DxramConfiguration platformConfig;

	public GraphalyticsAbstractJob() {}
	
	public GraphalyticsAbstractJob(
			String jobId,
			String logPath,
			String vertexPath,
			String edgePath,
			String outputPath,
			DxramConfiguration platformConfig) {
		this.jobId = jobId;
		this.logPath = logPath;
		this.vertexPath = vertexPath;
		this.edgePath = edgePath;
		this.outputPath = outputPath;
		this.platformConfig = platformConfig;
	}
	
	@Override
	public int sizeofObject() {
		return ObjectSizeUtil.sizeofString(this.jobId) +
				ObjectSizeUtil.sizeofString(this.logPath) +
				ObjectSizeUtil.sizeofString(this.vertexPath) +
				ObjectSizeUtil.sizeofString(this.edgePath) +
				ObjectSizeUtil.sizeofString(this.outputPath) +
				this.platformConfig.sizeofObject() + 
				super.sizeofObject();
	}

	@Override
	public void exportObject(Exporter p_exporter) {
		super.exportObject(p_exporter);
		p_exporter.writeString(this.jobId);
		p_exporter.writeString(this.logPath);
		p_exporter.writeString(this.vertexPath);
		p_exporter.writeString(this.edgePath);
		p_exporter.writeString(this.outputPath);
		p_exporter.exportObject(this.platformConfig);
	}

	@Override
	public void importObject(Importer p_importer) {
		super.importObject(p_importer);
		this.jobId = p_importer.readString(this.jobId);
		this.logPath = p_importer.readString(this.logPath);
		this.vertexPath = p_importer.readString(this.vertexPath);
		this.edgePath = p_importer.readString(this.edgePath);
		this.outputPath = p_importer.readString(this.outputPath);
		p_importer.importObject(this.platformConfig);
	}

}
