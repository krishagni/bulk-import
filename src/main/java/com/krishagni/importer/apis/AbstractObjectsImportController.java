package com.krishagni.importer.apis;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.multipart.MultipartFile;

import com.krishagni.importer.domain.ObjectSchema;
import com.krishagni.importer.events.FileRecordsDetail;
import com.krishagni.importer.events.ImportDetail;
import com.krishagni.importer.events.ImportJobDetail;
import com.krishagni.importer.events.ObjectSchemaCriteria;
import com.krishagni.importer.repository.ListImportJobsCriteria;
import com.krishagni.importer.services.ImportService;
import com.krishagni.importer.services.ObjectReader;

public abstract class AbstractObjectsImportController {

	@RequestMapping(method = RequestMethod.GET, value="/input-file-template")
	@ResponseStatus(HttpStatus.OK)
	@ResponseBody
	public void getInputFileTemplate(
		@RequestParam(value = "schema", required = true)
		String schemaName,

		@RequestParam
		Map<String, String> params,

		HttpServletResponse httpResp) {

		params.remove("schema");

		ObjectSchemaCriteria detail = new ObjectSchemaCriteria();
		detail.setObjectType(schemaName);
		detail.setParams(params);

		String template = getImportService().getInputFileTemplate(detail);
		String filename = getTemplateFilename(schemaName, params);

		httpResp.setContentType("application/csv");
		httpResp.setHeader("Content-Disposition", "attachment;filename=" + filename);

		try {
			httpResp.getOutputStream().write(template.getBytes());
		} catch (IOException e) {
			throw new RuntimeException("Error sending file", e);
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/input-file")
	@ResponseStatus(HttpStatus.OK)
	@ResponseBody
	public Map<String, String> uploadJobInputFile(@PathVariable("file") MultipartFile file)
	throws IOException {
		return Collections.singletonMap("fileId", getImportService().uploadImportJobFile(file.getInputStream()));
	}

	@RequestMapping(method = RequestMethod.POST, value = "/process-file-records")
	@ResponseStatus(HttpStatus.OK)
	@ResponseBody
	public List<Map<String, Object>> processFileRecords(@RequestBody FileRecordsDetail detail) {
		return getImportService().processFileRecords(detail);
	}

	@RequestMapping(method = RequestMethod.POST, value = "/record-fields-csv")
	@ResponseStatus(HttpStatus.OK)
	@ResponseBody
	public String getRecordFieldsCsv(@RequestBody List<ObjectSchema.Field> fields) {
		ObjectSchema.Record schemaRec = new ObjectSchema.Record();
		schemaRec.setFields(fields);

		ObjectSchema schema = new ObjectSchema();
		schema.setRecord(schemaRec);
		return ObjectReader.getSchemaFields(schema);
	}

	@RequestMapping(method = RequestMethod.GET)
	@ResponseStatus(HttpStatus.OK)
	@ResponseBody
	public List<ImportJobDetail> getImportJobs(
		@RequestParam(value = "startAt", required = false, defaultValue = "0")
		int startAt,

		@RequestParam(value = "maxResults", required = false, defaultValue = "25")
		int maxResults,

		@RequestParam(value = "objectType", required = false)
		String[] objectTypes,

		@RequestParam
		Map<String, String> params) {

		String[] nonParams = {"startAt", "maxResults", "objectType"};
		for (String nonParam : nonParams) {
			params.remove(nonParam);
		}

		ListImportJobsCriteria crit = new ListImportJobsCriteria()
			.startAt(startAt)
			.maxResults(maxResults)
			.objectTypes(objectTypes != null ? Arrays.asList(objectTypes) : null)
			.params(params);
		return getImportService().getImportJobs(crit);
	}

	@RequestMapping(method = RequestMethod.GET, value = "/{id}")
	@ResponseStatus(HttpStatus.OK)
	@ResponseBody
	public ImportJobDetail getImportJob(@PathVariable("id") Long jobId) {
		return getImportService().getImportJob(jobId);
	}

	@RequestMapping(method = RequestMethod.GET, value = "/{id}/output")
	@ResponseStatus(HttpStatus.OK)
	@ResponseBody
	public void getImportJobOutputFile(@PathVariable("id") Long jobId, HttpServletResponse httpResp) {
		String filePath = getImportService().getImportJobFile(jobId);

		httpResp.setContentType("application/csv");
		httpResp.setHeader("Content-Disposition", "attachment;filename=BulkImportJob_" + jobId + ".csv");

		InputStream in = null;
		try {
			in = new FileInputStream(new File(filePath));
			IOUtils.copy(in, httpResp.getOutputStream());
		} catch (IOException e) {
			throw new RuntimeException("Error sending file", e);
		} finally {
			IOUtils.closeQuietly(in);
		}
	}

	@RequestMapping(method = RequestMethod.POST)
	@ResponseStatus(HttpStatus.OK)
	@ResponseBody
	public ImportJobDetail createImportJob(@RequestBody ImportDetail detail) {
		return getImportService().importObjects(detail);
	}

	@RequestMapping(method = RequestMethod.PUT, value = "{id}/stop")
	@ResponseStatus(HttpStatus.OK)
	@ResponseBody
	public ImportJobDetail stopImportJob(@PathVariable("id") Long jobId) {
		return getImportService().stopJob(jobId);
	}

	public abstract String getTemplateFilename(String schema, Map<String, String> params);

	public abstract ImportService getImportService();
}
