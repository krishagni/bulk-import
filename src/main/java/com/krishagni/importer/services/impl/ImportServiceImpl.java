package com.krishagni.importer.services.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.SessionFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.krishagni.commons.cache.LinkedEhCacheMap;
import com.krishagni.commons.domain.IUser;
import com.krishagni.commons.errors.AppException;
import com.krishagni.commons.errors.ErrorType;
import com.krishagni.commons.io.CsvException;
import com.krishagni.commons.io.CsvFileReader;
import com.krishagni.commons.io.CsvFileWriter;
import com.krishagni.commons.io.CsvWriter;
import com.krishagni.commons.io.ZipUtil;
import com.krishagni.commons.util.Util;
import com.krishagni.importer.domain.ImportJob;
import com.krishagni.importer.domain.ImportJobErrorCode;
import com.krishagni.importer.domain.ObjectSchema;
import com.krishagni.importer.events.FileRecordsDetail;
import com.krishagni.importer.events.ImportDetail;
import com.krishagni.importer.events.ImportJobDetail;
import com.krishagni.importer.events.ImportObjectDetail;
import com.krishagni.importer.events.MergedObject;
import com.krishagni.importer.events.ObjectSchemaCriteria;
import com.krishagni.importer.repository.ImportJobDao;
import com.krishagni.importer.repository.ListImportJobsCriteria;
import com.krishagni.importer.services.ImportConfig;
import com.krishagni.importer.services.ImportListener;
import com.krishagni.importer.services.ImportService;
import com.krishagni.importer.services.ObjectImporter;
import com.krishagni.importer.services.ObjectImporterFactory;
import com.krishagni.importer.services.ObjectReader;
import com.krishagni.importer.services.ObjectSchemaFactory;


public class ImportServiceImpl implements ImportService {
	private static final Log logger = LogFactory.getLog(ImportServiceImpl.class);

	private static final int MAX_RECS_PER_TXN = 10000;

	private static final String CFG_MAX_TXN_SIZE = "import_max_records_per_txn";

	private static Map<Long, ImportJob> runningJobs = new HashMap<>();

	private ImportConfig config;

	private ImportJobDao importJobDao;

	private ThreadPoolTaskExecutor taskExecutor;
	
	private ObjectSchemaFactory schemaFactory;
	
	private ObjectImporterFactory importerFactory;
	
	private PlatformTransactionManager transactionManager;

	private TransactionTemplate txTmpl;

	private TransactionTemplate newTxTmpl;

	private SessionFactory sessionFactory;

	public void setConfig(ImportConfig config) {
		this.config = config;
	}

	public void setImportJobDao(ImportJobDao importJobDao) {
		this.importJobDao = importJobDao;
	}
	
	public void setTaskExecutor(ThreadPoolTaskExecutor taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

	public void setSchemaFactory(ObjectSchemaFactory schemaFactory) {
		this.schemaFactory = schemaFactory;
	}

	public void setImporterFactory(ObjectImporterFactory importerFactory) {
		this.importerFactory = importerFactory;
	}

	public void setTransactionManager(PlatformTransactionManager transactionManager) {
		this.transactionManager = transactionManager;

		this.txTmpl = new TransactionTemplate(this.transactionManager);
		this.txTmpl.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);

		this.newTxTmpl = new TransactionTemplate(this.transactionManager);
		this.newTxTmpl.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
	}

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	@Override
	public List<ImportJobDetail> getImportJobs(ListImportJobsCriteria crit) {
		try {
			List<ImportJob> jobs = importJobDao.getImportJobs(crit);
			return ImportJobDetail.from(jobs);
		} catch (Exception e) {
			return AppException.raiseError(e);
		}
	}

	@Override
	public ImportJobDetail getImportJob(Long jobId) {
		try {
			return ImportJobDetail.from(getJob(jobId));
		} catch (Exception e) {
			return AppException.raiseError(e);
		}
	}
	
	@Override
	public String getImportJobFile(Long jobId) {
		try {
			ImportJob job = getJob(jobId);
			File file = new File(getJobOutputFilePath(job.getId()));
			if (!file.exists()) {
				throw AppException.userError(ImportJobErrorCode.OUTPUT_FILE_NOT_CREATED);
			}

			return file.getAbsolutePath();
		} catch (Exception e) {
			return AppException.raiseError(e);
		}
	}
		
	@Override
	public String uploadImportJobFile(InputStream in) {
		OutputStream out = null;
		
		try {
			//
			// 1. Ensure import directory is present
			//
			String importDir = getImportDir();			
			new File(importDir).mkdirs();
			
			//
			// 2. Generate unique file ID
			//
			String fileId = UUID.randomUUID().toString();
			
			//
			// 3. Copy uploaded file to import directory
			//
			out = new FileOutputStream(importDir + File.separator + fileId);
			IOUtils.copy(in, out);
			
			return fileId;
		} catch (Exception e) {
			return AppException.raiseError(e);
		} finally {
			IOUtils.closeQuietly(out);
		}
	}	
	
	@Override
	public ImportJobDetail importObjects(ImportDetail input) {
		ImportJob job = null;
		try {
			String inputFile = getFilePath(input.getInputFileId());

			if (isZipFile(inputFile)) {
				inputFile = inflateAndGetInputCsvFile(inputFile, input.getInputFileId());
			}
			
			//
			// Ensure transaction size is well within configured limits
			//
			int inputRecordsCnt = CsvFileReader.getRowsCount(inputFile, true, config.getFieldSeparator());
			if (input.isAtomic() && inputRecordsCnt > getMaxRecsPerTxn()) {
				return ImportJobDetail.txnSizeExceeded(inputRecordsCnt);
			}

			job = createImportJob(input);
			importJobDao.saveOrUpdate(job, true);

			//
			// Set up file in job's directory
			//
			createJobDir(job.getId());
			moveToJobDir(inputFile, job.getId());

			runningJobs.put(job.getId(), job);
			taskExecutor.submit(new ImporterTask(config.getAuth(), job, input.getListener(), input.isAtomic()));
			return ImportJobDetail.from(job);
		} catch (Exception e) {
			if (job != null && job.getId() != null) {
				runningJobs.remove(job.getId());
			}

			if (e instanceof AppException) {
				throw e;
			} else if (e instanceof CsvException) {
				throw AppException.userError(ImportJobErrorCode.RECORD_PARSE_ERROR, e.getLocalizedMessage());
			}

			return AppException.raiseError(e);
		}
	}

	@Override
	public ImportJobDetail stopJob(Long jobId) {
		try {
			ImportJob job = runningJobs.get(jobId);
			if (job == null || !job.isInProgress()) {
				throw AppException.userError(ImportJobErrorCode.NOT_IN_PROGRESS, jobId);
			}

			ensureAccess(job).stop();
			for (int i = 0; i < 5; ++i) {
				TimeUnit.SECONDS.sleep(1);
				if (!job.isInProgress()) {
					break;
				}
			}

			return ImportJobDetail.from(job);
		} catch (Exception e) {
			return AppException.raiseError(e);
		}
	}

	@Override
	public String getInputFileTemplate(ObjectSchemaCriteria schemaCriteria) {
		try {
			ObjectSchema schema = schemaFactory.getSchema(schemaCriteria.getObjectType(), schemaCriteria.getParams());
			if (schema == null) {
				throw AppException.userError(ImportJobErrorCode.OBJ_SCHEMA_NOT_FOUND, schemaCriteria.getObjectType());
			}

			return ObjectReader.getSchemaFields(schema);
		} catch (Exception e) {
			return AppException.raiseError(e);
		}
	}

	@Override
	public List<Map<String, Object>> processFileRecords(FileRecordsDetail detail) {
		ObjectReader reader = null;
		File file = null;

		try {
			ObjectSchema.Record schemaRec = new ObjectSchema.Record();
			schemaRec.setFields(detail.getFields());

			ObjectSchema schema = new ObjectSchema();
			schema.setRecord(schemaRec);

			file = new File(getFilePath(detail.getFileId()));
			reader = new ObjectReader(
				file.getAbsolutePath(), schema,
				config.getDateFmt(), config.getTimeFmt(), config.getFieldSeparator());

			List<Map<String, Object>> records = new ArrayList<>();
			Map<String, Object> record = null;
			while ((record = (Map<String, Object>)reader.next()) != null) {
				records.add(record);
			}

			return records;
		} catch (Exception e) {
			return AppException.raiseError(e);
		} finally {
			IOUtils.closeQuietly(reader);
			if (file != null) {
				file.delete();
			}
		}
	}

	private boolean isZipFile(String zipFilename) {
		ZipFile zipFile = null;
		try {
			zipFile = new ZipFile(zipFilename);
			return true;
		} catch (Exception e) {
			return false;
		} finally {
			IOUtils.closeQuietly(zipFile);
		}
	}

	private String inflateAndGetInputCsvFile(String zipFile, String fileId) {
		FileInputStream fin = null;
		String inputFile = null;
		try {
			fin = new FileInputStream(zipFile);

			File zipDirFile = new File(getImportDir() + File.separator + "inflated-" + fileId);
			ZipUtil.extractZipToDestination(fin, zipDirFile.getAbsolutePath());

			inputFile = Arrays.stream(zipDirFile.listFiles())
				.filter(f -> !f.isDirectory() && f.getName().endsWith(".csv"))
				.map(File::getAbsolutePath)
				.findFirst()
				.orElse(null);
		
			if (inputFile == null) {
				throw AppException.userError(ImportJobErrorCode.CSV_NOT_FOUND_IN_ZIP, zipFile);
			}
		} catch (Exception e) {
			throw AppException.serverError(e);
		} finally {
			IOUtils.closeQuietly(fin);
		}

		return inputFile;
	}

	private int getMaxRecsPerTxn() {
		return config.getAtomicitySize();
	}

	private ImportJob getJob(Long jobId) {
		ImportJob job = importJobDao.getById(jobId);
		if (job == null) {
			throw AppException.userError(ImportJobErrorCode.NOT_FOUND, jobId);
		}

		return ensureAccess(job);
	}

	private ImportJob ensureAccess(ImportJob job) {
		if (!config.isAccessAllowed(job)) {
			throw AppException.userError(ImportJobErrorCode.ACCESS_DENIED);
		}

		return job;
	}
	
	private String getDataDir() {
		return config.getDataDir();
	}
	
	private String getImportDir() {
		return getDataDir() + File.separator + "bulk-import";
	}
	
	private String getFilePath(String fileId) { 
		 return getImportDir() + File.separator + fileId;
	}
	
	private String getJobsDir() {
		return getDataDir() + File.separator + "bulk-import" + File.separator + "jobs";
	}
	
	private String getJobDir(Long jobId) {
		return getJobsDir() + File.separator + jobId;
	}
	
	private String getJobOutputFilePath(Long jobId) {
		return getJobDir(jobId) + File.separator + "output.csv";
	}
	
	private String getFilesDirPath(Long jobId) {
		return getJobDir(jobId) + File.separator + "files";
	}
	
	private boolean createJobDir(Long jobId) {
		return new File(getJobDir(jobId)).mkdirs();
	}
	
	private void moveToJobDir(String file, Long jobId) {
		String jobDir = getJobDir(jobId);

		//
		// Input file and its parent directory
		//
		File inputFile = new File(file);
		File srcDir = inputFile.getParentFile();

		//
		// Move input CSV file to job dir
		//
		inputFile.renameTo(new File(jobDir + File.separator + "input.csv"));

		//
		// Move other uploaded files to job dir as well
		//
		File filesDir = new File(srcDir + File.separator + "files");
		if (filesDir.exists()) {
			filesDir.renameTo(new File(getFilesDirPath(jobId)));
		}
	}
	
	private ImportJob createImportJob(ImportDetail detail) { // TODO: ensure checks are done
		AppException ae = new AppException(ErrorType.USER_ERROR);

		ImportJob job = new ImportJob();
		job.setCreatedBy((IUser) config.getAuth().getPrincipal());
		job.setCreationTime(Calendar.getInstance().getTime());
		job.setName(detail.getObjectType());
		job.setStatus(ImportJob.Status.IN_PROGRESS);
		job.setParams(detail.getObjectParams());

		setImportType(detail, job, ae);
		setCsvType(detail, job, ae);
		setDateAndTimeFormat(detail, job, ae);
		ae.checkAndThrow();

		return job;		
	}

	private void setImportType(ImportDetail detail, ImportJob job, AppException ose) {
		String importType = detail.getImportType();
		job.setType(StringUtils.isBlank(importType) ? ImportJob.Type.CREATE : ImportJob.Type.valueOf(importType));
	}

	private void setCsvType(ImportDetail detail, ImportJob job, AppException ose) {
		String csvType = detail.getCsvType();
		job.setCsvtype(StringUtils.isBlank(csvType) ? ImportJob.CsvType.SINGLE_ROW_PER_OBJ : ImportJob.CsvType.valueOf(csvType));
	}

	private void setDateAndTimeFormat(ImportDetail detail, ImportJob job, AppException ose) {
		String dateFormat = detail.getDateFormat();
		if (StringUtils.isBlank(dateFormat)) {
			dateFormat = config.getDateFmt();
		} else if (!Util.isValidDateFormat(dateFormat)) {
			ose.addError(ImportJobErrorCode.INVALID_DATE_FORMAT, dateFormat);
			return;
		}

		job.setDateFormat(dateFormat);

		String timeFormat = detail.getTimeFormat();
		if (StringUtils.isBlank(timeFormat)) {
			timeFormat = config.getTimeFmt();
		} else if (!Util.isValidDateFormat(dateFormat + " " + timeFormat)) {
			ose.addError(ImportJobErrorCode.INVALID_TIME_FORMAT, timeFormat);
			return;
		}

		job.setTimeFormat(timeFormat);
	}

	private class ImporterTask implements Runnable {
		private Authentication auth;
		
		private ImportJob job;

		private ImportListener callback;

		private boolean atomic;

		private int totalRecords = 0;

		private int failedRecords = 0;

		public ImporterTask(Authentication auth, ImportJob job, ImportListener callback, boolean atomic) {
			this.auth = auth;
			this.job = job;
			this.callback = callback;
			this.atomic = atomic;
			job.setAtomic(atomic);
		}

		@Override
		public void run() {
			SecurityContextHolder.getContext().setAuthentication(auth);

			ObjectReader objReader = null;
			CsvWriter csvWriter = null;
			try {
				ImporterContextHolder.getInstance().newContext();
				ObjectSchema schema = schemaFactory.getSchema(job.getName(), job.getParams());
				String filePath = getJobDir(job.getId()) + File.separator + "input.csv";
				csvWriter = getOutputCsvWriter(job);
				objReader = new ObjectReader(
					filePath, schema,
					job.getDateFormat(), job.getTimeFormat(), config.getFieldSeparator());

				List<String> columnNames = objReader.getCsvColumnNames();
				columnNames.add("OS_IMPORT_STATUS");
				columnNames.add("OS_ERROR_MESSAGE");

				csvWriter.writeNext(columnNames.toArray(new String[0]));

				ImportJob.Status status = processRows(objReader, csvWriter);
				saveJob(totalRecords, failedRecords, status);
			} catch (Exception e) {
				logger.error("Error running import records job", e);
				saveJob(totalRecords, failedRecords, ImportJob.Status.FAILED);

				String[] errorLine = null;
				if (e instanceof CsvException) {
					errorLine = ((CsvException) e).getErroneousLine();
				}

				if (errorLine == null) {
					errorLine = new String[] { e.getMessage() };
				}

				csvWriter.writeNext(errorLine);
				csvWriter.writeNext(new String[] { ExceptionUtils.getStackTrace(e) });
			} finally {
				ImporterContextHolder.getInstance().clearContext();
				runningJobs.remove(job.getId());

				IOUtils.closeQuietly(objReader);
				closeQuietly(csvWriter);

				//
				// Delete uploaded files
				//
				FileUtils.deleteQuietly(new File(getFilesDirPath(job.getId())));
			}

			notifyJobStatus();
		}

		private ImportJob.Status processRows(ObjectReader objReader, CsvWriter csvWriter) {
			if (atomic) {
				return txTmpl.execute(new TransactionCallback<ImportJob.Status>() {
					@Override
					public ImportJob.Status doInTransaction(TransactionStatus txnStatus) {
						ImportJob.Status jobStatus = processRows0(objReader, csvWriter);
						if (jobStatus == ImportJob.Status.FAILED || jobStatus == ImportJob.Status.STOPPED) {
							txnStatus.setRollbackOnly();
						}

						return jobStatus;
					}
				});
			} else {
				return processRows0(objReader, csvWriter);
			}
		}

		private ImportJob.Status processRows0(ObjectReader objReader, CsvWriter csvWriter) {
			boolean stopped;
			ObjectImporter<Object, Object> importer = importerFactory.getImporter(job.getName());
			if (job.getCsvtype() == ImportJob.CsvType.MULTIPLE_ROWS_PER_OBJ) {
				stopped = processMultipleRowsPerObj(objReader, csvWriter, importer);
			} else {
				stopped = processSingleRowPerObj(objReader, csvWriter, importer);
			}

			return stopped ? ImportJob.Status.STOPPED : (failedRecords > 0) ? ImportJob.Status.FAILED : ImportJob.Status.COMPLETED;
		}

		private boolean processSingleRowPerObj(ObjectReader objReader, CsvWriter csvWriter, ObjectImporter<Object, Object> importer) {
			boolean stopped = false;
			while (!job.isAskedToStop() || !(stopped = true)) {
				String errMsg = null;
				try {
					Object object = objReader.next();
					if (object == null) {
						break;
					}
	
					errMsg = importObject(importer, object, job.getParams());
				} catch (AppException ae) {
					errMsg = ae.getMessage();
				}
				
				++totalRecords;
				
				List<String> row = objReader.getCsvRow();
				if (StringUtils.isNotBlank(errMsg)) {
					row.add("FAIL");
					row.add(errMsg);
					++failedRecords;
				} else {
					row.add("SUCCESS");
					row.add("");
				}
				
				csvWriter.writeNext(row.toArray(new String[0]));
				if (totalRecords % 25 == 0) {
					saveJob(totalRecords, failedRecords, ImportJob.Status.IN_PROGRESS);
				}
			}

			return stopped;
		}

		private boolean processMultipleRowsPerObj(ObjectReader objReader, CsvWriter csvWriter, ObjectImporter<Object, Object> importer) {
			LinkedEhCacheMap<String, MergedObject> objectsMap =  new LinkedEhCacheMap<>();
			
			while (true) {
				String errMsg = null;
				Object parsedObj = null;
				
				try {
					parsedObj = objReader.next();
					if (parsedObj == null) {
						break;
					}
				} catch (Exception e) {
					errMsg = e.getMessage();
					if (errMsg == null) {
						errMsg = e.getClass().getName();
					}
				}
				
				String key = objReader.getRowKey();
				MergedObject mergedObj = objectsMap.get(key);
				if (mergedObj == null) {
					mergedObj = new MergedObject();
					mergedObj.setKey(key);
					mergedObj.setObject(parsedObj);
				}

				if (errMsg != null) {
					//
					// mark the object as processed whenever an error message is encountered.
					//
					mergedObj.addErrMsg(errMsg);
					mergedObj.setProcessed(true);
				}

				mergedObj.addRow(objReader.getCsvRow());
				mergedObj.merge(parsedObj);
				objectsMap.put(key, mergedObj);
			}

			boolean stopped = false;
			Iterator<MergedObject> mergedObjIter = objectsMap.iterator();
			while (mergedObjIter.hasNext() && (!job.isAskedToStop() || !(stopped = true))) {
				MergedObject mergedObj = mergedObjIter.next();
				if (!mergedObj.isErrorneous()) {
					String errMsg = null;
					try {
						errMsg = importObject(importer, mergedObj.getObject(), job.getParams());
					} catch (AppException ae) {
						errMsg = ae.getMessage();
					}

					if (StringUtils.isNotBlank(errMsg)) {
						mergedObj.addErrMsg(errMsg);
					}

					mergedObj.setProcessed(true);
					objectsMap.put(mergedObj.getKey(), mergedObj);
				}
				
				++totalRecords;
				if (mergedObj.isErrorneous()) {
					++failedRecords;
				}
				
				if (totalRecords % 25 == 0) {
					saveJob(totalRecords, failedRecords, ImportJob.Status.IN_PROGRESS);
				}
			}
			
			mergedObjIter = objectsMap.iterator();
			while (mergedObjIter.hasNext()) {
				MergedObject mergedObj = mergedObjIter.next();
				csvWriter.writeAll(mergedObj.getRowsWithStatus());
			}

			objectsMap.clear();
			return stopped;
		}
		
		private String importObject(final ObjectImporter<Object, Object> importer, Object object, Map<String, String> params) {
			try {
				final ImportObjectDetail<Object> input = new ImportObjectDetail<>();
				input.setCreate(job.getType() == ImportJob.Type.CREATE);
				input.setObject(object);
				input.setParams(params);
				input.setUploadedFilesDir(getFilesDirPath(job.getId()));

				txTmpl.execute(
					new TransactionCallback<Object>() {
						@Override
						public Object doInTransaction(TransactionStatus status) {
							return importer.importObject(input);
						}
					}
				);

				if (atomic) {
					//
					// Let's give a clean session for every object to be imported
					//
					clearSession();
				}

				return StringUtils.EMPTY;
			} catch (Exception e) {
				if (StringUtils.isBlank(e.getMessage())) {
					return "Internal Server Error";
				} else {
					return e.getMessage();
				}
			}
		}
		
		private void saveJob(long totalRecords, long failedRecords, ImportJob.Status status) {
			job.setTotalRecords(totalRecords);
			job.setFailedRecords(failedRecords);
			job.setStatus(status);
			
			if (status != ImportJob.Status.IN_PROGRESS) {
				job.setEndTime(Calendar.getInstance().getTime());
			}

			newTxTmpl.execute(new TransactionCallback<Void>() {
				@Override
				public Void doInTransaction(TransactionStatus status) {
					importJobDao.saveOrUpdate(job);
					return null;
				}
			});
		}

		private CsvWriter getOutputCsvWriter(ImportJob job)
		throws IOException {
			return CsvFileWriter.createCsvFileWriter(new FileWriter(getJobOutputFilePath(job.getId())), config.getFieldSeparator());
		}
				
		private void closeQuietly(CsvWriter writer) {
			if (writer != null) {
				try {
					writer.close();
				} catch (Exception e) {
											
				}
			}
		}

		private void notifyJobStatus() {
			config.onFinish(job);

			if (callback == null) {
				return;
			}

			if (job.getStatus() == ImportJob.Status.COMPLETED) {
				callback.success(job, totalRecords, failedRecords);
			} else {
				callback.fail(job, totalRecords, failedRecords);
			}
		}

		private void clearSession() {
			try {
				sessionFactory.getCurrentSession().flush();
			} catch (Exception e) {
				//
				// Oops, we encountered error. This happens when we've received database errors
				// like data truncation error, unique constraint etc ... We can't do much except
				// log and move forward
				//
				logger.info("Error flushing the database session", e);
			} finally {
				try {
					sessionFactory.getCurrentSession().clear();
				} catch (Exception e) {
					//
					// Something severely wrong...
					//
					logger.error("Error cleaning the database session", e);
				}
			}
		}
	}
}
