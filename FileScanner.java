import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class FileScanner {

	/* for multi-threading */
	private static Integer threadCount = 10;
	private static ExecutorService executor = null;
	private static volatile boolean ready = true;
	private static volatile boolean allOK = true;
	static final ReentrantLock dispatchLock = new ReentrantLock();
	static final ReentrantLock mapLock = new ReentrantLock();
	static final ReentrantLock doneLock = new ReentrantLock();
	static final ReentrantLock logLock = new ReentrantLock();
	static final ReentrantLock cntLock = new ReentrantLock();
	static final ReentrantLock nonMapLock = new ReentrantLock();
	static final ReentrantLock beatLock = new ReentrantLock();
	
	/* for I/O */
	private static BufferedWriter resultBW = null;
	private static BufferedWriter logBW = null;
	private static BufferedWriter mapBW = null;
	private static BufferedWriter cntBW = null;
	private static BufferedWriter hbBW = null;
	private static File output = null;
	private static File progressLog = null;
	private static File mapFile = null;
	private static File cntFile = null;
	private static File rootPath = null;
	private static File rootDir = null;
	private static File opConfig = null;
	private static File heartBeatFile = null;
	private static int totalCount = 0;
	private static AtomicReference<Integer> refCount = new AtomicReference<Integer>(0);
	private static String outputName = "";
	private static String runtimeDir = "";
	private static String startDir = "";
	private static String resultPath = "";
	private static String mapFileName = "";
	private static String pathMapFileStr = "";
	private static String heartBeatName = "scannerHeartBeat.txt";
	private static List<File> result = new ArrayList<File>();
	private static HashMap<File, Integer> targetDirMap = new HashMap<File, Integer>();
	private static HashMap<File, Integer> nonTargetMap = new HashMap<File, Integer>();
	private static HashMap<File, Integer> pathMap = new HashMap<File, Integer>();
	private static List<HashMap<String, String>> configList = new ArrayList<HashMap<String, String>>();
	private static List<String> infoList = new ArrayList<String>();
	private static List<File> sumList = new ArrayList<File>();
	private static File[] topDirList = null;
	private static List<String> outputIdxList = new ArrayList<String>();
	private static Long startTime = null;
	private static int resultSN = 0;
	private static boolean checkDateBefore = true;
	private static Long beforeThis = 0L;
	private static final long MAX_RESULT_FILE_SIZE = 1000000 * 10; // ~10MB
	private static final String BEFORE_DATE = "2015-05-01";
	
	/* for test */
	private static boolean Do_Console_Test = false;
	private static boolean RUN_SERVER = true;
	private static boolean resumeMapFirst = true;
	
	private static int signalInterval = 1 * 1000; // n * second
	private static Long lastSignalTime = 0L;
	private static int curSignal = 0;
	
	/* For config IO */
 	static OutputStream configOutput  = null;
 	static FileInputStream configInput = null;
	static Properties configProp = new Properties();
	private static String StartDirCfgStr = "ScanStartDir";
	private static String ThreadCntCfgStr = "ScanThreadCount";
 		
	public static void main(String[] args) throws IOException {
		ready = checkOptions(args);
		if (ready) {
			try {
				progressLog = new File(resultPath + "\\scanLog.txt");
				if (!progressLog.createNewFile()) {
					throw new Exception("The log file already exists.");
				}
				logBW = new BufferedWriter(new FileWriter(progressLog.getAbsoluteFile()));
				
				heartBeatFile = new File(runtimeDir + "\\" + heartBeatName);
				if (!heartBeatFile.exists()) {
					heartBeatFile.createNewFile();
				}
				
 			    initPathMap(); // retrieve data from pathmap.txt file.	
 			    writeScanConfig();
				startTime = new Date().getTime();			
				
				for (Map.Entry<File, Integer> kvp : pathMap.entrySet()) {
					File tf = kvp.getKey();
					int hadDone = kvp.getValue();
					targetDirMap.put(tf, hadDone);				
					if (hadDone == 0) {
						dispatchTasks(tf);
						targetDirMap.put(tf, 1);
						pathMap.put(tf, 1);
						rewritePathMap(1);
					}
				}
		
				// Process root at last.
				dispatchTasks(rootDir);
				targetDirMap.put(rootDir, 1);
				pathMap.put(rootDir, 1);
				logResult();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (!resumeMapFirst) { // Never happen. REMOVE Later
					targetDirMap.put(rootPath, 1);
					String finalMap = resultPath + "\\" + mapFileName + ".txt";
					writeTargetMapFile(finalMap);
				}

				if (logBW != null) {
					try {
						logBW.flush();
						logBW.close();
					} catch (IOException ioe) {
						ioe.printStackTrace();
					}
				}

				if (mapBW != null) {
					try {
						mapBW.flush();
						mapBW.close();

					} catch (IOException e) {
						e.printStackTrace();
					}
				}

				if (cntBW != null) {
					try {
						cntBW.flush();
						cntBW.close();

					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	public static void startScan(String[] args) throws IOException {
		ready = checkOptions(args);
		if (ready) {
			try {
				progressLog = new File(resultPath + "\\scanLog.txt");
				if (!progressLog.createNewFile()) {
					throw new Exception("The log file already exists.");
				}
				logBW = new BufferedWriter(new FileWriter(progressLog.getAbsoluteFile()));
				
				heartBeatFile = new File(runtimeDir + "\\" + heartBeatName);
				if (!heartBeatFile.exists()) {
					heartBeatFile.createNewFile();
				}
				
 			    initPathMap(); // retrieve data from pathmap.txt file.	
 			    writeScanConfig();
				startTime = new Date().getTime();			
				
				for (Map.Entry<File, Integer> kvp : pathMap.entrySet()) {
					File tf = kvp.getKey();
					targetDirMap.put(tf, 0);
					if (pathMap.get(tf) == 0) {
						dispatchTasks(tf);
						targetDirMap.put(tf, 1);
						pathMap.put(tf, 1);
						rewritePathMap(1);
					}
				}
		
				// Process root at last.
				dispatchTasks(rootDir);
				targetDirMap.put(rootDir, 1);
				pathMap.put(rootDir, 1);
				logResult();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (!resumeMapFirst) { // Never happen. REMOVE Later
					targetDirMap.put(rootPath, 1);
					String finalMap = resultPath + "\\" + mapFileName + ".txt";
					writeTargetMapFile(finalMap);
				}

				if (logBW != null) {
					try {
						logBW.flush();
						logBW.close();
					} catch (IOException ioe) {
						ioe.printStackTrace();
					}
				}

				if (mapBW != null) {
					try {
						mapBW.flush();
						mapBW.close();

					} catch (IOException e) {
						e.printStackTrace();
					}
				}

				if (cntBW != null) {
					try {
						cntBW.flush();
						cntBW.close();

					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}	
	
	private static void readScanConfig() throws Exception {
		try {
			configInput = new FileInputStream(opConfig);
			configProp.loadFromXML(configInput);
			startDir = configProp.getProperty(StartDirCfgStr);
			threadCount = Integer.valueOf(configProp.getProperty(ThreadCntCfgStr));
		} finally {
			if (configInput != null) {
				try {
					configInput.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}	

	private static void writeScanConfig() {
		try {
			HashMap<String, String> lastStartDir = new HashMap<String, String>();
			lastStartDir.put(StartDirCfgStr, startDir);
			configList.add(lastStartDir);		
			
			HashMap<String, String> buildThreadCount = new HashMap<String, String>();
			lastStartDir.put(ThreadCntCfgStr, threadCount.toString());
			configList.add(buildThreadCount);		
			
			for (HashMap<String, String> m : configList) {				
				for (Map.Entry<String, String> kvp : m.entrySet()) {
					configProp.setProperty(kvp.getKey(), kvp.getValue());
				}				
			}
			configOutput = new FileOutputStream(opConfig);
			configProp.storeToXML(configOutput, "PathMapBuilder properties");
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			if (configOutput != null) {
				try {
					configOutput.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static void initPathMap() throws Exception {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(new File(pathMapFileStr)));
			String preLine = null;
			String line = null;
			boolean keepReading = true;
			String delim = "[|]";
			while (keepReading) {
				line = br.readLine();
				if (line != null && !line.equalsIgnoreCase("")) {
					System.out.println("Got path: " + line);
					String[] dirInfo = line.split(delim);
					File dirPath = new File(dirInfo[0].trim());
					Integer val = Integer.valueOf(dirInfo[1].trim());
					pathMap.put(dirPath, val);
				} else {
					// Last line is the root directory, must be processed at last.
					String[] preDirInfo = preLine.split(delim);
					rootDir = new File(preDirInfo[0].trim());
					pathMap.remove(rootDir);
					keepReading = false;
				}
				preLine = line;
			}
			br.close();
		} catch (IOException ioe) {
			System.out.println(" Error! The path map file is unavailable.");
			ioe.printStackTrace();
		} finally {
			if (br != null) {
				br.close();
			}
		}
	}
	
	private static void rewritePathMap(Integer opt) throws Exception {
		BufferedWriter newMapBW = null;
		try {
			File newPathMapFile = new File(pathMapFileStr);
			if (newPathMapFile.exists()) {
				newPathMapFile.delete();
			}
			newPathMapFile.createNewFile();
			newMapBW = new BufferedWriter(new FileWriter(newPathMapFile.getAbsoluteFile(), false));
			
			for (Map.Entry<File, Integer> kvp : pathMap.entrySet()) {
				File subf = kvp.getKey();
				String dirpath = subf.getCanonicalPath();
				String content = String.format("%s|%s", dirpath, pathMap.get(subf).toString());
				newMapBW.write(content);
				newMapBW.newLine();
			}			
			String rtDirpath = rootDir.getCanonicalPath();
			String rtContent = String.format("%s|%s", rtDirpath, opt.toString());
			newMapBW.write(rtContent);
			newMapBW.newLine();				
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			newMapBW.flush();
			newMapBW.close();
		}
	}
	
	private static void logResult() throws IOException {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z"); 
		Long endTime = new Date().getTime();
		Long spentTime = endTime - startTime;
		String statusStr = "";
		if (allOK) {
			statusStr = "All Complete";
		} else {
			statusStr = "Errors happedned during processing.";
		}
		
		if (logBW != null) {
			logBW.write("-------------------Finished-------------------");
			logBW.newLine();
			logBW.write("Thread count: " + threadCount);
			logBW.newLine();
			logBW.write(String.format("Status: %s", statusStr));
			logBW.newLine();
			logBW.write("Starting directory: " + startDir);
			logBW.newLine();
			logBW.write("Last target date: " + df.format(new Date(beforeThis)));
			logBW.newLine();
			logBW.write("Totally found: " + refCount);
			logBW.newLine();
			logBW.write("Started at: " + df.format(new Date(startTime)));
			logBW.newLine();
			logBW.write("Finished at: " + df.format(new Date(endTime)));
			logBW.newLine();
			logBW.write("Spent milliseconds: " + spentTime);
			logBW.newLine();
			int minutes = (int) (spentTime / 60000);
			logBW.write("Spent minutes: " + minutes);
			logBW.newLine();
			int hours = minutes / 60;
			logBW.write("Spent hours: " + hours);
			logBW.newLine();
			logBW.newLine();
		}		
		System.out.println("\n\nFinished at: " +  df.format(new Date(endTime)));
		System.out.println("Found: " +  refCount);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static void dispatchTasks(File curStartDir) throws IOException {	
		Future[] taskHandlers = null;
		LinkedList<TaskParameter> taskWaitingList = null;
		int taskListSize = 0;		
		try {
			topDirList = curStartDir.listFiles();
			taskListSize = topDirList.length;
			taskWaitingList = new LinkedList<TaskParameter>();

			for (int i = 0; i < taskListSize; i++) {
				TaskParameter param = new TaskParameter();
				param.startFileDir = topDirList[i];
				param.resultBW = resultBW;
				param.logBW = logBW;
				param.output = output;
				param.totalCount = totalCount;
				param.outputName = outputName;
				param.runtimeDir = runtimeDir;
				param.startDir = startDir;
				param.result = result;
				param.infoList = infoList;
				param.sumList = sumList;
				param.outputIdxList = outputIdxList;
				param.startTime = startTime;
				param.resultSN = resultSN;
				param.checkDateBefore = checkDateBefore;
				param.beforeThis = beforeThis;
				param.MAX_RESULT_FILE_SIZE = MAX_RESULT_FILE_SIZE;
				param.refCount = refCount;
				//param.targetDirMap = targetDirMap;
				param.Do_Console_Test = Do_Console_Test;
				
				taskWaitingList.add(param);
			}

			executor = Executors.newFixedThreadPool(threadCount);
			taskHandlers = new Future[threadCount];

			/* Threads dispatching */
			while (taskWaitingList.size() > 0) {
				for (int i = 0; i < taskHandlers.length; i++) {
					if (taskHandlers[i] == null
							|| taskHandlers[i].isDone()
							|| taskHandlers[i].isCancelled()) {
						try {
							dispatchLock.lock();
							if (taskWaitingList.size() > 0) {
								taskHandlers[i] = (Future<FileScanHelper>) executor.submit(new FileScanHelper(taskWaitingList.pop(), i));
							}
						} finally {
							dispatchLock.unlock();
						}
					}
				}
			}

			executor.shutdown();
			executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
		} catch (Exception e) {
			e.printStackTrace();
			try {
				logBW.newLine();
				logBW.write("Error in dispatchTasks: " + e.getMessage());
				logBW.newLine();	
			} catch (IOException e1) {
				e1.printStackTrace();
			}	
		} 
	}
	
	private static boolean checkOptions(String[] args) {
		boolean goStart = true;
		try {
			runtimeDir = System.getProperty("user.dir");
			
			String	configFolderStr = runtimeDir + "\\config"; 	
			new File(configFolderStr).mkdir();
			
			opConfig = new File(configFolderStr + "\\buildcfg.xml");
			if (!opConfig.exists()) {
				opConfig.createNewFile();
			}

			if (Do_Console_Test) {
				startDir = "c:\\tmp";
			}
			else if (args.length < 1 && !resumeMapFirst) {
				readScanConfig();
		        //throw new Exception("A start directory is required.");		
			} else if (args[0] == "true") {
				resumeMapFirst = true;
			}			
			else {
				startDir = args[0];
			}

			if (args.length < 2) {
				threadCount = 1;
			} else {
				try {
				threadCount = Integer.parseInt(args[1]);
				} catch (NumberFormatException ne) {
					System.out.println("Invalid thread count");
					throw ne;
				}
			}			
			
			if (args.length < 3) {
				resultSN += 1;
				outputName = runtimeDir + "\\" + "scanResult_" + String.format("%06d", resultSN);
			} 

			SimpleDateFormat aDF = new SimpleDateFormat("yyyy-MM-dd");
			String inStr = "2015-05-01";
			try {
				if (Do_Console_Test) {
					beforeThis = aDF.parse(inStr).getTime();
				} else if (RUN_SERVER) {
					//beforeThis = aDF.parse(args[2]).getTime();
					beforeThis = aDF.parse(BEFORE_DATE).getTime(); // "2015-05-01"				
				} else if (args.length < 3) {
					System.out.println("Will get data before/include today.");
					System.out.println(" - Press 'Enter' or 'y' to continue.");
					System.out.println(" - Or input a date to use. (e.g. '2001-01-01') ");
					System.out.print("> ");
					inStr = System.console().readLine();

					if (inStr == null || inStr.equalsIgnoreCase("y")|| inStr.equalsIgnoreCase("")) {
						beforeThis = new Date().getTime();
					} else {
						beforeThis = aDF.parse(inStr).getTime();
					}
					
				} else {
					beforeThis = aDF.parse(args[2]).getTime();
				}
			} catch (Exception ex) {
				ex.printStackTrace();
				throw new Exception("Invalid input!");
			}
			output = new File(outputName);		
			SimpleDateFormat rDF = new SimpleDateFormat("yyyyMMddHHmmss");
			String runDate = rDF.format(new Date().getTime());
			resultPath = runtimeDir + "\\result";
			new File(resultPath).mkdir();
			resultPath = resultPath +"\\result_" + runDate;
			new File(resultPath).mkdir();
			configFolderStr = runtimeDir + "\\config"; 
			new File(configFolderStr).mkdir();
			pathMapFileStr = runtimeDir + "\\pathmap\\pathmap.txt";
			
			if (Do_Console_Test) {
				//keepRunning = false;
			}	
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("Usage: java FileInfoScanner [StartDirectory] [ThreadCount] [OutputPath] [LastDate e.g.'2001-01-01']");
			goStart = false;
		}		
		return goStart;
	}
	
	public static void writeTargetMapFile(String mapTmpFile) throws IOException {
		try {
			mapLock.lock();
			if (targetDirMap != null) {
				mapFile = new File(mapTmpFile);
		    	if (!mapFile.exists()) {
		    		mapFile.createNewFile();
				}
		    	mapBW = new BufferedWriter(new FileWriter(mapFile.getAbsoluteFile(), false));	
		    	
				for (Map.Entry<File, Integer> kvp : targetDirMap.entrySet()) {
					File dir = kvp.getKey();
					String path = dir.getCanonicalPath();
					int checked = (targetDirMap.get(dir).equals(1) ? 1 : 0);
					String content = String.format("%s|%s", path, checked);

					// System.out.println(content);
					mapBW.write(content);
					mapBW.newLine();
				}			    	
			}
		} finally {
			mapBW.flush();
			//mapBW.close();
			mapLock.unlock();
		}
	}

	public static void writeAllCount() throws IOException {
		cntFile = new File(resultPath + "\\tcount.txt");
    	cntBW = new BufferedWriter(new FileWriter(cntFile.getAbsoluteFile(), false));
    	System.out.println("Write Count: " + refCount.get().toString());
    	cntBW.write(refCount.get().toString());	
	}
	
	public static void readAllCount(File fin) {
		try {
			Integer val = -1;
			BufferedReader br = new BufferedReader(new FileReader(fin));
			String line = null;
			while ((line = br.readLine()) != null) {
				val = Integer.valueOf(line.trim());
			}
			br.close();
			refCount.set(val);
			System.out.println("Init Count: " + refCount.get().toString());
		} catch (IOException ioe) {
			System.out.println(ioe.getMessage());
		}		
	}
	
	public static void readTargetMapFile(File fin) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(fin));
			String line = null;
			while ((line = br.readLine()) != null) {
				String delim = "[|]";
				String[] dirInfo = line.split(delim);			
				File dirPath = new File(dirInfo[0].trim());
				Integer val = Integer.valueOf(dirInfo[1].trim());
				targetDirMap.put(dirPath, val);
			}
			br.close();
		} catch (IOException ioe) {
			System.out.println(ioe.getMessage());
		}
	}
	
	static class FileScanHelper implements Runnable {
		File startFileDir = null;
		int resultSN = 0;
		int eachCnt = 0;
		Integer handlerID = 0;
		BufferedWriter resultBW = null;
		File output = null;
		String outputName = "";
		String runtimeDir = "";
		String startDir = "";
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z"); 
		List<File> result = null;
		List<String> infoList = null;
		List<File> sumList = null;
		List<String> outputIdxList = null;
		AtomicReference<Integer> refCount = null;
		static BufferedWriter logBW = null;
		static int totalCount = 0;
		static Long startTime = null;
		static boolean checkDateBefore = true;
		static Long beforeThis = 0L;
		static long MAX_RESULT_FILE_SIZE = 0; // ~ 10MB

		/* for test */
		static boolean Do_Console_Test = false;	
		
		FileScanHelper(TaskParameter param, int tIdx) {
			startFileDir = param.startFileDir;
			logBW = param.logBW;
			eachCnt = 0;
			totalCount = param.totalCount;
			handlerID = tIdx;
			outputName = param.outputName;
			runtimeDir = param.runtimeDir;
			startDir = param.startDir;
			result = param.result;
			infoList = param.infoList;
			sumList = new ArrayList<File>();;
			outputIdxList = param.outputIdxList;
			startTime = param.startTime;
			resultSN = param.resultSN;
			checkDateBefore = param.checkDateBefore;
			beforeThis = param.beforeThis;
			MAX_RESULT_FILE_SIZE = param.MAX_RESULT_FILE_SIZE; 	
			refCount = param.refCount;
			Do_Console_Test = param.Do_Console_Test;
		}

		public void run() {
			String tID = handlerID.toString();//((Long)Thread.currentThread().getId()).toString();
			
			try {
			    output = new File(resultPath + "\\t_" + tID + "_result");//_" + String.format("%05d", resultSN));
			    
			    if (!output.exists()) {
			    	output.createNewFile();
				} 
		    	resultBW = new BufferedWriter(new FileWriter(output.getAbsoluteFile(), true)); // append
		    	
				if (Do_Console_Test) {
					resultBW.write("Starting directory: " + "[" + startFileDir	+ "]");
					resultBW.newLine();
				}	
				
				writeAllFileList(startFileDir, tID);
				
				if (Do_Console_Test) {
		    	    resultBW.write("Finished - found: " + eachCnt + " items");
		    	    resultBW.newLine();	
		    	    resultBW.newLine();
					writeLog(tID, 1, output, new Exception("FAKE"));
				}
			} catch (Exception ex) {
				ex.printStackTrace();
				System.out.println("Usage: java FileInfoScanner [TargetDirectory] [OutputFile] [LastDate e.g.'2001-01-01']");
			} finally {
				if (resultBW != null) {
					try {
						resultBW.flush();
						resultBW.close();
					} catch (IOException ioe) {
					    ioe.printStackTrace();
					}
				}
			}
		}

		private void writeAllFileList(File topDir, String tID) throws Exception {	
			if (topDir == null || !topDir.exists() || !topDir.canRead()
					|| (!topDir.isDirectory() && !topDir.isFile())) {
				throw new Exception(String.format("From t_%s, [%s] is not a valid directory.", tID, topDir));
			}
			
			/* Filter out repeat paths */
			if (!targetDirMap.containsKey(topDir)) { 
				try {
					writeSubDirFileList(topDir, tID);

					if (sumList.size() > 0) {
						writeToFile(sumList, tID);
						sumList.clear();
					}
				} catch (Exception ex) {
					ex.printStackTrace();
					writeLog(tID, -1, topDir, ex);
					throw ex;
				}
			}
		}
		
		private void heartBeat(Long lastSignalTime) {
			try {
				hbBW = new BufferedWriter(new FileWriter(heartBeatFile.getAbsoluteFile(), false));
				beatLock.lock();			
     		    hbBW.write(lastSignalTime.toString());
     		    hbBW.flush();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				beatLock.unlock();
			}
		}		
		
		/* Recursively */
		private void writeSubDirFileList(File topDir, String tID) throws IOException {
			Long cur = new Date().getTime();
			if (cur - lastSignalTime >= signalInterval) {
				if (curSignal == 0) {
					System.out.println("<" + tID + "> working on [" + topDir + "] .");
					curSignal = 1;
				} else {
					System.out.println("<" + tID + "> working on [" + topDir + "]  .");
					curSignal = 0;
				}				
				lastSignalTime = new Date().getTime();
				heartBeat(lastSignalTime);
			}				
			
			if (topDir.isDirectory() && !(targetDirMap.containsKey(topDir) && targetDirMap.get(topDir) == 1)) {

				List<File> pathList = new ArrayList<File>();
				try {
					pathList = Arrays.asList(topDir.listFiles());
				} catch (Exception e) {
					System.out.println("Invalid path: "	+ topDir.getCanonicalPath());
				}

				for (File p : pathList) {			
					if (sumList.size() >= 1000) {
						writeToFile(sumList, tID);
						sumList.clear();
					}
					
					writeSubDirFileList(p, tID);

					if (!nonTargetMap.containsKey(topDir)) {
						try {
							mapLock.lock();
							if (!nonTargetMap.containsKey(topDir)) {
								targetDirMap.put(topDir, 1);
							}
						} finally {
							mapLock.unlock();
						}
					}
				}
			} else if (topDir.isFile()) {
				File thisParent = topDir.getParentFile();
				
				if (checkDateBefore) {
					if (topDir.lastModified() <= beforeThis) {
						sumList.add(topDir);
						incrementCount();
					}
				} else {
					sumList.add(topDir);
					incrementCount();
				}
				
				try {
					nonMapLock.lock();
				    nonTargetMap.put(thisParent, 0);
				} finally {
					nonMapLock.unlock();
				}
				
				try {
					mapLock.lock();
				    targetDirMap.remove(thisParent);
				} finally {
					mapLock.unlock();
				}
				
			}
		}
		
	    private void incrementCount() {
	    	try {
				eachCnt += 1;
				cntLock.lock();
				int tCnt = refCount.get();
				tCnt += 1;
				refCount.set(tCnt);
	    	} finally {
	    		cntLock.unlock();
	    	}
	    }

		private String getFileInfo(File f) throws Exception {
			String fInfo = "";
			String fpath = "";
			String dStr = "";
			String lenStr = "";
			Long lastModifiedMS = 0L;
			Date lastdate = null;
			Long fbytes = 0L;
			try {
				fpath = f.getCanonicalPath();
				lastModifiedMS = f.lastModified();
				lastdate = new Date(lastModifiedMS);
				dStr = df.format(lastdate);
				fbytes = f.length();
				lenStr = fbytes.toString();
				fInfo = fpath + "|" + dStr + "|" + lenStr; 
			} catch (Exception ex) {
				System.out.println(fpath);
				ex.printStackTrace();
				fInfo = "  - Exception thrown in getFileInfo on :[" + fpath + "]" + " [" + df.format(lastdate) + "] " + "[" + fbytes +"]";
				writeErrorToFile(ex.toString() + fInfo);
				throw ex;
			}
			return fInfo;
		}
		
		private void writeLog(String tID, int status, File curDir, Exception e) {
			Long endTime = new Date().getTime();
			Long spentTime = endTime - startTime;
			
			logLock.lock();
			try {
				logBW.newLine();

				if (status == 1) {
					logBW.write("1");
					logBW.newLine();
					logBW.write("Status: Complete by Thread[" + tID + "]");
					logBW.newLine();
				} else {
					logBW.write("0");
					logBW.newLine();
					logBW.write("Status: Incomplete by Thread" + "[" + tID + "]");
					logBW.newLine();
					logBW.write("Last directory: " + curDir.getCanonicalPath());
					logBW.newLine();
					logBW.write("Exception at: " + df.format(new Date(endTime)));
					logBW.newLine();
					logBW.write(e.toString());
					logBW.newLine();
					
					if (allOK == true) {
						allOK = false;
					}
				}
				logBW.write("Starting directory: "	+ startFileDir.getCanonicalPath());
				logBW.newLine();
				logBW.write("Last target date: " + df.format(new Date(beforeThis)));
				logBW.newLine();
				logBW.write("Found: " + eachCnt);
				logBW.newLine();
				logBW.write("Started at: " + df.format(new Date(startTime)));
				logBW.newLine();
				logBW.write("Finished at: " + df.format(new Date(endTime)));
				logBW.newLine();
				logBW.write("Spent time in milliseconds: " + spentTime);
				logBW.newLine();
				int minutes = (int) (spentTime / 60000);
				logBW.write("In minutes: " + minutes);
				logBW.newLine();
				int hours = minutes / 60;
				logBW.write("In hours: " + hours);
				logBW.newLine();
				logBW.newLine();			
			} catch (IOException ioe) {
				ioe.printStackTrace();
			} finally {
				logLock.unlock();
			}
		}	

		private void writeToFile(List<File> files, String logID) throws IOException {
			File tmpF = null;
			try {
				if (files != null && !files.isEmpty()) {
					for (File f : files) {
						tmpF = f;
						resultBW.write(getFileInfo(f));
						resultBW.newLine();
					}
				}
			} catch (Exception ex) {
				ex.printStackTrace();
				String errInfo = "Error! - Exception when writeToFile on :["+ tmpF.getCanonicalPath() + "]";
				writeErrorToFile(errInfo + ex.toString());
			}
	    }
	   
	    private void writeErrorToFile(String errInfo) {
	    logLock.lock();
			try {
				logBW.write(errInfo);
				logBW.newLine();
			}catch (Exception e) {
				e.printStackTrace();
			} finally {
				logLock.unlock();
			}
		}
	}

	static class TaskParameter {
		public BufferedWriter resultBW = null;
		public BufferedWriter logBW = null;
		public File output = null;
		public int totalCount = 0;
		public String outputName = "";
		public String runtimeDir = "";
		public String startDir = "";
		public File startFileDir = null;
		public SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");
		public List<File> result = new ArrayList<File>();
		public List<String> infoList = new ArrayList<String>();
		public List<File> sumList = new ArrayList<File>();
		public List<String> outputIdxList = new ArrayList<String>();
		public Long startTime = null;
		public int resultSN = 0;
		public boolean checkDateBefore = true;
		public Long beforeThis = 0L;
		public long MAX_RESULT_FILE_SIZE = 0;// 1000000 * 10; // ~10MB	
		public AtomicReference<Integer> refCount = null;
		public boolean Do_Console_Test;
	}	
	
}
