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

public class PathMapBuilder {
	private static BufferedWriter logBW = null;
	private static BufferedWriter topBW = null;
	private static BufferedWriter hbBW = null;
	private static BufferedWriter mapBW = null;
	private static File topMapFile = null;
	private static File mapFile = null;
	private static File mapLog = null;
	private static File rootPath = null;
	private static File heartBeatFile = null;
	private static String runtimeDir = "";
	private static String startDir = "";
	private static String topMapName = "topmap";
	private static String pathMapName = "pathmap";
	private static String mapPath = "";
	private static String heartBeatName = "heartbeat.txt";
	private static HashMap<File, Integer> targetDirMap = new HashMap<File, Integer>();
	private static HashMap<File, Integer> topDirMap = new HashMap<File, Integer>();
	private static Long startTime = null;
	private static Long endTime = null;
	private static Long spentTime = 0L;
	private static int scanCnt = 0;	
	private static int signalInterval = 2 * 1000; // n * second
	private static Long lastSignalTime = 0L;
	private static int curSignal = 0;
	private static final boolean resumeTopDirMap = true; // Should always be true.
	private static final boolean onlyScanFirstLayer  = false; // for test

	/* For Multi-threading */
	private static Integer threadCount = 10;
	private static ExecutorService executor = null;
	private static File[] firstDirList = null;
	static final ReentrantLock cntLock = new ReentrantLock();
	static final ReentrantLock pathMapLock = new ReentrantLock();
	static final ReentrantLock beatLock = new ReentrantLock();
	private static ArrayList<HashMap<File, Integer>> threadResultList = new ArrayList<HashMap<File, Integer>>();
	static final ReentrantLock dispatchLock = new ReentrantLock();
	
	private static List<HashMap<String, String>> configList = new ArrayList<HashMap<String, String>>();
	private static File opConfig = null;
	private static OutputStream configOutput  = null;
	private static FileInputStream configInput = null;
	private static Properties configProp = new Properties();
	private static String StartDirCfgStr = "BuildStartDir";
	private static String ThreadCntCfgStr = "BuildThreadCount";
	
	public static void main(String[] args) throws Exception {
		if (checkArgsOK(args)) {
			try {
				createResultFolders();				
				prepareWriter(1);
				
				if (!scanFirstLayer(startDir)) {
					throw new Exception("Failed to build top layer index.");
				}
         	
				if (resumeTopDirMap) {
				    readTopMapFile();
				}
   				
				prepareWriter(2);
				prepareWriter(3);
				writeBuildConfig();
				startTime = new Date().getTime();
				scanTopDirectories();				
				endProcess();
			} catch (Exception e) {		
				e.printStackTrace();
				logBW.write(e.getStackTrace().toString());
				logBW.newLine();
			} finally {
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
					} catch (IOException ioe) {
						ioe.printStackTrace();
					}	
				}
			}
		}
	}
	
	public static void startBuidPath(String[] args) throws Exception {
		System.out.println("Start path building!");		
		if (checkArgsOK(args)) {
			try {
				createResultFolders();				
				prepareWriter(1);
				
				if (!scanFirstLayer(startDir)) {
					throw new Exception("Failed to build top layer index.");
				}
         	
				if (resumeTopDirMap) {
				    readTopMapFile();
				}
   				
				prepareWriter(2);
				prepareWriter(3);
				writeBuildConfig();
				startTime = new Date().getTime();
				scanTopDirectories();				
				endProcess();
			} catch (Exception e) {		
				e.printStackTrace();
				logBW.write(e.getStackTrace().toString());
				logBW.newLine();
			} finally {
				if (logBW != null) {
					try {
						logBW.flush();
						logBW.close();
						mapBW.flush();
						mapBW.close();
					} catch (IOException ioe) {
						ioe.printStackTrace();
					}
				}
			}
		}
	}	
	
	private static boolean checkArgsOK(String[] args) {
		try {
			runtimeDir = System.getProperty("user.dir");
			String	configFolderStr = runtimeDir + "\\config"; 	
			new File(configFolderStr).mkdir();
			
			opConfig = new File(configFolderStr + "\\buildcfg.xml");
			if (!opConfig.exists()) {
				opConfig.createNewFile();
			}
			
			if (args.length < 1) {
				readBuildConfig();
				//throw new Exception("A start directory is required.");
				//startDir = "c:\\Files";
			} else {
				startDir = args[0];
			}
			
			if (args.length >= 2) {
				 threadCount = Integer.parseInt(args[1]);
			} 			
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("Usage: java PathMapBuilder [StartDirectory]");
			return false;
		}	
		return true;
	}
	
	/*
	 * Dispatch threads to work on each path.
	 */
	private static void scanTopDirectories() throws Exception {	
		for (int i = 0; i < threadCount; i++) {
            threadResultList.add(new HashMap<File, Integer>());
		}	
		
/* For Test */		
//if (new Date().getTime() - startTime > 20000) {
//	System.out.println("Existing system!");
//	System.exit(0);
//}


		for (Map.Entry<File, Integer> kvp : topDirMap.entrySet()) {
			File topDir = kvp.getKey();
			Integer val = kvp.getValue();
			if (val == 0) {
                System.out.println("Dispatch: " + topDir);				
				dispatch(topDir);
				mergeThreadResult();
				writePathMap(topDir);
				topDirMap.put(topDir, 1);		
				rewriteTopDirMap();					
			}
		}		
		String content = String.format("%s|%s", rootPath.getCanonicalPath(), 0);
		mapBW.write(content);
		mapBW.newLine();
	}
	
	private static void mergeThreadResult() {
		for (HashMap<File, Integer> hm : threadResultList) {
			targetDirMap.putAll(hm);
		    hm.clear();
		}		
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static void dispatch(File curStartDir) {
		Future[] taskHandlers = null;
		LinkedList<TaskInfo> taskWaitingList = null;
		int taskListSize = 0;		
		try {
			firstDirList = curStartDir.listFiles();
			taskListSize = firstDirList.length;
			taskWaitingList = new LinkedList<TaskInfo>();

			for (int i = 0; i < taskListSize; i++) {
				TaskInfo param = new TaskInfo();
				param.startFileDir = firstDirList[i];
				param.tStartTime = startTime;
				taskWaitingList.add(param);
			}

			new Date().getTime();
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
								taskHandlers[i] = (Future<PathBuildHelper>) executor.submit(new PathBuildHelper(taskWaitingList.pop(), i));
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
	
	private static void readBuildConfig() throws Exception {
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

	private static void writeBuildConfig() {
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
	
	private static void endProcess() {
		endTime = new Date().getTime();
		spentTime = endTime - startTime;
	
		writeLogFile();
		System.out.println("---Build map end---");
		System.out.println("\n\nTotally found: " + (targetDirMap.size() + 1));
		System.out.println("Time spent -");
		System.out.println(" in ms: " + spentTime);
		System.out.println(" in minutes: " + spentTime / 60000);
		System.out.println(" in hours: " + spentTime / 3600000);		
	}
	
	private static void readTopMapFile() throws IOException {
		File lastTopMapFile = new File(runtimeDir + "\\pathmap\\" + topMapName + ".txt");
		if (lastTopMapFile.exists()) {
			rebuildTopDirMap(lastTopMapFile);
		}		
	}
	
	private static void createResultFolders() throws IOException {
		rootPath = new File(startDir);		
		mapPath = runtimeDir + "\\pathmap";
		new File(mapPath).mkdir();
		new File(mapPath + "\\mapfiles").mkdir();
	}
	
	private static void prepareWriter(int opt) throws Exception {
		switch (opt) {
		case 1:
			mapLog = new File(mapPath + "\\mapLog.txt");
			if (!mapLog.exists()) {
				mapLog.createNewFile();
			}
			logBW = new BufferedWriter(new FileWriter(mapLog.getAbsoluteFile()));
			break;
		case 2:
			mapFile = new File(mapPath + "\\" + pathMapName + ".txt");	
			if (!mapFile.exists()) {
				mapFile.createNewFile();
			}
			mapBW = new BufferedWriter(new FileWriter(mapFile.getAbsoluteFile()));
			break;
		case 3:
			heartBeatFile = new File(runtimeDir + "\\" + heartBeatName);
			if (!heartBeatFile.exists()) {
				heartBeatFile.createNewFile();
			}
			break;
		default:
			System.out.println("Invalid writer option: " + opt);
		}
	}
	
	private static File getFirstLayerDir(File thisDir) throws Exception {
		List<File> pathList = new ArrayList<File>();
		try {
			pathList = Arrays.asList(thisDir.listFiles()); // poor performance in/under java 1.6!
		} catch (Exception e) {
			System.out.println("Invalid path: "	+ thisDir.getCanonicalPath());
		}	
		
		if (pathList.size() == 1) {
			File child = pathList.get(0);
			if (!child.isDirectory()) {
				return thisDir;
			}
			return getFirstLayerDir(child);
		} else {
			return thisDir;
		} 
	}		
	
	@SuppressWarnings("unused")
	private static boolean scanFirstLayer(String path) throws Exception {
		File inTopDir = new File(path);
		if (inTopDir == null || !inTopDir.exists() || !inTopDir.canRead()
				|| (!inTopDir.isDirectory() && !inTopDir.isFile())) {
			System.out.println(String.format("[%s] is not a valid path.", inTopDir));
			return false;
		}
		File topDir = getFirstLayerDir(inTopDir); 		
		topMapFile = new File(mapPath + "\\" + topMapName + ".txt");
		if (!resumeTopDirMap && !topMapFile.createNewFile()) {
			System.out.println("Invalid top directory path.");
			return false;
		}
		else if (topMapFile.exists()) {
			return true;
		}
		topBW = new BufferedWriter(new FileWriter(topMapFile.getAbsoluteFile(),	false));

		if (topDir.isDirectory()) {
			List<File> pathList = new ArrayList<File>();
			try {
				pathList = Arrays.asList(topDir.listFiles()); // poor performance!
			} catch (Exception e) {
				System.out.println("Invalid path: " + topDir.getCanonicalPath());
			}

			try {
				if (pathList.size() > 0) {
					for (File subp : pathList) {
						if (!subp.isFile()) {
							topDirMap.put(subp, 0);
							String dirpath = subp.getCanonicalPath();
							String content = String.format("%s|%s", dirpath, 0);
							topBW.write(content);
							topBW.newLine();
						}
					}
				}
			} finally {
				topBW.flush();
				topBW.close();
			}
		}

		if (onlyScanFirstLayer) {
			SimpleDateFormat logdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String curStr = logdf.format(new Date().getTime());
			logBW.write("First level directories of [" + startDir + "] were retrived at " + curStr);
			logBW.newLine();
			logBW.flush();
			logBW.close();
		    System.exit(1);
		}		
		return true;
	}

	private static void rewriteTopDirMap() throws Exception {
		topMapFile = new File(mapPath + "\\" + topMapName + ".txt");
		
		if (topMapFile.exists()) {
			topMapFile.delete();
		}
		
		if (!topMapFile.createNewFile()) {
			throw new Exception("Invalid top map directory path. :" + topMapFile);
		}
		topBW = new BufferedWriter(new FileWriter(topMapFile.getAbsoluteFile(),	false));

		try {
			for (Map.Entry<File, Integer> kvp : topDirMap.entrySet()) {
				File subf = kvp.getKey();
				if (!subf.equals(rootPath)) {
					String dirpath = subf.getCanonicalPath();
					String content = String.format("%s|%s", dirpath, topDirMap.get(subf).toString());
					topBW.write(content);
					topBW.newLine();
				}
			}
			String rootDirPath = rootPath.getCanonicalPath();
			if (topDirMap.get(rootPath) != null) {
				String rootContent = String.format("%s|%s", rootDirPath, topDirMap.get(rootPath).toString());
				topBW.write(rootContent);
				topBW.newLine();
			}
		} finally {
			topBW.flush();
			topBW.close();
		}	
	}

	private static void rebuildTopDirMap(File fin) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(fin));
			String line = null;
			String delim = "[|]";
			while ((line = br.readLine()) != null) {
				try {
					String[] dirInfo = line.split(delim);
					File dirPath = new File(dirInfo[0].trim());
					Integer val = Integer.valueOf(dirInfo[1].trim());
					topDirMap.put(dirPath, val);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
			br.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
	
	private static String convertNTFS(File topPath) {
		String pthSpltr = "^##^";
		String rtSpltr = "^%%^";
		String fName = "";
		try {
			fName = topPath.getCanonicalPath();
            fName = fName.replace(File.separator, pthSpltr);
            fName = fName.replace(":", rtSpltr);
		} catch (IOException e) {
			e.printStackTrace();
			return "";
		}
		return fName;
	}
	
	private static void writePathMap(File topPath) throws Exception {
		if (targetDirMap != null && targetDirMap.size() > 0) {
			String pathFile = convertNTFS(topPath);
			File pathMapFile = new File(mapPath + "\\mapfiles\\" + pathFile);
			if (pathMapFile.exists()) {
				pathMapFile.delete();
				pathMapFile.createNewFile();
			}
			BufferedWriter onePathMapBW = new BufferedWriter(new FileWriter(pathMapFile.getAbsoluteFile()));	

			try {
				for (Map.Entry<File, Integer> kvp : targetDirMap.entrySet()) {
					File dir = kvp.getKey();
					if (!dir.equals(rootPath)) {
						String dirpath = dir.getCanonicalPath();
						String content = String.format("%s|%s", dirpath, 0);
						onePathMapBW.write(content);
						onePathMapBW.newLine();
						mapBW.write(content);
						mapBW.newLine();
					}
				}
				targetDirMap.clear();
			} finally {
				onePathMapBW.flush();
				onePathMapBW.close();
			}		
		}
	}		
	
	private static void writeLogFile() {
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss Z"); 
			logBW.write("Found: " + (targetDirMap.size() + 1));
			logBW.newLine();
			logBW.write("Started at: " + df.format(new Date(startTime)));
			logBW.newLine();
			logBW.write("Finished at: " + df.format(new Date(endTime)));
			logBW.newLine();			
			logBW.write("Thread count: " + threadCount);
			logBW.newLine();			
			logBW.write("Spent time (ms): " + spentTime);
			logBW.newLine();		
			int minutes = (int) (spentTime / 60000);
			logBW.write("In minutes: " + minutes);
			logBW.newLine();
			int hours = minutes / 60;
			logBW.write("In hours: " + hours);
			logBW.newLine();
			logBW.newLine();			
		} catch (IOException e) {
			e.printStackTrace();
		} 
	} 
	
	static class PathBuildHelper implements Runnable {
		File startFileDir = null;
		int resultSN = 0;
		int eachCnt = 0;
		Integer handlerID = 0;
		BufferedWriter resultBW = null;
		File output = null;
		String outputName = "";
		String runtimeDir = "";
		String startDir = "";
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss Z"); 
		List<File> result = null;
		List<String> infoList = null;
		List<File> sumList = null;
		List<String> outputIdxList = null;
		AtomicReference<Integer> refCount = null;
		static BufferedWriter logBW = null;
		static int totalCount = 0;
		static Long tStartTime = null;
		static boolean checkDateBefore = true;
		static Long beforeThis = 0L;
		static long MAX_RESULT_FILE_SIZE = 0; // ~ 10MB

		/* for test */
		static boolean Do_Console_Test = false;	
		
		PathBuildHelper(TaskInfo param, int tIdx) {
			startFileDir = param.startFileDir;
			handlerID = tIdx;
			logBW = param.logBW;
			tStartTime = param.tStartTime;
			eachCnt = 0;
			totalCount = param.totalCount;
			outputName = param.outputName;
			runtimeDir = param.runtimeDir;
			startDir = param.startDir;
			result = param.result;
			infoList = param.infoList;
			sumList = new ArrayList<File>();;
			outputIdxList = param.outputIdxList;		
			resultSN = param.resultSN;
			checkDateBefore = param.checkDateBefore;
			beforeThis = param.beforeThis;
			MAX_RESULT_FILE_SIZE = param.MAX_RESULT_FILE_SIZE; 	
			refCount = param.refCount;
		}

		public void run() {
			try {
				threadBuildAllMap();
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

		private void threadBuildAllMap() throws Exception {
			if (startFileDir == null || !startFileDir.exists() || !startFileDir.canRead()
					|| (!startFileDir.isDirectory() && !startFileDir.isFile())) {
				throw new Exception(String.format("[%s] is not a valid path.", startFileDir.getCanonicalPath()));
			}

			threadBuildMap(startFileDir);
		}
		
		private boolean threadBuildMap(File thisDir) throws Exception {
			
			/* Heart Beat, For TEST */
//			java.util.Random rand = new java.util.Random();
//			Thread.sleep(rand.nextInt(20)*1000);
						
			if (scanCnt >= 1000) {
				System.out.println("Total retrived: " + targetDirMap.size());
				scanCnt = 0;
			}

			Long cur = new Date().getTime();
			if (cur - lastSignalTime >= signalInterval) {
				if (curSignal == 0) {
					System.out.println("<" + handlerID + "> working on [" + thisDir + "] .");
					curSignal = 1;
				} else {
					System.out.println("<" + handlerID + "> working on [" + thisDir + "]  .");
					curSignal = 0;
				}
				lastSignalTime = new Date().getTime();
				heartBeat(lastSignalTime);
			}

			if (thisDir.isDirectory()) {
				List<File> pathList = new ArrayList<File>();
				try {
					pathList = Arrays.asList(thisDir.listFiles()); 
				} catch (Exception e) {
					System.out.println("Invalid path: "	+ thisDir.getCanonicalPath());
				}
				
				if (pathList.size() < 2) {
					return false;
				}

				try {
					pathMapLock.lock();
					threadResultList.get(handlerID).put(thisDir, 0);
				} finally {
					pathMapLock.unlock();
				}				
				
				boolean validDir = true;
				for (File subp : pathList) {
					scanCnt += 1;
					if (threadBuildMap(subp) == false) {
						validDir = false;
						break;
					}
				}
				
				if (validDir) {
					File parent = thisDir.getParentFile();
					try {
						pathMapLock.lock();
						if (threadResultList.get(handlerID).containsKey(parent)) {
							threadResultList.get(handlerID).remove(thisDir);
						}
					} finally {
						pathMapLock.unlock();
					}					
				} else if (!validDir) {
					try {
						pathMapLock.lock();
						threadResultList.get(handlerID).remove(thisDir);

					} finally {
						pathMapLock.unlock();
					}
				}
			} else if (thisDir.isFile()) {
				File thisParent = thisDir.getParentFile();
				if (!thisParent.getCanonicalPath().equals(startFileDir)) {
					try {
						pathMapLock.lock();
					    threadResultList.get(handlerID).remove(thisParent);
					} finally {
						pathMapLock.unlock();
					}
					return false;
				}
			}
			return true;
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
	}
	
	static class TaskInfo {
		public BufferedWriter resultBW = null;
		public BufferedWriter logBW = null;
		public File output = null;
		public int totalCount = 0;
		public String outputName = "";
		public String runtimeDir = "";
		public String startDir = "";
		public File startFileDir = null;
		public SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss Z");
		public List<File> result = new ArrayList<File>();
		public List<String> infoList = new ArrayList<String>();
		public List<File> sumList = new ArrayList<File>();
		public List<String> outputIdxList = new ArrayList<String>();
		public Long tStartTime = null;
		public int resultSN = 0;
		public boolean checkDateBefore = true;
		public Long beforeThis = 0L;
		public long MAX_RESULT_FILE_SIZE = 0;// 1000000 * 10; // ~10MB	
		public AtomicReference<Integer> refCount = null;
		public boolean Do_Console_Test;
	}	
}
		