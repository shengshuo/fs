import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class CheckRepeatFiles {
	public static void main(String[] args) throws IOException {
		System.out.println("Find repeat files:");
		readFile(new File(args[0]));
		checkRepeat();
		BufferedWriter rBW = null;
		try {
			rBW = getWriter();
			Iterator<String> itr = fileCountMap.keySet().iterator();
			while (itr.hasNext()) {
				String s = (String) itr.next();
				if (fileCountMap.get(s) > 1) {
					System.out.println(s);
					rBW.write(s);
					rBW.newLine();
				}
			}
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} finally {
			rBW.flush();
			rBW.close();
		}
	}

    static ArrayList<String> fileList = new ArrayList<String>();
    static HashMap<String, Integer> fileCountMap = new HashMap<String, Integer>();
	
	private static void readFile(File fin) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(fin));
			String line = null;
			while ((line = br.readLine()) != null) {
				fileList.add(line);
			}
			br.close();
		} catch (IOException ioe) {
			System.out.println(ioe.getMessage());
		}
	}
	
	private static void checkRepeat() {
    	for (String s: fileList) {
    		if (!fileCountMap.containsKey(s)) {
    		    fileCountMap.put(s, 1);
    		} else {
    			int n = fileCountMap.get(s);
    			n += 1;
    			fileCountMap.put(s, n);
    		}
    	}
	}
	
	private static BufferedWriter getWriter() throws Exception {
		String runtimeDir = System.getProperty("user.dir");
		String rname = runtimeDir + "\\" + "repeat.txt";
		File resultFile = new File(rname);

		if (fileCountMap != null && fileCountMap.size() > 0) {
			if (!resultFile.createNewFile()) {
				throw new Exception("The repeat result file already exists.");
			}
			return new BufferedWriter(new FileWriter(resultFile.getCanonicalPath()));
		}
		return null;
	}

	@SuppressWarnings("unused")
	private static void writeResult() throws Exception {
		String runtimeDir = System.getProperty("user.dir");
		String rname = runtimeDir + "\\" + "repeat.txt";
		File resultFile = new File(rname);

		if (fileCountMap != null && fileCountMap.size() > 0) {
			if (!resultFile.createNewFile()) {
				throw new Exception("The repeat result file already exists.");
			}
			BufferedWriter rBW = new BufferedWriter(new FileWriter(resultFile.getCanonicalPath()));

			Iterator<String> itr = fileCountMap.keySet().iterator();
			try {
				while (itr.hasNext()) {
					String line = itr.next();
					rBW.write(line);
					rBW.newLine();
				}
			} finally {
				rBW.flush();
				rBW.close();
			}
		}
	}		
}
