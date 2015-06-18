import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;


public class FileReaderTest {
	public static void main(String[] args) {
	    System.out.println("File read test:");
	    File tf = new File("C:\\works\\FileScanner\\hbase_insert\\wms\\po_receiving_pod2\\result\\result_20150508143200\\t_0_result");
	    //File tf = new File("C:\\works\\FileScanner\\hbase_insert\\wms\\wirpod\\result\\result_20150508143954\\t_0_result");
	    try {
			ParseFileInfo(tf);
			System.out.println("Done.");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	private static boolean ParseFileInfo(File tf) throws Exception {
		int tCnt = 0;
		BufferedReader br = null;
		try {
			if (!tf.exists()) {
				System.out.println("Return false");
				return false;
			}
			br = new BufferedReader(new FileReader(tf));
			String line = null;
			String delim = "[|]";
			while ((line = br.readLine()) != null) {
				System.out.println("Get in");
				if (line.trim() != "") {
										
					String[] fileInfo = line.split(delim);
					for (String info : fileInfo) {
						System.out.println("Info: " + info);
					}
//					cntLock.lock();
					tCnt += 1;
					System.out.println("count: " + tCnt);
				}
			}
			return true;
		} catch (IOException ioe) {
			System.out.println(" Error! The file is unavailable.");
			ioe.printStackTrace();
			return false;
		} finally {
			if (br != null) {
				br.close();
			}
//			cntLock.unlock();
		}
	}
}
