/**
 * Created by aishwarya on 30/5/17.
 */
import java.io.*;

public class ProcessBuilderExample {

        public static void main(String[] args) throws Exception {

            File f = new File("/home/aishwarya/apex/apex-malhar-master/demos/PracticeProgs/src");
            ProcessBuilder pb = new ProcessBuilder("javac", "/home/aishwarya/apex/apex-malhar-master/demos/PracticeProgs/src/FirstJVM.java");

            System.out.println("Run echo command");
            Process   process = pb.start();
            int errCode = process.waitFor();
            System.out.println("Echo command executed, any errors? " + (errCode == 0 ? "No" : "Yes"));
            System.out.println("Echo Output:\n" + output(process.getInputStream()));

            Thread.sleep(4000);
            ProcessBuilder p = new ProcessBuilder("java", "FirstJVM");
            p.directory(f);
            Process process2 = p.start();
            errCode = process2.waitFor();

            System.out.println("Echo command executed, any errors? " + (errCode == 0 ? "No" : "Yes"));
            System.out.println("ENter values");
            System.out.println("Echo Output:\n" + output(process2.getInputStream()));

        }
        private static String output(InputStream inputStream) throws IOException {
            StringBuilder sb = new StringBuilder();
            BufferedReader br = null;
            try {
                br = new BufferedReader(new InputStreamReader(inputStream));
                String line = null;
                while ((line = br.readLine()) != null) {
                    sb.append(line + System.getProperty("line.separator"));
                }
            }
            finally
            {
                br.close();
            }
            return sb.toString();
        }
}
