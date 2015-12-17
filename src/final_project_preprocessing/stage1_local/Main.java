package final_project_preprocessing.stage1_local;

import java.io.*;
import java.util.regex.Pattern;

/**
 * Created by Asher Cohen asherc@andrew
 */
public class Main implements Runnable {

    //regexp to remove digits, punctuations and whitespaces from documents
    private static Pattern pattern = Pattern.compile("(\\d|\\[|\\]|\\s|\\p{Punct})+");
    private String folderName;
    private int folderCount;
    private String fileNamePrefix;
    private int fileCount;

    public Main(String folderName, int folderCount) {
        this.folderName = folderName;
        this.folderCount = folderCount;
        this.fileNamePrefix = folderName.substring(folderName.lastIndexOf(".") + 1).replaceAll("-","").concat("_");
        this.fileCount = 0;
    }

    public static void main(String[] args) {

        if(args.length != 2) {
            System.err.println("Usage: java Main <folder> <folderCount>");
            System.exit(1);
        }

        new Thread(new Main(args[0], Integer.parseInt(args[1]))).start();
    }

    @Override
    public void run() {

        File folder = new File(this.folderName);
        File[] subDirs = folder.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.isDirectory();
            }
        });


        BufferedWriter writer = null;
        try {
            int count = 0;
            int idx = 0;
            for(File dir : subDirs) {

                if(count % this.folderCount == 0) {
                    if(writer != null) {
                        writer.close();
                    }
                    writer = new BufferedWriter(new FileWriter(new File(folder, this.folderName + "_" + idx++ + ".txt")));
                }
                writer.write(processSubDir(dir));
                writer.flush();
                count++;
            }
        }
        catch(IOException ioe) {
            ioe.printStackTrace();
        }


    }

    private String processSubDir(File dir) {

        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".txt");
            }
        });

        StringBuilder sb = new StringBuilder();
        for(File file : files) {
            sb.append(handleFile(file)).append("\n");
        }

        return sb.toString();
    }

    private String handleFile(File file) {

        StringBuilder sb = new StringBuilder();

        try(BufferedReader reader = new BufferedReader(new FileReader(file))) {

            sb.append(this.fileNamePrefix).append(this.fileCount++).append("\t");
            String line;

            while( (line = reader.readLine()) != null) {
                if(line.isEmpty()) {
                    continue;
                }

                sb.append(pattern.matcher(line.toLowerCase()).replaceAll(" ")).append(" ");
            }
        }
        catch(IOException ioe) {
            ioe.printStackTrace();
        }

        return sb.toString();
    }
}
