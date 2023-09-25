package system;

import java.io.IOException;

public class CommandRunner {

    public void pdalCliRunner(String command, String inputFilePath, String outputFilePath)
            throws InterruptedException, IOException {
        ProcessBuilder pb = new ProcessBuilder("pdal", command, inputFilePath, outputFilePath);
        pb.inheritIO(); // 将子进程的标准输入输出重定向到当前进程
        Process p = pb.start();
        p.waitFor();
    }

//    public void txt2LasRunner(String binPath, String inputFilePath, String outputFilePath)
//            throws InterruptedException, IOException {
//        ProcessBuilder pb = new ProcessBuilder(binPath, "-i", inputFilePath, "-o", outputFilePath);
//        pb.inheritIO(); // 将子进程的标准输入输出重定向到当前进程
//        Process p = pb.start();
//        p.waitFor();
//    }
}
