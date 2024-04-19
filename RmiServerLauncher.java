import java.io.IOException;

public class RmiServerLauncher {

    public static void main(String[] args) {
        // Define the ports for each server instance
        int[] ports = {10000, 10001, 10002, 10003, 10004};
        
        for (int i = 0; i < ports.length; i++) {
            ProcessBuilder processBuilder = new ProcessBuilder(
                "java",
                "-cp",
                System.getProperty("java.class.path"), // Use the current class path
                "RmiServer", // The main class to run
                String.valueOf(ports[i]), // Pass the port as an argument
                String.valueOf(i), // Pass the server index as an argument
                (i == 0) ? "coordinator" : "" // Optionally, designate the first instance as the coordinator
            );
            
            try {
                // Start the process without assigning it to a variable
                processBuilder.inheritIO().start();
                System.out.println("Started RmiServer on port " + ports[i] + (i == 0 ? " as coordinator" : ""));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
