import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


public class RmiServer {

    private static final int registryPort = 1099; // Port for the shared RMI registry
    private static final String registryHost = "localhost"; // Host for the shared RMI registry
    private static final int[] defaultPorts = {10000, 10001, 10002, 10003, 10004};


    /**
     * Main method to start the RMI registry and server instances.
     * @param args Command line arguments (not used in this implementation).
     */
    public static void main(String[] args) {
        try {
            // Start RMI registry on port 1099
            LocateRegistry.createRegistry(registryPort);
            log("RMI registry started on port 1099.");
        } catch (RemoteException e) {
            log("RMI registry already running on port 1099.");
        }


        // Start each server in its own thread to simulate independent servers
        for (int i = 0; i < 5; i++) {
            int port = defaultPorts[i];
            boolean isCoordinator = (i == 0); // First server is the coordinator
           new Thread(() -> startServer(port, isCoordinator)).start();
        }
    }

    /**
     * Starts a server instance and binds it to the RMI registry.
     * @param port The port number for the server.
     * @param isCoordinator Boolean flag indicating whether this server is the coordinator.
     */
    private static void startServer(int port, boolean isCoordinator) {
        String serverName = "KeyValueStore";
        try {
            Registry registry = LocateRegistry.getRegistry(registryHost, registryPort); // Locate existing RMI registry
            KeyValueStoreImpl serviceInstance = new KeyValueStoreImpl(isCoordinator); // create a new instance of the service
            //KeyValueStoreImpl server = new KeyValueStoreImpl();
            registry.rebind(serverName, serviceInstance);
            log("Server " + serverName + " (Coordinator: " + isCoordinator + ") ready at port " + port);

        } catch (RemoteException e) {
            log("Error starting server " + serverName + ": " + e.getMessage());
        }
    }


    /**
     * Logs a message with the current date and time.
     * @param message The message to be logged.
     */
    private static void log(String message) {
        LocalDateTime now = LocalDateTime.now();
        String timestamp = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        System.out.println("[" + timestamp + "] " + message);
    }
}
