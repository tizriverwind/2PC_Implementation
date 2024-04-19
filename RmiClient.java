import java.io.BufferedReader;
import java.io.FileReader;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Scanner;


public class RmiClient {

    
    private static final String characters_file = "famous_wizards.txt";


    /**
     * Main method to execute the RMI client.
     * @param args Command line arguments expecting host and port of the RMI registry.
     */
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Please specify IP address or hostname, and port number.");
            return;
        }
        String host = args[0];
        int port = parsePort(args[1]);

        try {
            Registry registry = LocateRegistry.getRegistry(host, port);
            KeyValueStore stub = (KeyValueStore) registry.lookup("KeyValueStore");
            // Pre-populate the server's store with data from file and interact with user
            populateData(stub);
            displayCurrentData(stub);

            handleUserCommands(stub);
        } catch (Exception e) {
            System.err.println("Client exception: " + e.toString());
            e.printStackTrace();
        }
    }

    /**
     * Parses the port number from a string argument.
     * @param portStr The port number as a string.
     * @return The port number as an integer, or -1 if the input is invalid.
     */
    private static int parsePort(String portStr) {
        try {
            int port = Integer.parseInt(portStr);
            if (port < 0 || port > 65535) {
                System.out.println("Invalid port number. Port must be between 0 and 65535.");
                return -1;
            }
            return port;
        } catch (NumberFormatException e) {
            System.out.println("Invalid port number format.");
            return -1;
        }
    }

    /**
     * Populates the remote key-value store with data from a text file.
     * @param stub The remote object stub for performing operations.
     */
    private static void populateData(KeyValueStore stub) throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(characters_file))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] input = line.split("=", 2);
                if (input.length == 2) {
                    String key = input[0].trim();
                    String value = input[1].trim();
                    stub.PUT(key, value);
                    System.out.println("Populated: " + key + " = " + value);
                }
            }
        }
    }


     /**
     * Displays the current contents of the key-value store to the user.
     * @param stub The remote object stub for fetching data.
     */
    private static void displayCurrentData(KeyValueStore stub) throws Exception {
        String keys = stub.LIST_KEYS();
        System.out.println("Current keys in the Key-Value store: " + keys);
    }


    
    /**
     * Handles user input to perform various operations on the remote key-value store.
     * @param stub The remote object stub used for command execution.
     * @throws Exception If there is an error processing commands or during remote method invocation.
     */
    private static void handleUserCommands(KeyValueStore stub) throws Exception {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter command (PUT <key> <value>, GET <key>, DELETE <key>, LIST_KEYS, or EXIT):");
    
        while (scanner.hasNextLine()) {
            String command = scanner.nextLine().trim();
            if ("exit".equalsIgnoreCase(command)) break;
    
            try {
                // Direct all PUT and DELETE commands to the coordinator server
                String response = "";
                if (command.startsWith("PUT ") || command.startsWith("DELETE ")) {
                    response = getCoordinatorStub().handleIncomingCommand(command);
                } else {
                    response = stub.handleIncomingCommand(command);
                }
                System.out.println(response);
            } catch (Exception e) {
                System.err.println("Command processing error: " + e.getMessage());
            }
        }
        scanner.close();
    }
    

    /**
     * Retrieves a stub to the coordinator server.
     * @return A KeyValueStore stub for the coordinator server.
     * @throws Exception If there is an error locating the registry or looking up the stub.
     */
    private static KeyValueStore getCoordinatorStub() throws Exception {
        Registry registry = LocateRegistry.getRegistry("localhost", 10000);
        return (KeyValueStore) registry.lookup("KeyValueStore");
    }
    
}