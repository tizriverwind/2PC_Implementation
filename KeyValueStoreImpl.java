import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;



public class KeyValueStoreImpl extends UnicastRemoteObject implements KeyValueStore {

    private boolean isCoordinator = false;
    private ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();
    // Mapping from transaction ID to operations involved in the transaction
    private ConcurrentHashMap<String, Map<String, String>> transactionOperations = new ConcurrentHashMap<>();

    private static final String registryHost = "localhost";
    private static final int registryPort = 1099;
    

    /**
     * Constructor for KeyValueStoreImpl that prepares the instance for remote communication.
     * @param isCoordinator Indicates if this instance should act as the transaction coordinator.
     * @throws RemoteException If a remote method call fails.
     */
    public KeyValueStoreImpl(boolean isCoordinator) throws RemoteException {
        super(); // Calls the constructor of UnicastRemoteObject to prepare this object for remote communication
        this.isCoordinator = isCoordinator;

    }

    /**
     * Stores a key-value pair in the key-value store.
     * @param key The key to store the value under.
     * @param value The value to be stored.
     * @return A success message if the operation is successful, otherwise an error message.
     * @throws RemoteException If a remote method call fails.
     */
    @Override
    public String PUT(String key, String value) throws RemoteException {
        if (key == null || value == null) {
            throw new RemoteException("Key or value cannot be null.");
        }
    
        // Ensuring thread-safety with double-checked locking
        ReentrantLock lock = locks.computeIfAbsent(key, k -> new ReentrantLock());
    
        if (!lock.tryLock()) {
            return "Unable to PUT: Key-value pair is currently locked by another transaction.";
        }
    
        try {
            String transactionID = UUID.randomUUID().toString();
            HashMap<String, String> operationDetails = new HashMap<>();
            operationDetails.put(key, "PUT:" + value);
    
            if (prepare(transactionID, operationDetails)) {
                commit(transactionID);  // Commit both locally and across all servers
                return "PUT operation completed successfully.";
            } else {
                abort(transactionID);  // Abort both locally and across all servers
                return "PUT operation aborted.";
            }
        } catch (Exception e) {
            abort(null);  // Attempt to abort if an exception occurs
            return "Error during PUT operation: " + e.getMessage();
        } finally {
            lock.unlock();  // Ensure the lock is always released
        }
    }
      
    
    /**
     * Retrieves the value associated with a given key.
     * @param key The key whose value is to be retrieved.
     * @return The value associated with the key, or a not found message if the key does not exist.
     * @throws RemoteException If a remote method call fails.
     */
    @Override
    public String GET(String key) throws RemoteException {
        String value = store.get(key);
        if (value != null) {
            log("GET operation - Key: '" + key + "', Value: '" + value + "'");
            return value;
        } else {
            log("GET operation failed - Key: '" + key + "' not found in HashMap.");
            return "Key '" + key + "' not found in HashMap.";
        }
    }

    /**
     * Deletes a key-value pair from the store.
     * @param key The key whose associated value is to be deleted.
     * @return A success message if the key was deleted, otherwise a not found message.
     * @throws RemoteException If a remote method call fails.
     */
    @Override
    public String DELETE(String key) throws RemoteException {
        ReentrantLock lock = locks.computeIfAbsent(key, k -> new ReentrantLock());
        if (!lock.tryLock()) return "DELETE failed: Key is currently locked by another transaction.";
        try {
            if (store.remove(key) != null) {
                log("DELETE operation - Key: '" + key + "', removed successfully.");
                return "DELETE success for key: '" + key + "'.";
            } else {
                log("DELETE operation failed - Key: '" + key + "' not found.");
                return "Key '" + key + "' not found.";
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Lists all keys in the store.
    * @return A comma-separated list of keys.
    * @throws RemoteException if a remote communication error occurs.
    */
    @Override
    public String LIST_KEYS() throws RemoteException {
        String keys = String.join(", ", store.keySet());
        log("LIST_KEYS operation - Keys: " + keys);
        return keys;
    }

    /**
     * Logs a message with the current timestamp.
    * @param message The message to be logged.
    */
    private static void log(String message) {
        LocalDateTime now = LocalDateTime.now();
        String timestamp = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        System.out.println("[" + timestamp + "] " + message);
    }

 
    /**
     * Prepares a transaction by locking the necessary keys.
     * @param transactionID The identifier for the transaction.
     * @param operations A map of keys and their associated operations.
     * @return true if all locks were successfully acquired, false otherwise.
     * @throws RemoteException If a remote method call fails.
     */
    @Override
    public boolean prepare(String transactionID, HashMap<String, String> operations) throws RemoteException {
        boolean canLock = true;
        for (Map.Entry<String, String> operation : operations.entrySet()) {
            String key = operation.getKey();
            if (!"GET".equals(operation.getValue())) {
                locks.computeIfAbsent(key, k -> new ReentrantLock());
                canLock = canLock && locks.get(key).tryLock();
            }
        }

        if (canLock) {
            transactionOperations.put(transactionID, operations);
            return true;
        } else {
            // Release any acquired lock if not all can be locked
            operations.forEach((key, value) -> {
                if (locks.containsKey(key)) {
                    locks.get(key).unlock();
                }
            });
            return false;
        }
    }


    /**
     * Commits a transaction by applying all operations and releasing locks.
     * @param transactionID The identifier for the transaction.
     * @throws RemoteException If a remote method call fails.
     */
    @Override
    public void commit(String transactionID) throws RemoteException {
        Map<String, String> operations = transactionOperations.get(transactionID);
        if (operations != null) {
            operations.forEach((key, value) -> {
                if (!"GET".equals(value)) {
                    if ("DELETE".equals(value)) {
                        store.remove(key);
                    } else {
                        store.put(key, value);
                    }
                    locks.get(key).unlock();
                }
            });
            transactionOperations.remove(transactionID);
        }
    }

    @Override
    public void abort(String transactionID) throws RemoteException {
        Map<String, String> operations = transactionOperations.get(transactionID);
        if (operations != null) {
            operations.keySet().forEach(key -> {
                if (locks.containsKey(key)) {
                    locks.get(key).unlock();
                }
            });
            transactionOperations.remove(transactionID);
        }
    }


    /**
     * Aborts a transaction by releasing all involved locks without applying operations.
     * @param transactionID The identifier for the transaction.
     * @throws RemoteException If a remote method call fails.
     */
    @Override
    public String handleIncomingCommand(String command) throws RemoteException {
        String[] parts = command.split("\\s+", 3);
        if (parts.length < 2) return "Error: Invalid command format.";

        String cmdType = parts[0].toUpperCase();
        String key = parts.length > 1 ? parts[1] : null;
        String value = parts.length > 2 ? parts[2] : null;
        String transactionID = UUID.randomUUID().toString();  // Transaction ID generated here for PUT/DELETE
        HashMap<String, String> operations = new HashMap<>();

        try {
            switch (cmdType) {
                case "PUT":
                case "DELETE":
                    if (key == null || (cmdType.equals("PUT") && value == null))
                        return "Error: Key and value required for " + cmdType + ".";
                    operations.put(key, cmdType + ":" + value);
                    if (isCoordinator) {
                        boolean success = coordinatorDuty(transactionID, operations);
                        return success ? "Transaction " + transactionID + " committed successfully." : "Transaction " + transactionID + " aborted.";
                    } else {
                        return "Error: Non-coordinator server received a transaction command.";
                    }
                case "GET":
                    if (key == null) return "Error: Key required for GET.";
                    return this.GET(key);
                case "LIST_KEYS":
                    return this.LIST_KEYS();
                default:
                    return "Unsupported command type: " + cmdType + ".";
            }
        } catch (Exception e) {
            return "Error processing command: " + e.getMessage();
        }
    }


    /**
     * Coordinates a transaction across multiple servers, ensuring all participants are prepared before committing.
     * This method handles the two-phase commit protocol by first preparing all servers and then either committing
     * the transaction across all servers if all preparations were successful, or aborting the transaction if any
     * server fails to prepare.
     *
     * @param transactionID The unique identifier of the transaction to coordinate.
     * @param operations The operations to be performed in the transaction, keyed by the item key with the operation type and value.
     * @return true if the transaction was successfully committed across all servers, false if the transaction was aborted.
     * @throws RemoteException If a remote method invocation fails.
     */
    private static boolean coordinatorDuty(String transactionID, HashMap<String, String> operations) {
        try {
            Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
            boolean allYes = true;

            // Prepare phase: ask all participants to prepare (lock resources and ready for transaction)
            for (int i = 0; i < 5; i++){
                try {
                    KeyValueStore participant =(KeyValueStore) registry.lookup("KeyValueServer" + i);
                    if (!participant.prepare(transactionID, operations)) {
                        allYes = false;
                        break; // Abort if any participant cannot prepare
                    }
                } catch (Exception e) {
                    allYes = false;
                    log("Error during prepare phase with server " + i +": " + e.getMessage());
                    break; // Abort the transaction on error

                }
            }

            // Commit or Abort phase, based on preparation results
            if (allYes) {
                for (int i =0; i < 5; i++) {
                    try {
                        KeyValueStore participant = (KeyValueStore) registry.lookup("KeyValueServer" + i);
                        participant.commit(transactionID); // commit the transaction
                    } catch (Exception e) {
                        log("Error during commit phase with server " + i + ": " + e.getMessage());
                    }
                }
            } else {
                for (int i =0; i < 5; i++) {
                    try {
                        KeyValueStore participant = (KeyValueStore) registry.lookup("KeyValueServer" + i);
                        participant.abort(transactionID); // Abort

                } catch (Exception e) {
                    log("Error during abort phase with server " + i+ ": " + e.getMessage());
                    // Hnadling abort error
                }
            }
        }
    } catch (Exception e){
        log("Failed to start transaction " + transactionID + ": " + e.getMessage());
        return false;
    }
    return true;
    }

}