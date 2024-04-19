import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;


public interface KeyValueStore extends Remote {
  String PUT(String key, String value) throws RemoteException;
  String GET(String key) throws RemoteException;
  String DELETE(String key) throws RemoteException;
  String LIST_KEYS() throws RemoteException;
  String handleIncomingCommand(String command) throws RemoteException;

  // Lock the kvpair at prepare phase; commit and abort both release the lock
  boolean prepare(String transactionID, HashMap<String, String> lockStatusMap) throws RemoteException;
  void commit(String transactionID) throws RemoteException;
  void abort(String transactionID) throws RemoteException;
}