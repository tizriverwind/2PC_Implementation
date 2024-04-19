Directory Structure
- RmiServer.java - The RMI server implementation.
- RmiClient.java - The RMI client implementation.
- KeyValueStore.java - The RMI interface.
- KeyValueStoreImpl.java - The implementation of the RMI interface.
- famous_wizards.txt - A text file containing key-value pairs to populate the key-value store.


Compiling the program
Open a terminal in your project directory and run the following commands to compile the Java files:
***
javac KeyValueStore.java
javac KeyValueStoreImpl.java
javac RmiServer.java
javac RmiClient.java
***


Basic Command Structure
***
java RmiServer 
***


To start the RmiServer program, you use one command and the RMI register and all 5 servers will automatically run on default ports. The RMI register runs on port 1099, and the server ports are ranging from  10000 to 10004.
The first server running on port 10000 is the coordinator. 

To start the RmiClient program, you will need to follow below structure percisely:
***
java RmiClient 1ocalhost 1099
***


Important Notes
- Ensure the RMI Registry is Running: Before starting your server instances, make sure the RMI registry is running on the designated registryPort and registryHost. If you're using the default localhost and port 1099, you can start the registry using the "rmiregistry" command in the background or programmatically in another Java application.



Executive Summary
In undertaking this project, I faced a range of challenges that tested my technical skills and resilience to their limits. Initially, setting up five replica server instances proved to be a daunting task. The complexities involved demanded a high level of effort and persistence, particularly when troubleshooting issues that arose unexpectedly.

The most significant hurdle was implementing the Two-Phase Commit (2PC) protocol. Integrating 2PC into the existing client-server code structure was not only challenging but required multiple iterations of reconstruction—each more complex than the last. This experience underscored a recurring issue I have faced: translating theoretical concepts into practical applications, a gap that continues to present considerable challenges. During the process I had to reconstructed the design several time because of unresolved issue, and each time it took longer than the last.

Despite rigorous efforts, the project concluded with some unresolved issues by the end of the testing phase. I did not have enought time to continue debugging and honestly, I think I needed help identifying what went wrong. This was a stark reminder of the realities of working with complex systems and the unpredictability of software development, and I'm glad this project has been a huge lesson-learned for me:
    1. Theoretical vs. Practical application: I identified a significant gap between theoretical concepts and their practical implementation. 

    2. Importance of Robust Testing: The unresolved issues at the end of the testing phase highlighted the need for more comprehensive and adaptable testing, which I did not do.

    3. Adaptability in problem-Solving: Facing and overcoming the numerous challenges throughout this project emphasized the importance of adaptability and persistence in problem-solving, and also good mental health.

