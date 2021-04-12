# Assignment 3
Developed by: Rick Tesmond and Jordan Smith

## Overview
Building on Assignment 2, Assignment 3 not includes ownership strength on topic publishing, load balancing of Broker nodes, and a maintained history of topics.

To satisfy the requirements of this assignment:
* Utilized Zookeeper for load balancing and strength tracking, and ZMQ for history negotiation
* Load Balancing nodes based on Topic type (publisher type 1 or type 2), with each broker node providing redundency for both types.
  * Replica nodes are created and elected to lead node in-case of singular node failure
    
* Ownership Strength is tracked using the 1-based highest power, where publishers who do not have ownership are suspended until they can become the owner.
* History QoS is compliant with the assignment requirements and can provide access to historic samples of a given topic.

## Running the Program
System requirements: Ubuntu 20.04, ZMQ, Python3, Mininet, Xterm, Zookeeper, Wireshark \
Git clone URL: https://github.com/Tesmond-Smith-CS6831/Assignment3.git

**Ensure your mininet infrastructure and Zookeeper server can speak with each other**

1. In your Ubuntu environment, clone our repo and cd into the root of our repo.

2. Open a terminal session, and enter (without the quotes) "sudo mn -x --topo=tree,fanout=3,depth=2".
   * If everything is installed properly, you should see 9 hosts spin up, and an Xterm window open for each host.
    * If this did not occur, make sure you have mininet and xterm installed.
    
3. Spin up the middleware file on any of the host terminals by utilize the command line input: python3 middleware.py'
    * Zookeeper handles all leadership elections in which the middleware broker objects, have their own unique access ports
    * Middleware also handles leader replication and load balanced fault tolerance.
    
4. Spin up the Publishers on other hosts using 'python3 publisher.py ip-of-broker history-to-keep'
   * Example: 'python3 publisher.py 10.0.0.5 1/2 ZipCode(e.g. 23666)'
   * The publisher script can take three commandline arguments: 
      * ip-of-broker: IP address of the broker. Defaults to 'localhost'
      * publisher-flag: This input allows either input 1 - allows publishing of any topic vs. input 2 - publishing of singular topic. Defaults to 1
      * topic-to-publish: if input 2 is chosen, input for the specific topic to publish on. Defaults to 10001
          * Example: "python3 publisher.py localhost 2 45208"
      * history-to-keep: integer value of history samples to keep. Defaults to 10.
    
5. Spin up the Subscriber on other hosts using 'python3 subscriber.py ip-of-broker topic-zip'
   * Example: 'python3 subscriber.py 10.0.0.5 53715 10' 
   * Subscriber script takes three command line args:
     * broker-ip: IP address of middleware broker. 'localhost' if running locally. Default: localhost
     * topic-zip: zipcode you are interested in receiving weather info from. Default: '10001'
     * number of times to listen for a specefic topic. Default: 10
    
**Ensure you execute Middleware first! Publisher and Subscriber order does not matter following middleware execution!**
    
As soon as the system is set up, you should begin to see Subscribers receiving information for their subscriber topics!
 * As a note, depending on the toggle switch used in the publisher, the time to receive information to the subscriber may vary

## Testing
### Unittesting
In order for the unit tests to run properly, ensure your dev environment has the dependencies necessary but running 'pip install -r requirements.txt'. This will load your environment with the necessary libraries.
Once this step is complete, simply run "python3 test.py".

Note the majority of these tests are connectivity tests; please follow the instructions above to fully test out our code/process!

### Simulations and Graph Output
Tested using Mininet and Wireshark.

To ensure our code matched the necessary use cases (approach #1 and approach #2), we ran simulations of the expected I/O rates and roundtrip packet times for our cases. These were captured by running wireshark in tandem with mininet to monitor all traffic and acknowledgements through our middleware.

Similar to what we witnessed in Assignment #1, we saw the expected performance boost from approach #1 where publishers disseminated topic information without dedication to a specific topic resulted in correct data responses from one host to another in a matter of seconds, while with approach #2, where publishers disseminated dedication topic information, successful delivery of the topic data to interested subscribers was almost instantaneous.

Regarding the new fault-tolerant broker setup with Zookeeper, functionality is successfully maintained even in the instance of nodes being killed off. However, unlike Assignment 1 where we had no set fault tolerance at the broker level and therefore all communication stopped until another middleware was spun up, we see an expected drop in communication when the node get killed off, but immediately regains request-response functionality thanks to the data watcher functionality:

![IO Averages](./graphs/IOAverages.png)
The Node was killed at 26s, and only too 2s to instantiate the connection to the new broker node, and disseminate the new broker port info.

Following this trend, we don't notice any lapses in roundtrip time in the request/response cycles when the broker node was killed off either:

![ReqResp](graphs/RRTimeZookeeper.png)
 


