# Assignment1
Developed by: Rick Tesmond and Jordan Smith

## Overview
In order to achieve anonymity between publishers and subscribers, we developed a middleware script that acts as a broker between publishers publishing topics, and subscribers consuming topics. The only requirements for the publisher and subscriber are that they know the host IP for the middleware broker; everything else is taken care of by the broker.

To satisfy the requirements of approach #1 and approach #2, we included a commandline flag for the publisher script, which allows the user to toggle between publisher for a specific topic vs. publishing information to the middleware regardless of topic.
  * When this flag is set to 1, the publisher will publish any topic information.
  * When this flag is set to 2, the publisher will publish information for a specific topic.

## Running the Program
System requirements: Ubuntu 20.04, ZMQ, Python3, Mininet, Xterm \
Git clone URL: https://github.com/Tesmond-Smith-CS6831/Assignment1.git

1. In your Ubuntu environment, clone our repo and cd into the root of our repo.
2. Open a terminal session, and enter (without the quotes) "sudo mn -x --topo=tree,fanout=3,depth=2".
   * If everything is installed properly, you should see 9 hosts spin up, and an Xterm window open for each host.
    * If this did not occur, make sure you have mininet and xterm installed.
    
3. Retrieve the IP of the host which will run as your middleware broker. Record the IP because you will use it when spinning up the Publishers and Subscribers.
   * You can run 'ip address show' on the host; the IP address will be within the "2." section, item 'inet'
    * Or the IP should be '10.0.0.(host number)'. For example, host5's IP address would be '10.0.0.5'.
    
4. Spin up the middleware broker on the host you just recorded the IP for by executing 'python3 middleware.py'
   * The default ports are '6663' and '5556' for publisher and subscriber respectively.
     * Functionality exists for dynamic port changes based on user preference. 
    * You can customize the ports by running 'python3 middleware.py pub-port sub-port'. For example, 'python3 5556 6444'
    
5. Spin up the Publishers on other hosts using 'python3 publisher.py ip-of-broker'
   * Example: 'python3 publisher.py 10.0.0.5'
   * The publisher script can take four commandline arguments: 
      * ip-of-broker: IP address of the broker. Defaults to 'localhost'
      * custom port: custom publisher port to use. Defaults to '6663'
        * If you choose to use a custom port, ENSURE IT MATCHES THE PUB PORT SET ON THE BROKER!
      * publisher-flag: This input allows either input 1 - allows publishing of any topic vs. input 2 - publishing of singular topic. Defaults to 1
      * topic-to-publish: if input 2 is chosen, input for the specific topic to publish on. Defaults to 10001
          * Example: "python3 publisher.py localhost 6663 2 45208"
    
6. Spin up the Subscriber on other hosts using 'python3 subscriber.py ip-of-broker topic-zip'
   * Example: 'python3 subscriber.py 10.0.0.5 53715 5556' 
   * Subscriber script takes three command line args:
     * broker-ip: IP address of middleware broker. 'localhost' if running locally.
     * topic-zip: zipcode you are interested in receiving weather info from. Default: '10001'
     * custom port: custom subscriber port. Default '5556'
        * If you choose to use a custom port, ENSURE IT MATCHES THE SUB PORT SET ON THE BROKER!
    
**Ensure you execute Middleware, Publisher, Subscriber in order!**
    
As soon as the system is set up, you should begin to see Subscribers receiving information for their subscriber topics!

## Testing
### Unittesting
In order for the unit tests to run properly, ensure your dev environment has the dependencies necessary but running 'pip install -r requirements.txt'. This will load your environment with the necessary libraries.
Once this step is complete, simply run "python3 test.py".

Note the majority of these tests are connectivity tests; please follow the instructions above to fully test out our code/process!

### Simulations and Graph Output
Tested using Mininet and Wireshark.

To ensure our code matched the necessary use cases (approach #1 and approach #2), we ran simulations of the expected I/O rates for our cases. These were captured by running wireshark in tandem with mininet to monitor all traffic and acknowledgements through our middleware.

We saw the expected performance boost from approach #1 where 100 requests flowed in a matter of microseconds, while with approach #2, successfully delivering the topic data to interested subscribers took seconds.

Overall, the I/O flow remained between 50-75 packets/second between both approaches, with intermittent spikes when subscribers were either joined or removed from the system. See output/performancecapture.pdf for this I/O graph.



