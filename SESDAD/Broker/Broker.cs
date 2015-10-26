using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Threading;
using SESDAD;

namespace SESDADBroker
{
    public class Broker
    {
        SESDADBrokerConfig configuration;
        TcpChannel channel;
        RemoteBroker remoteBroker;
        Queue<Event> eventQueue;
        RemoteBroker parentBroker = null;
        List<RemoteBroker> childBrokers = new List<RemoteBroker>();
        List<SubscriptionInfo> subscriptionsList = new List<SubscriptionInfo>();

        List<string> topicList = new List<string>();
        string name;

        public static void Main(string[] args)
        {
            Console.WriteLine("Getting Broker configuration from site slave at " + args[0]);
            TcpChannel temp_channel = new TcpChannel();
            ChannelServices.RegisterChannel(temp_channel, true);

            RemotePuppetSlave remotePuppetSlave = (RemotePuppetSlave) Activator.GetObject(typeof(RemotePuppetSlave), args[0]);
            SESDADBrokerConfig configuration = (SESDADBrokerConfig) remotePuppetSlave.GetConfiguration();
            Console.WriteLine("Starting broker channel on port: " + new Uri(configuration.brokerAddress).Port);
            TcpChannel channel = new TcpChannel(new Uri(configuration.brokerAddress).Port);
            ChannelServices.UnregisterChannel(temp_channel);
            ChannelServices.RegisterChannel(channel, true);
            Broker bro = new Broker(configuration);
            bro.configuration = configuration;
            bro.Channel = channel;
            Console.WriteLine(  "write [flood] for flooding of Event..." + Environment.NewLine 
                                + "write [quit] to terminate Broker process..." + Environment.NewLine
                                + "write [topic] to set subscribe interest..." + Environment.NewLine
                                + "write [show] to print queue...");
            string s = Console.ReadLine();
            while (!s.Equals("quit"))
            {
                if (s.Equals("topic"))
                {
                    Console.WriteLine("Insert interested topic...");
                    string topic = Console.ReadLine();
                    bro.addTopic(topic);
                }
                if (s.Equals("flood"))
                {
                    Console.WriteLine("insert: [Message] [Topic]...");
                    string input = Console.ReadLine();
                    String[] array = input.Split(' '); 
                    bro.Flood(new Event(array[0], array[1], bro.name));
                }
                if (s.Equals("show"))
                {
                    Console.WriteLine("printing Queue...");
                    while (bro.remoteBroker.floodList.Any())
                    {
                        Event e = bro.remoteBroker.floodList.Dequeue();
                        if (bro.checkTopic(e.topic))
                        {
                            Console.WriteLine(e.Message());
                        }
                    }                                 
                }
                Console.WriteLine("write [flood] for flooding of Event..." + Environment.NewLine
                                + "write [quit] to terminate Broker process..." + Environment.NewLine
                                + "write [topic] to set subscribe interest..." + Environment.NewLine
                                + "write [show] to print queue...");
                s = Console.ReadLine();
            }
            Console.WriteLine("Ending Broker Process: " + bro.name);
        }

        public Broker(SESDADBrokerConfig config)
        {
            Console.WriteLine("---Starting Broker---");
            Console.WriteLine("Creating remote broker on " + config.brokerAddress);
            configuration = config;
            remoteBroker = new RemoteBroker();
            remoteBroker.floodEvents += new NotifyEvent(Flood);
            remoteBroker.OnSubscribe += new SubscriptionEvent(Subscription);
            remoteBroker.OnUnsubscribe += new SubscriptionEvent(Unsubscription);
            eventQueue = new Queue<Event>();
            Name = config.brokerName;
            if (config.parentBrokerAddress != null)
            {
                List<string> pList = new List<string>();
                pList.Add(config.parentBrokerAddress);
                SetParent(pList);
            }
            if (configuration.childrenBrokerAddresses.Any())
            {
                SetChildren(config.childrenBrokerAddresses);
            }
            RemotingServices.Marshal(remoteBroker, this.name);
            Console.WriteLine("Broker is listening...");
        }

        public void Subscription(string topic, string address)
        {
            try
            {
                SubscriptionInfo info = SearchSubscription(address);
                info.topics.Add(topic);
            }
            catch(NotImplementedException ex)
            {
                SubscriptionInfo subscription = new SubscriptionInfo(topic, address);
                subscriptionsList.Add(subscription);
            }
        }

        public void Unsubscription(string topic, string address)
        {
            SubscriptionInfo info = SearchSubscription(address);
            info.topics.Remove(topic);
        }

        public SubscriptionInfo SearchSubscription(string address)
        {
            foreach(SubscriptionInfo info in subscriptionsList)
            {
                if (info.subscription_address.Equals(address))
                {
                    return info;
                }
            }
            throw new NotImplementedException("No Subscription Found!");
        }

        public string Name
        {
            set
            {
                name = value;
                remoteBroker.name = name;
            }
        }
        public TcpChannel Channel
        {
            get {   return channel;     }
            set {   channel = value;    }
        }

        public void addTopic(string topic)
        {
            topicList.Add(topic);
        }

        public bool checkTopic(string topic)
        {
            foreach (string s in topicList)
            {
                if (s.Equals(topic))
                {
                    return true;
                }
            }
            return false;
        }

        public bool isRoot() // checks if broker belongs to Root Node
        {
            return (parentBroker == null) ? true : false;
        }

        private void SetParent(List<string> parentAddress) // add parent broker addresss to list
        {
            Console.WriteLine("Adding parent broker at: " + parentAddress.ElementAt(0));
            parentBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), parentAddress.ElementAt(0));
        }

        private void SetChildren(List<string> childrenAddresses)
        {
            foreach (string childAddress in childrenAddresses)
            {
                Console.WriteLine("Adding child broker at: " + childAddress);
                childBrokers.Add((RemoteBroker)Activator.GetObject(typeof(RemoteBroker), childAddress));
            }
        }

        public void Flood(Event e)
        {
            Thread thr = new Thread(() => FloodWork(e));
            thr.Start();
        }

        public void FloodWork(Event e)
        {
            string lastHopName = e.lastHop;
            //Console.WriteLine("Flooding event: " + e.Message() + " from " + e.lastHop + " to all children!");
            e.lastHop = name;
            remoteBroker.floodList.Enqueue(e);
            if (parentBroker != null && parentBroker.name != lastHopName)
            {
                Console.WriteLine("Sending event to " + parentBroker.name);
                parentBroker.Flood(e);
            }
            foreach (RemoteBroker child in childBrokers)
            {
                if (child.name != lastHopName)
                {
                    Console.WriteLine("Sending event to " + child.name);
                    child.Flood(e);
                }
            }
        }
    }

    public class SubscriptionInfo
    {
        public string subscription_address;
        public List<string> topics = new List<string>();

        public SubscriptionInfo(string topic, string address)
        {
            this.topics.Add(topic);
            subscription_address = address;
        }


    }
}
