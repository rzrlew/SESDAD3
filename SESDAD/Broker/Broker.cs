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
        string name;
        SESDADBrokerConfig configuration;
        TcpChannel channel;
        RemotePuppetSlave remoteSlave;
        RemoteBroker remoteBroker;
        RemoteBroker parentBroker = null;
        List<RemoteBroker> childBrokers = new List<RemoteBroker>();

        List<SubscriptionInfo> subscriptionsList = new List<SubscriptionInfo>();// stores information regarding subscriptions[subscriber][topic]  
        List<PublisherInfo> publicationList = new List<PublisherInfo>();

        List<PublicationEvent> FIFOWaitQueue = new List<PublicationEvent>();
        

        public static void Main(string[] args)
        {
            Console.WriteLine("Getting Broker configuration from site slave at " + args[0]);
            TcpChannel temp_channel = new TcpChannel();
            ChannelServices.RegisterChannel(temp_channel, true);

            RemotePuppetSlave remotePuppetSlave = (RemotePuppetSlave) Activator.GetObject(typeof(RemotePuppetSlave), args[0]);
            SESDADBrokerConfig configuration = (SESDADBrokerConfig) remotePuppetSlave.GetConfiguration();
            Console.WriteLine("Starting broker channel on port: " + new Uri(configuration.processAddress).Port);
            TcpChannel channel = new TcpChannel(new Uri(configuration.processAddress).Port);
            ChannelServices.UnregisterChannel(temp_channel);
            ChannelServices.RegisterChannel(channel, true);
            Broker bro = new Broker(configuration, args[0]);
            bro.configuration = configuration;
            bro.Channel = channel;
            bro.remoteSlave = remotePuppetSlave;       
            Console.WriteLine("press key to terminate...");
            Console.ReadLine();
            Console.WriteLine("Ending Broker Process: " + bro.name);
        }

        public Broker(SESDADBrokerConfig config, string slaveAddress)
        {
            Console.WriteLine("---Starting Broker---");
            Console.WriteLine("Creating remote broker on " + config.processAddress);
            configuration = config;
            remoteBroker = new RemoteBroker(slaveAddress);
            remoteBroker.floodEvents += new NotifyEventDelegate(FIFOFlood);
            remoteBroker.OnSubscribe += new PubSubEventDelegate(Subscription);
            remoteBroker.OnUnsubscribe += new PubSubEventDelegate(Unsubscription);
            remoteBroker.OnStatusRequest = new StatusRequestDelegate(SendStatus);
            //remoteBroker.OnAdvertise += new PubSubEventDelegate(AdvertisePublish);
            Name = config.processName;
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

        public string SendStatus()
        {
            string msg = "";
            if (parentBroker != null)
            {
                msg += "[Broker - " + this.name + "] Parent: " + this.parentBroker.name + Environment.NewLine;
            }
            foreach(string childAddress in configuration.childrenBrokerAddresses)
            {
                msg += "[Broker - " + this.name + "] Child: " + childAddress + Environment.NewLine;
            }
            return msg;
        }

        public void Subscription(string topic, string address)
        {
            try
            {
                SubscriptionInfo info = SearchSubscription(address);
                info.topics.Add(topic);
            }
            catch(NotImplementedException)
            {
                SubscriptionInfo info = new SubscriptionInfo(topic, address);
                subscriptionsList.Add(info);
            }
        }

        public void Unsubscription(string topic, string address)
        {
            SubscriptionInfo info = SearchSubscription(address);
            info.topics.Remove(topic);
        }

        public void SendEventLogWork(PublicationEvent e)
        {
            remoteSlave.SendLog("'" + configuration.processName + "' got event!" + Environment.NewLine + "[Topic]: " + e.topic + Environment.NewLine + "[Message]: " + e.eventMessage);
        }

        public PublisherInfo SearchPublication(string address)
        {
            foreach(PublisherInfo info in publicationList)
            {
                if (info.publication_address.Equals(address))
                {
                    return info;
                }
            }
            throw new NotImplementedException("No publication found...");
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
            throw new NotImplementedException("No Subscription Found...");
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

        public bool isRoot() // checks if broker belongs to Root Node
        {
            return (parentBroker == null) ? true : false;
        }

        public bool checkTopicInterest(PublicationEvent e, string topic)
        {
            string[] eventArgs = e.topic.Split('/');
            string[] topicArgs = topic.Split('/');
            if (e.topic.Equals(topic))  // Event and Subscription topic are the same
            {
                return true;
            }
            if (eventArgs.Count() < topicArgs.Count())
            {
                return false;
            }
            if(topicArgs[topicArgs.Count() - 1].Equals('*')) // check if the subscriber has interest in the subtopics
            {
                for (int i = 0; i < topicArgs.Count(); i++)
                {
                    if (!eventArgs[i].Equals(topicArgs[i]))     // verifies that the topic corresponds to a subtopic
                    {
                        return false;
                    }
                }
            }
            return true;
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

        public void Flood(PublicationEvent e)
        {
            Thread thread_log = new Thread(() => SendEventLogWork(e));
            Thread thread_1 = new Thread(() => FloodWork(e));
            Thread thread_2 = new Thread(() => sendToSubscriberWork(e));
            thread_log.Start();
            thread_1.Start();
            thread_2.Start();
        }
        public void FIFOFlood(PublicationEvent e)
        {
            Thread thread_log = new Thread(() => SendEventLogWork(e));
            Thread thread_1 = new Thread(() => FloodWork(e));
            Thread thread_2 = new Thread(() => sendFIFOEvents(e));
            thread_log.Start();
            thread_1.Start();
            thread_2.Start();
        }
        public void createPublisherInfo(PublicationEvent e)
        {
            try
            {
                PublisherInfo info = SearchPublication(e.publisher);
                if (!info.topics.Contains(e.topic))
                {
                    info.topics.Add(e.topic);
                }
            }
            catch (NotImplementedException)
            {
                PublisherInfo info = new PublisherInfo(e.topic, e.publisher);
                publicationList.Add(info);
            }
        }

        public void sendToSubscriberWork(PublicationEvent e)
        {
            if (subscriptionsList.Any())
            {
                foreach (SubscriptionInfo info in subscriptionsList)
                {
                    foreach (string topic in info.topics)
                    {
                        if (checkTopicInterest(e, topic))
                        {
                            RemoteSubscriber subscriber = (RemoteSubscriber)Activator.GetObject(typeof(RemoteSubscriber), info.subscription_address);
                            subscriber.NotifySubscriptionEvent(e);
                        }
                    }
                }
            }
        }

        public void FloodWork(PublicationEvent e)
        {
            string lastHopName = e.lastHop;
            e.lastHop = name;
            createPublisherInfo(e);

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
       
        public void getNextFIFOEvent(string address)
        {
            PublisherInfo info = SearchPublication(address);
            foreach (PublicationEvent e in FIFOWaitQueue)
            {
                if(e.SequenceNumber == info.LastSeqNumber + 1)
                {
                    sendFIFOEvents(e);
                }
            }
        }
        public void sendFIFOEvents(PublicationEvent e)
        {
            if (subscriptionsList.Any())
            {
                foreach (SubscriptionInfo info in subscriptionsList)
                {
                    foreach (string topic in info.topics)
                    {
                        if (checkTopicInterest(e, topic))
                        {
                            RemoteSubscriber subscriber = (RemoteSubscriber)Activator.GetObject(typeof(RemoteSubscriber), info.subscription_address);
                            PublisherInfo publisherInfo = SearchPublication(e.publisher);
                            if (publisherInfo.LastSeqNumber + 1 == e.SequenceNumber)
                            {
                                subscriber.NotifySubscriptionEvent(e);
                                ++publisherInfo.LastSeqNumber;
                                getNextFIFOEvent(e.publisher);
                            }
                            else
                            {
                                FIFOWaitQueue.Add(e);
                            }
                        }
                    }
                }
            }
        }
    }



    public class PublisherInfo  // alterar esta estrutura para List<string> topic
    {
        public string publication_address;
        public List<string> topics = new List<string>();
        public int LastSeqNumber = 0;

        public PublisherInfo(string topic, string address)
        {
            this.topics.Add(topic);
            publication_address = address;
        }
    }

    public class SubscriptionInfo
    {
        public string subscription_address;
        public List<string> topics = new List<string>();

        public SubscriptionInfo(string topic, string address)
        {
            topics.Add(topic);
            subscription_address = address;
        }
    }
}
