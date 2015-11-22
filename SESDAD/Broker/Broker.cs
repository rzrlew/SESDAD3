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
            get { return channel; }
            set { channel = value; }
        }


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
            remoteBroker.OnEventReceived = new NotifyEventDelegate(EventRouting);           
            remoteBroker.OnSubscribe += new PubSubEventDelegate(Subscription);
            remoteBroker.OnUnsubscribe += new PubSubEventDelegate(Unsubscription);
            remoteBroker.OnStatusRequest = new StatusRequestDelegate(SendStatus);
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
            RemotingServices.Marshal(remoteBroker, new Uri(configuration.processAddress).LocalPath.Split('/')[1]);
            Console.WriteLine("Broker is listening...");
        }

        private PublisherInfo SearchPublication(string address)
        {
            foreach (PublisherInfo info in publicationList)
            {
                if (info.publication_address.Equals(address))
                {
                    return info;
                }
            }
            throw new NotImplementedException("No publication found...");
        }
        private SubscriptionInfo SearchSubscription(string address)
        {
            foreach (SubscriptionInfo info in subscriptionsList)
            {
                if (info.subscription_address.Equals(address))
                {
                    return info;
                }
            }
            throw new NotImplementedException("No Subscription Found...");
        }
        private void CreatePublisherInfo(PublicationEvent e)
        {
            lock (publicationList)
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
        }
        private void SendToSubscriberWork(PublicationEvent e)
        {
            lock (subscriptionsList)
            {
                if (subscriptionsList.Any())
                {
                    foreach (SubscriptionInfo info in subscriptionsList)
                    {
                        foreach (string topic in info.topics)
                        {
                            if (CheckTopicInterest(e, topic))
                            {
                                RemoteSubscriber subscriber = (RemoteSubscriber)Activator.GetObject(typeof(RemoteSubscriber), info.subscription_address);
                                subscriber.NotifySubscriptionEvent(e);
                                lock (FIFOWaitQueue)
                                {
                                    FIFOWaitQueue.Remove(e);
                                }
                            }
                        }
                    }
                }
            }
        }
        public void FIFOSubscriberWork(string publicationAddress)
        {
            PublisherInfo info = SearchPublication(publicationAddress);
            foreach (PublicationEvent e in FIFOWaitQueue)
            {
                if (info.LastSeqNumber + 1 == e.SequenceNumber && info.publication_address.Equals(e.publisher))
                {
                    SendToSubscriberWork(e);
                    FIFOWaitQueue.Remove(e);
                    info.LastSeqNumber = e.SequenceNumber;
                    FIFOSubscriberWork(publicationAddress);
                    break;
                }
            }
        }
        public void FloodWork(PublicationEvent e)
        {
            string lastHopName = e.lastHop;
            e.lastHop = name;
            if (parentBroker != null && parentBroker.name != lastHopName)
            {
                Console.WriteLine("Sending event to " + parentBroker.name);
                parentBroker.RouteEvent(e);
            }

            foreach (RemoteBroker child in childBrokers)
            {
                if (child.name != lastHopName)
                {
                    Console.WriteLine("Sending event to " + child.name);
                    child.RouteEvent(e);
                }
            }
        }
        public void Subscription(string topic, string address)
        {
            lock (subscriptionsList)
            {
                try
                {
                    SubscriptionInfo info = SearchSubscription(address);
                    info.topics.Add(topic);
                }
                catch (NotImplementedException)
                {
                    SubscriptionInfo info = new SubscriptionInfo(topic, address);
                    subscriptionsList.Add(info);
                }
            }
        }
        public void Unsubscription(string topic, string address)
        {
            lock (subscriptionsList)
            {
                SubscriptionInfo info = SearchSubscription(address);
                info.topics.Remove(topic);
            }
        }
        public void SendEventLogWork(PublicationEvent e)
        {
            string logMessage = "[Broker - '" + name + "']----Got SESDAD Message Event!----" +
                                "'" + name + "'][From: '" + e.GetPublisher() + "'] Topic: " + e.topic +
                                "|| Message: " + e.eventMessage + "|| Sequence: " + e.GetSeqNumber().ToString() + Environment.NewLine;
            remoteSlave.SendLog(logMessage);
        }
        public void EventRouting(PublicationEvent e)
        {
            CreatePublisherInfo(e);
            lock (FIFOWaitQueue)
            { 
                FIFOWaitQueue.Add(e);
            }
            new Thread(() => SendEventLogWork(e)).Start();
            switch (configuration.orderMode)
            {
                case OrderMode.NoOrder:
                    new Thread(() => SendToSubscriberWork(e)).Start();
                    break;
                case OrderMode.FIFO:
                    new Thread(() => {lock(FIFOWaitQueue){FIFOSubscriberWork(e.publisher);}}).Start();
                    break;
                case OrderMode.TotalOrder:
                    throw new NotImplementedException("Total ordering is not implemented yet!");
            }
            switch (configuration.routingPolicy)
            {
                case "flooding":
                    new Thread(() => FloodWork(e)).Start();
                    break;
                case "filter":
                    throw new NotImplementedException("Filtering is not implemented yet!!!!!111!!!!");
            }

        }
        public string SendStatus()
        {
            string msg = "[Broker - " + this.name + "]";
            if (parentBroker != null)
            {
                msg +=  "Parent: " + this.parentBroker.name;
            }
            msg += "|| Children: ";
            foreach (string childAddress in configuration.childrenBrokerAddresses)
            {
                msg += childAddress + " ; ";
            }
            return msg;
        }
        public bool CheckTopicInterest(PublicationEvent e, string topic)
        {
            string[] eventArgs = e.topic.Split('/');
            string[] topicArgs = topic.Split('/');
            if (e.topic.Equals(topic))  // Event and Subscription topic are the same
            {
                return true;
            }
            if(topicArgs[topicArgs.Count() - 1].Equals('*')) // check if the subscriber has interest in the subtopics
            {
                for (int i = 0; !topicArgs[i].Equals('*'); i++)
                {
                    if (!eventArgs[i].Equals(topicArgs[i]))     // verifies that the topic corresponds to a subtopic
                    {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }
        private void SetParent(List<string> parentAddress)
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
    }

    public class PublisherInfo
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
