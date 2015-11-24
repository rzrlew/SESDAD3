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
        List<NeighborForwardingFilter> neighborFilters = new List<NeighborForwardingFilter>();
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
            remoteBroker.OnAdvertisePublisher = new PubSubEventDelegate(HandlePubAdvertisement);
            remoteBroker.OnAdvertiseSubscriber = new PubSubEventDelegate(HandleSubAdvertisement);
            remoteBroker.OnPublication = new PublicationRequestDelegate(HandlePublisherPublication);
            remoteBroker.OnAdvertiseUnsubscriber = new PubSubEventDelegate(HandleUnsubAdvertisement);
            remoteBroker.OnFilterUpdate = new PubSubEventDelegate(HandleFilterUpdate);
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
            PopulateFilters();
            RemotingServices.Marshal(remoteBroker, new Uri(configuration.processAddress).LocalPath.Split('/')[1]);
            Console.WriteLine("Broker is listening...");
        }

        private void PopulateFilters()
        {
            foreach(string childAddress in configuration.childrenBrokerAddresses)
            {
                neighborFilters.Add(new NeighborForwardingFilter(childAddress));
            }
            if(parentBroker != null)
                neighborFilters.Add(new NeighborForwardingFilter(configuration.parentBrokerAddress));
        }

        public void EventRouting(PublicationEvent e)
        {
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
                    new Thread(() => { lock (FIFOWaitQueue) { FIFOSubscriberWork(e.publisher); } }).Start();
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
                    new Thread(() => FilterWork(e)).Start();
                    break;
            }
        }

        public void FilterWork(PublicationEvent e)
        {
            foreach (NeighborForwardingFilter filter in neighborFilters)
            {
                if (checkForward(filter, e))
                {
                    RemoteBroker remoteBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), filter.neighborAddress);
                    remoteBroker.RouteEvent(e);
                }
            }
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
            new Thread(() =>
            {
                lock (subscriptionsList)
                {
                    try
                    {
                        SubscriptionInfo info = SearchSubscription(address);
                        if (info.topics.Find(x => x.Equals(topic) ? true : false) == null)
                            info.topics.Add(topic);
                    }
                    catch (NotImplementedException)
                    {
                        SubscriptionInfo info = new SubscriptionInfo(topic, address);
                        subscriptionsList.Add(info);
                    }
                }
                if (configuration.routingPolicy == "filter")
                {
                    if (parentBroker != null)
                    {
                        parentBroker.AdvertiseSubscriber(topic, configuration.processAddress);
                    }
                }
            }).Start();
        }

        private void HandleFilterUpdate(string topic, string address)
        {
            SearchFilters(address).AddInterestTopic(topic);
            foreach(NeighborForwardingFilter filter in neighborFilters)
            {
                foreach(PublisherInfo info in filter.publishers)
                {
                    foreach(string t in info.topics)
                    {
                        if (t.Equals(topic))
                        {
                            if (!filter.neighborAddress.Equals(address))
                            {
                                RemoteBroker remoteBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), filter.neighborAddress);
                                remoteBroker.FilterUpdate(topic, configuration.processAddress);
                            }
                        }
                    }
                }
            }
        }

        private void HandlePublisherPublication(PublicationEvent e)
        {
            CreatePublisherInfo(e);
            switch (configuration.routingPolicy)
            {
                case "flooding":
                    EventRouting(e);
                    break;
                case "filter":
                    if (parentBroker != null)
                        parentBroker.AdvertisePublisher(e.topic, configuration.processAddress);
                    EventRouting(e);
                    break;
            }
        }

        private NeighborForwardingFilter SearchFilters(string address)
        {
            foreach (NeighborForwardingFilter filter in neighborFilters)
            {
                if (filter.neighborAddress.Equals(address))
                {
                    return filter;
                }
            }
            throw new NotImplementedException();
        }

        private void HandleSubAdvertisement(string topic, string address)
        {
            NeighborForwardingFilter filter = SearchFilters(address);
            filter.AddInterestTopic(topic);
            if (parentBroker != null)
            {
                parentBroker.AdvertiseSubscriber(topic, configuration.processAddress);
            }
        }

        private void HandleUnsubAdvertisement(string topic, string address)
        {
            lock (neighborFilters)
            {
                SearchFilters(address).interestedTopics.Remove(topic);
                foreach (NeighborForwardingFilter filter in neighborFilters)
                {
                    if (filter.MatchTopic(topic))
                    {
                        return;
                    }
                }
            }
            if (parentBroker != null)
            {
                parentBroker.AdvertiseUnsub(topic, configuration.processAddress);
            }
        }

        public void Unsubscription(string topic, string address)
        {
            lock (subscriptionsList)
            {
                SubscriptionInfo info = SearchSubscription(address);
                info.topics.Remove(topic);
            }
            if (configuration.routingPolicy.Equals("filter"))
            {
                foreach (SubscriptionInfo subInfo in subscriptionsList)
                {
                    foreach(string t in subInfo.topics)
                    {
                        if (t.Equals(topic))
                        {
                            return;
                        }
                    }
                }
                lock (neighborFilters)
                {
                    foreach (NeighborForwardingFilter filter in neighborFilters)
                    {
                        if (filter.MatchTopic(topic))
                        {
                            return;
                        }
                    }    
                }
                if(parentBroker != null)
                    parentBroker.AdvertiseUnsub(topic, configuration.processAddress);
            }       
        }

        public bool MatchFilters(string topic)
        {
            foreach(NeighborForwardingFilter filter in neighborFilters)
            {
                foreach(string t in filter.interestedTopics)
                {
                    if (t.Equals(topic))
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        public void HandlePubAdvertisement(string topic, string address)
        {
            NeighborForwardingFilter filter = SearchFilters(address);
            filter.publishers.Add(new PublisherInfo(topic, address));
            if (MatchFilters(topic))
            {
                new Thread(() =>
                {
                    RemoteBroker remoteBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), address);
                    remoteBroker.FilterUpdate(topic, configuration.processAddress);
                }).Start();
            }
            if (parentBroker != null)
            {
                parentBroker.AdvertisePublisher(topic, configuration.processAddress);
            }
        }

        public void SendEventLogWork(PublicationEvent e)
        {
            string logMessage = "[Broker - '" + name + "']----Got SESDAD Message Event!----" +
                                "'" + name + "'][From: '" + e.GetPublisher() + "'] Topic: " + e.topic +
                                "|| Message: " + e.eventMessage + "|| Sequence: " + e.GetSeqNumber().ToString() + Environment.NewLine;
            remoteSlave.SendLog(logMessage);
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
            msg += "||Filters: ";
            foreach(NeighborForwardingFilter filter in neighborFilters)
            {
                msg += filter.neighborAddress + "->";
                foreach(string topic in filter.interestedTopics)
                {
                    msg += ";" + topic;
                }
            }
            return msg;
        }

        private bool checkForward(NeighborForwardingFilter filter, PublicationEvent e)
        {
            foreach(string interestedTopic in filter.interestedTopics)
            {
                if(CheckTopicInterest(e, interestedTopic))
                {
                    return true;
                }
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
        public bool CheckTopicInterest(PublicationEvent e, string topic)
        {
            string[] eventArgs = e.topic.Split('/');
            string[] topicArgs = topic.Split('/');
            if (e.topic.Equals(topic))  // Event and Subscription topic are the same
            {
                return true;
            }
            if (topicArgs[topicArgs.Count() - 1].Equals('*')) // check if the subscriber has interest in the subtopics
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
    }

    public class NeighborForwardingFilter
    {
        public string neighborAddress;
        public List<string> interestedTopics = new List<string>();
        public List<PublisherInfo> publishers = new List<PublisherInfo>();

        public NeighborForwardingFilter(string neighborAddress)
        {
            this.neighborAddress = neighborAddress;
        }

        public void AddInterestTopic(string topic)
        {
            foreach(string localTopic in interestedTopics)
            {
                if (localTopic.Equals(topic))
                {
                    return;
                }
            }
            interestedTopics.Add(topic);
        }

        public bool MatchTopic(string topic)
        {
            foreach (string t in interestedTopics)
            {
                if (t.Equals(topic))
                {
                    return true;
                }
            }
            return false;
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
