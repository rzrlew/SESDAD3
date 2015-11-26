using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Threading;
using SESDAD;
using System.Collections;

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
        AutoResetEvent[] lockAdvertiseHandles = new AutoResetEvent[1];
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
            lockAdvertiseHandles[0] = new AutoResetEvent(true);
            remoteBroker.OnEventReceived = new NotifyEventDelegate(EventRouting);           
            remoteBroker.OnSubscribe += new PubSubEventDelegate(Subscription);
            remoteBroker.OnUnsubscribe += new PubSubEventDelegate(Unsubscription);
            remoteBroker.OnStatusRequest = new StatusRequestDelegate(SendStatus);
            remoteBroker.OnAdvertisePublisher = new PubAdvertisementEventDelegate(HandlePubAdvertisement);
            remoteBroker.OnAdvertiseSubscriber = new SubscriptionAdvertisementDelegate(HandleSubAdvertisement);
            remoteBroker.OnPublication = new PublicationRequestDelegate(HandlePublisherPublication);
            remoteBroker.OnAdvertiseUnsubscriber = new SubscriptionAdvertisementDelegate(HandleUnsubAdvertisement);
            remoteBroker.OnFilterUpdate = new PubSubEventDelegate(HandleFilterUpdate);
            remoteBroker.OnSequenceUpdate = new PubAdvertisementEventDelegate(HandleSeqUpdate);
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
        private void EventRouting(PublicationEvent e)
        {
            CreateLocalPublisherInfo(e);
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
                    new Thread(() => FIFOSubscriberWork(e.publisher)).Start();
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
        private void Subscription(string topic, string address)
        {
            lock (subscriptionsList)
            {
                try
                {
                    SubscriptionInfo info = SearchSubscription(address);
                    if (info.interestedTopics.Find(x => x.Equals(topic) ? true : false) == null)
                        info.interestedTopics.Add(topic);
                }
                catch (NotImplementedException)
                {
                    SubscriptionInfo info = new SubscriptionInfo(topic, address);
                    subscriptionsList.Add(info);
                }
            }

            if (configuration.routingPolicy == "filter")
            {
                lock (neighborFilters)
                {
                    foreach (NeighborForwardingFilter filter in neighborFilters)
                    {
                        if (filter.GetAllPublishersTopic(topic).Any() && !filter.neighborAddress.Equals(configuration.parentBrokerAddress))
                        {
                            RemoteBroker remoteBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), filter.neighborAddress);
                            remoteBroker.FilterUpdate(topic, configuration.processAddress);
                        }
                    }

                    if (parentBroker != null)
                    {
                        parentBroker.AdvertiseSubscriber(topic, configuration.processAddress, address);
                    }
                }
            }
        }
        private void Unsubscription(string topic, string address)
        {
                lock (subscriptionsList)
                {
                    SubscriptionInfo info = SearchSubscription(address);
                    info.interestedTopics.Remove(topic);
                    if (configuration.routingPolicy.Equals("filter"))
                    {
                        foreach (SubscriptionInfo subInfo in subscriptionsList)
                        {
                            foreach (string t in subInfo.interestedTopics)
                            {
                                if (t.Equals(topic) && !subInfo.subscription_address.Equals(address))//Checks if other subs have interest
                                {
                                    Console.WriteLine("Not propagating unsub found matching local subscriber!");
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
                                    Console.WriteLine("Not propagating unsub found matching filter interest!");
                                    return;
                                }
                            }
                        }
                        bool sendToparent = true;
                        foreach (NeighborForwardingFilter filter in neighborFilters)
                        {
                            foreach (PublisherInfo pubInfo in filter.GetAllPublishersTopic(topic)) // Chekcs if publisher for topic is through this link
                            {
                                if (!filter.neighborAddress.Equals(address))
                                {
                                    sendToparent = true;
                                    RemoteBroker remBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), filter.neighborAddress);
                                    Console.WriteLine("Propagating Unsub to " + filter.neighborAddress + " || local address:" + configuration.processAddress);
                                    remoteBroker.AdvertiseUnsub(topic, configuration.processAddress, address);
                                    break;
                                }
                            }
                        }
                        if (sendToparent && parentBroker != null)
                        {
                            parentBroker.AdvertiseUnsub(topic, configuration.processAddress, address);
                        }
                    }
                }
        }
        private void UpdateLocalPublisher(string publisherAddress, string topic, int seqNum)
        {
            try
            {
                PublisherInfo pubInfo = SearchLocalPublication(publisherAddress);
                pubInfo.LastSeqNumber = seqNum;
            }
            catch (NotImplementedException)
            {
                PublisherInfo pubInfo = new PublisherInfo(topic, publisherAddress);
                pubInfo.LastSeqNumber = seqNum;
                publicationList.Add(pubInfo);
            }
        }
        private string SendStatus()
        {
            string msg = "[Broker - " + this.name + "]";
            if (parentBroker != null)
            {
                msg += "Parent: " + this.parentBroker.name;
            }
            msg += " || Children: ";
            foreach (string childAddress in configuration.childrenBrokerAddresses)
            {
                msg += childAddress + " ; ";
            }
            msg += " || Filters: ";
            foreach (NeighborForwardingFilter filter in neighborFilters)
            {
                msg += filter.neighborAddress + "->";
                foreach (string topic in filter.GetAllInterestTopics())
                {
                    msg += topic + "; ";
                }
            }
            return msg;
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

        private PublisherInfo SearchLocalPublication(string address)
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
        private PublisherInfo SearchLocalPublicationTopic(string topic)
        {
            foreach (PublisherInfo info in publicationList)
            {
                foreach(string t in info.topics)
                {
                    if (t.Equals(topic))
                    {
                        return info;
                    }
                }
            }
            return null;
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
        private List<SubscriptionInfo> SearchSubscriptionsTopic(string topic)
        {
            List<SubscriptionInfo> subsList = new List<SubscriptionInfo>();
            foreach (SubscriptionInfo subInfo in subscriptionsList)
            {
                foreach (string subsTopic in subInfo.interestedTopics)
                {
                    if (subsTopic.Equals(topic))
                    {
                        subsList.Add(subInfo);
                    }
                }
            }
            return subsList;
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
            throw new NotImplementedException(configuration.processName + ": Filter for " + address + " not found!");
        }

        private void SendToSubscriberWork(PublicationEvent e)
        {
            lock (subscriptionsList)
            {
                if (subscriptionsList.Any())
                {
                    foreach (SubscriptionInfo info in subscriptionsList)
                    {
                        foreach (string topic in info.interestedTopics)
                        {
                            if (CheckTopicInterest(e.topic, topic))
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
            {
                PublisherInfo info = SearchLocalPublication(publicationAddress);
                lock (FIFOWaitQueue)
                {
                    foreach (PublicationEvent e in FIFOWaitQueue)
                    {
                        lock (info)
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
        private void FilterWork(PublicationEvent e)
        {
            lock (FIFOWaitQueue)
            {
                string lastHopAddress = e.lastHop;
                e.lastHop = configuration.processAddress;
                foreach (NeighborForwardingFilter filter in neighborFilters)
                {
                    if (filter.MatchTopic(e.topic) && !filter.neighborAddress.Equals(lastHopAddress))
                    {
                        RemoteBroker remoteBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), filter.neighborAddress);
                        remoteBroker.RouteEvent(e);
                    }
                    else if (!filter.neighborAddress.Equals(lastHopAddress))
                    {

                        RemoteBroker remoteBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), filter.neighborAddress);
                        remoteBroker.SequenceUpdate(e.topic, configuration.processAddress, e.publisher, e.SequenceNumber);
                    }
                }
            }
        }
        public void SendEventLogWork(PublicationEvent e)
        {
            string logMessage = "[Broker - '" + name + "']Message: " +
                                "From: '" + e.GetPublisher() + "' || Topic: " + e.topic +
                                "|| Message: " + e.eventMessage + "|| Sequence: " + e.GetSeqNumber().ToString();
            remoteSlave.SendLog(logMessage);
        }

        private void HandleFilterUpdate(string topic, string lastHopAddress)
        {
            Console.WriteLine("Received FilterUpdate message from " + lastHopAddress);
            SearchFilters(lastHopAddress).AddInterestTopic(topic)/*, subscriberAddress)*/;
            foreach (NeighborForwardingFilter filter in neighborFilters)
            {
                foreach (PublisherInfo info in filter.publishers)
                {
                    foreach (string t in info.topics)
                    {
                        if (t.Equals(topic) && !filter.neighborAddress.Equals(lastHopAddress))
                        {
                            RemoteBroker remoteBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), filter.neighborAddress);
                            remoteBroker.FilterUpdate(topic, configuration.processAddress);
                        }
                    }
                }
            }
        }
        private void HandlePublisherPublication(PublicationEvent e)
        {
            bool isNewTopic = CreateLocalPublisherInfo(e);
            switch (configuration.routingPolicy)
            {
                case "flooding":
                    EventRouting(e);
                    break;
                case "filter":
                    if (parentBroker != null && isNewTopic)
                    {
                        Console.WriteLine("New publication topic detected! Sending advertisement for topic" + e.topic + "!");
                        parentBroker.AdvertisePublisher(e.topic, configuration.processAddress, e.publisher, e.SequenceNumber);
                    }
                    EventRouting(e);
                    break;
            }
        }
        private void HandleSubAdvertisement(string topic, string lastHopAddress, string subscriberAddress)
        {
            lock (neighborFilters)
            {
                //Received Subscriber interest in topic! Add topic to filter!
                NeighborForwardingFilter filter = SearchFilters(lastHopAddress);
                filter.AddInterestTopic(topic)/*, subscriberAddress)*/;

                List<PublisherInfo> sentPublishers = new List<PublisherInfo>();
                foreach (NeighborForwardingFilter neighborFilter in neighborFilters)
                {
                    List<PublisherInfo> publishersTopic = neighborFilter.GetAllPublishersTopic(topic); //All publishers that publish topic
                    if (publishersTopic.Any() && !neighborFilter.neighborAddress.Equals(lastHopAddress)) // If publishers for this topic are found in any link!
                    {
                        //Send sequence update for all publishers of this topic!
                        RemoteBroker lastHopBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), lastHopAddress);
                        foreach (PublisherInfo pubInfo in publishersTopic)
                        {
                            if (!sentPublishers.Contains(pubInfo))
                            {
                                lastHopBroker.SequenceUpdate(topic, configuration.processAddress, pubInfo.publication_address, pubInfo.LastSeqNumber);
                                sentPublishers.Add(pubInfo);
                            }
                        }
                        //Send filter update to neighbor that knows publisher
                        RemoteBroker filterBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), neighborFilter.neighborAddress);
                        filterBroker.FilterUpdate(topic, configuration.processAddress);
                    }
                }
            }
            if (parentBroker != null)
            {
                parentBroker.AdvertiseSubscriber(topic, configuration.processAddress, subscriberAddress);
            }
        }
        private void HandleSeqUpdate(string topic, string lastHopAddress, string publisherAddress, int seqNum)
        {
            lock (FIFOWaitQueue)
            {
                NeighborForwardingFilter upStreamFilter = SearchFilters(lastHopAddress);
                UpdateLocalPublisher(publisherAddress, topic, seqNum);
                PublisherInfo pubInfo = upStreamFilter.AddPublisher(topic, publisherAddress, seqNum);
                foreach (NeighborForwardingFilter filter in neighborFilters)
                {
                    if (!filter.MatchTopic(topic) && !filter.neighborAddress.Equals(lastHopAddress)) // still has a link with interest except for the last hop
                    {
                        foreach (string t in filter.GetAllInterestTopics())
                        {
                            if (pubInfo.MatchTopic(t))
                            {
                                RemoteBroker filterBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), filter.neighborAddress);
                                filterBroker.SequenceUpdate(topic, configuration.processAddress, publisherAddress, seqNum);
                            }
                        }
                    }
                }
            }
        }
        private void HandleUnsubAdvertisement(string topic, string lastHopAddress, string subscriberAddress)
        {
            Console.WriteLine("Got unsub advertisement from " + lastHopAddress);
            lock (subscriptionsList)
            {
                lock (neighborFilters)
                {
                    //Received unsub advertisement! Remove subscription topic from filter
                    SearchFilters(lastHopAddress).RemoveSubscriberTopic(topic);
                    //Send to parent if parent is not last hop!
                    if (parentBroker != null && !lastHopAddress.Equals(configuration.parentBrokerAddress))
                    {
                        parentBroker.AdvertiseUnsub(topic, configuration.processAddress, subscriberAddress);
                    }
                    foreach (SubscriptionInfo subInfo in subscriptionsList)
                    {
                        foreach (string t in subInfo.interestedTopics)
                        {
                            //Checks if other local subs have interest
                            if (t.Equals(topic) && !subInfo.subscription_address.Equals(lastHopAddress))
                            {
                                return;
                            }
                        }
                    }
                    // Checks if other brokers except lastHop have interest in topic
                    foreach (NeighborForwardingFilter filter in neighborFilters) 
                    {
                        if (filter.MatchTopic(topic) && !filter.neighborAddress.Equals(lastHopAddress))
                        {
                            return;
                        }
                    }

                    //If there is are no others with interest! Propagate Unsub to 
                    foreach (NeighborForwardingFilter filter in neighborFilters)
                    {
                        foreach (PublisherInfo pubInfo in filter.GetAllPublishersTopic(topic)) // Chekcs if publisher for topic is through this link
                        {
                            if (!filter.neighborAddress.Equals(lastHopAddress) && !filter.neighborAddress.Equals(configuration.parentBrokerAddress))
                            {
                                RemoteBroker filterBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), filter.neighborAddress);
                                filterBroker.AdvertiseUnsub(topic, configuration.processAddress, subscriberAddress);
                                break;
                            }
                        }
                    }
                }
            }

        }
        private void HandlePubAdvertisement(string topic, string lastHopAddress, string publisherAddress, int seqNum)
        {
            lock (FIFOWaitQueue)
            {
                //Received Publisher advertisement! Add publisher to filter
                NeighborForwardingFilter filter = SearchFilters(lastHopAddress);
                filter.AddPublisher(topic, publisherAddress, seqNum);
                //If there are local subscriptions for topic create/update local publication info
                if (SearchSubscriptionsTopic(topic) != null)
                {
                    PublisherInfo pubInfo;
                    try
                    {
                        pubInfo = SearchLocalPublication(publisherAddress);
                        if (pubInfo.topics.Find(x => x.Equals(topic) ? true : false) == null)
                        {
                            pubInfo.topics.Add(topic);
                        }
                    }
                    catch (NotImplementedException)
                    {
                        pubInfo = new PublisherInfo(topic, publisherAddress);
                        publicationList.Add(pubInfo);
                    }
                    pubInfo.LastSeqNumber = seqNum;
                }

                if (MatchFilters(topic, lastHopAddress) || (SearchSubscriptionsTopic(topic).Any())) //If any show interest...
                {
                    Console.WriteLine("Interested in topic from publisher! sending update filter message to " + lastHopAddress);
                    //Send Reverse Filter Update
                    RemoteBroker lastHopBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), lastHopAddress);
                    lastHopBroker.FilterUpdate(topic, configuration.processAddress);
                    //Check each neighbor filter other than last hop for interest in topic
                    foreach (NeighborForwardingFilter filter1 in neighborFilters)
                    {
                        if (!filter1.neighborAddress.Equals(lastHopAddress) && filter1.MatchTopic(topic))
                        {
                            //Send Sequence update to filter broker
                            RemoteBroker filterBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), lastHopAddress);
                            filterBroker.SequenceUpdate(topic, configuration.processAddress, publisherAddress, seqNum);

                        }
                    }
                }
                if (parentBroker != null)
                {
                    parentBroker.AdvertisePublisher(topic, configuration.processAddress, publisherAddress, seqNum);
                }
            }
        }

        public bool CheckTopicInterest(string eventTopic, string topic)
        {
            string[] eventArgs = eventTopic.Split('/');
            string[] topicArgs = topic.Split('/');
            if (eventTopic.Equals(topic))  // Event and Subscription topic are the same
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
        private bool MatchFilters(string topic, string lastHopAddress)
        {
            foreach (NeighborForwardingFilter filter in neighborFilters)
            {
                Console.WriteLine("Search topic: " + topic);
                if (filter.MatchTopic(topic) && !filter.neighborAddress.Equals(lastHopAddress))
                {
                    Console.WriteLine("Filters Match!");
                    return true;
                }
            }
            return false;
        }
        private bool CreateLocalPublisherInfo(PublicationEvent e)
        {
            lock (publicationList)
            {
                try
                {
                    PublisherInfo info = SearchLocalPublication(e.publisher);
                    if (!info.topics.Contains(e.topic))
                    {
                        info.topics.Add(e.topic);
                        return true;
                    }
                    return false;
                }
                catch (NotImplementedException)
                {
                    PublisherInfo info = new PublisherInfo(e.topic, e.publisher);
                    publicationList.Add(info);
                    return true;
                }
            }
        }
    }

    public class NeighborForwardingFilter
    {
        public string neighborAddress;
        public IDictionary<string, int> interestTopics = new Dictionary<string, int>();//Key->topic ; Value->NumSubscribers
        public List<PublisherInfo> publishers = new List<PublisherInfo>();

        public NeighborForwardingFilter(string neighborAddress)
        {
            this.neighborAddress = neighborAddress;
        }

        public void AddInterestTopic(string topic)/*, string subscriberAddress)*/
        {

            if (interestTopics.ContainsKey(topic))
            {
                interestTopics[topic]++;
            }
            else
            {
                interestTopics.Add(new KeyValuePair<string, int>(topic, 1));
            }
            //SubscriptionInfo subInfo;
            //try {
            //     subInfo = SearchSubscription(subscriberAddress);
            //}
            //catch (NotImplementedException)
            //{
            //    subInfo = new SubscriptionInfo(topic, subscriberAddress);
            //    subscribers.Add(subInfo);
            //}
            //foreach (string t in subInfo.interestedTopics)
            //{
            //    if (t.Equals(topic))
            //    {
            //        return;
            //    }
            //}
            //subInfo.interestedTopics.Add(topic);
        }

        public void RemoveSubscriberTopic(string topic)
        {
            interestTopics[topic]--;
            if (interestTopics[topic] <= 0)
            {
                interestTopics.Remove(topic);
            }
        }

        public List<string> GetAllInterestTopics()
        {
            List<string> listTopics = new List<string>();

            foreach (KeyValuePair<string, int> entry in interestTopics)
            {
                listTopics.Add(entry.Key);
            }
            //foreach (SubscriptionInfo subInfo in subscribers)
            //{
            //    listTopics.Concat(subInfo.interestedTopics);
            //}
            return listTopics;
        }

        //public List<SubscriptionInfo> GetAllSubscriberTopic(string topic)
        //{
        //    List<SubscriptionInfo> subsList = new List<SubscriptionInfo>();
        //    foreach(SubscriptionInfo subInfo in subscribers)
        //    {
        //        foreach (string t in subInfo.interestedTopics)
        //        {
        //            if (t.Equals(topic))
        //            {
        //                subsList.Add(subInfo);
        //                break;
        //            }
        //        }
        //    }
        //    return subsList;
        //}

        public PublisherInfo AddPublisher(string topic, string publisherAddress, int seqNum)
        {
            foreach(PublisherInfo pubInfo in publishers)
            {
                if (pubInfo.publication_address.Equals(publisherAddress))
                {
                    bool noLuck = false;
                    foreach(string t in pubInfo.topics)
                    {
                        if (t.Equals(topic))
                        {
                            noLuck = true;
                            break;
                        }
                    }
                    pubInfo.LastSeqNumber = seqNum;
                    if (noLuck)
                        return pubInfo;
                    pubInfo.topics.Add(topic);
                    return pubInfo;
                }
            }
            PublisherInfo newPub = new PublisherInfo(topic, publisherAddress);
            newPub.LastSeqNumber = seqNum;
            publishers.Add(newPub);
            return newPub;
        }
        public PublisherInfo GetPublisher(string publisherAddress)
        {
            foreach(PublisherInfo pubInfo in publishers)
            {
                if (pubInfo.publication_address.Equals(publisherAddress))
                {
                    return pubInfo;
                }
            }
            return null;
        }

        public List<PublisherInfo> GetAllPublishersTopic(string topic)
        {
            List<PublisherInfo> listPublishers = new List<PublisherInfo>();
            foreach (PublisherInfo pubInfo in publishers)
            {
                foreach (string t in pubInfo.topics)
                {
                    if (t.Equals(topic))
                    {
                        listPublishers.Add(pubInfo);
                    }
                }
            }
            return listPublishers;
        }

        //public SubscriptionInfo SearchSubscription(string subAddress)
        //{



        //    foreach (SubscriptionInfo subInfo in subscribers)
        //    {
        //        if (subInfo.subscription_address.Equals(subAddress))
        //        {
        //            return subInfo;
        //        }
        //    }
        //    throw new NotImplementedException("Subscriber " + subAddress + " not found on filter for " + neighborAddress);
        //}

        public bool MatchTopic(string topic)
        {
            foreach (KeyValuePair<string, int> entry in interestTopics)
            {
                if (topic.Equals(entry.Key)){
                    return true;
                }
            }
            return false;

            //foreach (SubscriptionInfo subInfo in subscribers)
            //{
            //    foreach (string t in subInfo.interestedTopics)
            //    {
            //        if (t.Equals(topic))
            //        {
            //            return true;
            //        }
            //    }
            //}
            //return false;
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

        public bool MatchTopic(string topic)
        {
            foreach(string t in topics)
            {
                if (t.Equals(topic))
                {
                    return true;
                }
            }
            return false;
        }
    }

    public class SubscriptionInfo
    {
        public string subscription_address;
        public List<string> interestedTopics = new List<string>();

        public SubscriptionInfo(string topic, string address)
        {
            interestedTopics.Add(topic);
            subscription_address = address;
        }
    }
}
