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
        RemoteBroker filterBroker;
        RemoteBroker parentBroker = null;
        List<RemoteBroker> childBrokers = new List<RemoteBroker>();
        List<SubscriptionInfo> subscriptionsList = new List<SubscriptionInfo>();// stores information regarding subscriptions[subscriber][topic]  
        List<PublisherInfo> publicationList = new List<PublisherInfo>();

        /// <summary>
        /// Does not contain updated topics or sequence number!!!!
        /// </summary>
        List<PublisherInfo> localPublisherFIFO = new List<PublisherInfo>();

        List<PublicationEvent> FIFOWaitQueue = new List<PublicationEvent>();
        List<NeighborForwardingFilter> neighborFilters = new List<NeighborForwardingFilter>();
        AutoResetEvent[] lockAdvertiseHandles = new AutoResetEvent[1];
        public string Name
        {
            set
            {
                name = value;
                filterBroker.name = name;
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
            filterBroker = new RemoteBroker(slaveAddress);
            lockAdvertiseHandles[0] = new AutoResetEvent(true);
            filterBroker.OnEventReceived = new NotifyEventDelegate(EventRouting);           
            filterBroker.OnSubscribe += new PubSubEventDelegate(Subscription);
            filterBroker.OnUnsubscribe += new PubSubEventDelegate(Unsubscription);
            filterBroker.OnStatusRequest = new StatusRequestDelegate(SendStatus);
            filterBroker.OnAdvertisePublisher = new PubAdvertisementEventDelegate(HandlePubAdvertisement);
            filterBroker.OnAdvertiseSubscriber = new SubscriptionAdvertisementDelegate(HandleSubAdvertisement);
            filterBroker.OnPublication = new PublicationRequestDelegate(HandlePublisherPublication);
            filterBroker.OnAdvertiseUnsubscriber = new SubscriptionAdvertisementDelegate(HandleUnsubAdvertisement);
            filterBroker.OnFilterUpdate = new UpdateFilterEventDelegate(HandleFilterUpdate);
            filterBroker.OnSequenceUpdate = new PubAdvertisementEventDelegate(HandleSeqUpdate);
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
            RemotingServices.Marshal(filterBroker, new Uri(configuration.processAddress).LocalPath.Split('/')[1]);
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
            if (configuration.loggingLevel != null && configuration.loggingLevel.Equals("full")){
                new Thread(() => SendEventLogWork(e)).Start();
            }
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
            lock (FIFOWaitQueue)
            {
                lock (subscriptionsList)
                {
                    Console.WriteLine("Got local subscription on topic " + topic);
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

                    if (configuration.routingPolicy == "filter")
                    {
                        foreach (NeighborForwardingFilter filter in neighborFilters)
                        {
                            //If there are publishers for this topic and link is NOT to parent
                            if (filter.GetAllPublishersTopic(topic).Any() && !filter.neighborAddress.Equals(configuration.parentBrokerAddress))
                            {
                                //Send Filter Update in Publishers direction!
                                Console.WriteLine("Sending Filter Update to " + filter.neighborAddress + " for topic:" + topic);
                                RemoteBroker remoteBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), filter.neighborAddress);
                                remoteBroker.FilterUpdate(topic, configuration.processAddress, true);
                            }
                        }

                        //Send Subscription advertisement to parent!
                        if (parentBroker != null)
                        {
                            Console.WriteLine("Advertising subscription to parent!");
                            parentBroker.AdvertiseSubscriber(topic, configuration.processAddress, address);
                            Console.WriteLine("Parent Confirmed!");
                        }
                    }
                    Console.WriteLine("Subscription on topic '" + topic + "' is confirmed!");
                }
            }
        }
        private void Unsubscription(string topic, string address)
        {
            lock (FIFOWaitQueue)
            {
                lock (subscriptionsList)
                {
                    Console.WriteLine("Got local unsubscription for topic: " + topic);
                    SubscriptionInfo info = SearchSubscription(address);
                    info.interestedTopics.Remove(topic);
                    if (configuration.routingPolicy.Equals("filter"))
                    {
                        //foreach (SubscriptionInfo subInfo in subscriptionsList)
                        //{
                        //    foreach (string t in subInfo.interestedTopics)
                        //    {
                        //        //Checks if other subs have interest
                        //        if (t.Equals(topic) && !subInfo.subscription_address.Equals(address))
                        //        {
                        //            Console.WriteLine("Not propagating unsub found matching local subscriber!");
                        //            return;
                        //        }
                        //    }
                        //}
                        //lock (neighborFilters)
                        //{
                        //    foreach (NeighborForwardingFilter filter in neighborFilters)
                        //    {
                        //        if (filter.MatchTopic(topic))
                        //        {
                        //            Console.WriteLine("Not propagating unsub found matching filter interest!");
                        //            return;
                        //        }
                        //    }
                        //}
                        bool sendToparent = true;
                        foreach (NeighborForwardingFilter filter in neighborFilters)
                        {
                            // Chekcs if publisher for topic is through this link
                            foreach (PublisherInfo pubInfo in filter.GetAllPublishersTopic(topic))
                            {
                                if (!filter.neighborAddress.Equals(address))
                                {
                                    if (filter.neighborAddress.Equals(configuration.parentBrokerAddress))
                                    {
                                        sendToparent = false;
                                    }
                                    RemoteBroker filterBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), filter.neighborAddress);
                                    Console.WriteLine("Propagating Unsub to " + filter.neighborAddress + " || local address:" + configuration.processAddress);
                                    filterBroker.AdvertiseUnsub(topic, configuration.processAddress, address);
                                    break;
                                }
                            }
                        }
                        if (sendToparent && parentBroker != null)
                        {
                            parentBroker.AdvertiseUnsub(topic, configuration.processAddress, address);
                        }
                    }
                    Console.WriteLine("Unsubscription on topic '" + topic + "' is confirmed!");
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
            string msg = "[Status - " + this.name + "]";
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
            lock (FIFOWaitQueue)
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
                                    //Console.WriteLine("Sent message:" + e.Message() + " || to susbcriber at:" + info.subscription_address);
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
                PublisherInfo messagePublisher = SearchLocalPublication(e.publisher);
                foreach (NeighborForwardingFilter filter in neighborFilters)
                {
                    //bool isSent = false;
                    //foreach (KeyValuePair<string, int> interestTopic in filter.interestTopics)
                    //{
                    //    if (CheckTopicInterest(e.topic, interestTopic.Key))
                    //    {
                    //        RemoteBroker remoteBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), filter.neighborAddress);
                    //        remoteBroker.RouteEvent(e);
                    //        isSent = true;
                    //        break;
                    //    }
                    //}
                    //if (!isSent)
                    //{
                    //    foreach (string publisherTopic in messagePublisher.topics)
                    //    {
                    //        foreach (KeyValuePair<string, int> interestTopic in filter.interestTopics)
                    //        {
                    //            if (CheckTopicInterest(interestTopic.Key, publisherTopic) && !CheckTopicInterest(e.topic, publisherTopic))
                    //            {
                    //                Console.WriteLine("Filter Work sending non requested topic update! Found topic " + publisherTopic + " that needs seq number! Sending to " + filter.neighborAddress);
                    //                RemoteBroker filterBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), filter.neighborAddress);
                    //                //filterBroker.SequenceUpdate(publisherTopic, configuration.processAddress, e.publisher, e.SequenceNumber);
                    //                filterBroker.RouteEvent(e);
                    //                break;
                    //            }

                    //        }
                    //    }
                    //}
                    
                    if (filter.MatchTopic(e.topic) && !filter.neighborAddress.Equals(lastHopAddress))
                    {
                        RemoteBroker remoteBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), filter.neighborAddress);
                        remoteBroker.RouteEvent(e);
                    }
                    else if (!filter.neighborAddress.Equals(lastHopAddress))
                    {
                        foreach (string publisherTopic in messagePublisher.topics)
                        {
                            if (filter.MatchTopic(publisherTopic) && !publisherTopic.Equals(e.topic))
                            {
                                Console.WriteLine("Filter Work sending non requested topic update! Found topic " + publisherTopic + " that needs seq number! Sending to " + filter.neighborAddress);
                                RemoteBroker filterBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), filter.neighborAddress);
                                //filterBroker.SequenceUpdate(publisherTopic, configuration.processAddress, e.publisher, e.SequenceNumber);
                                filterBroker.RouteEvent(e);
                                break;
                            }
                        }
                    }
                }
            }
        }
        public void SendEventLogWork(PublicationEvent e)
        {
            string logMessage = "[Routing - '" + name + "']Message: " +
                                "From: '" + e.GetPublisher() + "' || Topic: " + e.topic +
                                "|| Message: " + e.eventMessage + "|| Sequence: " + e.GetSeqNumber().ToString();
            remoteSlave.SendLog(logMessage);
        }

        private void HandleFilterUpdate(string topic, string lastHopAddress, bool toAdd)
        {
            new Thread(() =>
            {
                if (toAdd)
                {
                    lock (neighborFilters)
                    {
                        Console.WriteLine("Received FilterUpdate message from " + lastHopAddress);
                        //If new topic being added!
                        NeighborForwardingFilter lastHopFilter = SearchFilters(lastHopAddress);
                        if (SearchFilters(lastHopAddress).AddInterestTopic(topic))
                        {/*, subscriberAddress)*/

                            foreach (PublisherInfo pubInfo in publicationList)
                            {
                                //if publisher publishes topic and is directly connected (local).
                                if (pubInfo.topics.Find(x => x.Equals(topic)) != null && localPublisherFIFO.Find(x => x.publication_address.Equals(pubInfo.publication_address)) != null)
                                {
                                    bool doUpdate = true;
                                    //For each publisher topic check if its already in link's interest, if yes then the sequence updates for this publisher are already sent!
                                    foreach (string publisherTopic in pubInfo.topics)
                                    {
                                        if (!publisherTopic.Equals(topic) && lastHopFilter.MatchTopic(publisherTopic))
                                        {
                                            doUpdate = false;
                                            break;
                                        }
                                    }
                                    if (doUpdate)
                                    {
                                        Console.WriteLine("Sending seq update to " + lastHopAddress + " for publisher at " + pubInfo.publication_address);
                                        RemoteBroker lastHopBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), lastHopAddress);
                                        lastHopBroker.SequenceUpdate(topic, configuration.processAddress, pubInfo.publication_address, pubInfo.LastSeqNumber);
                                        Console.WriteLine("Sequence update confirmed!");
                                    }
                                }
                            }

                            //localPublisherFIFO contains all directly connected publihsers. Reverse Send SequenceUpdate for all that publish topic.
                            //foreach (PublisherInfo localPubInfo in localPublisherFIFO)
                            //{
                            //    //If topic is found on locally connected publisher! (localPubInfo does not contain updated topics!)
                            //    if (publicationList.Find(x => x.publication_address.Equals(localPubInfo.publication_address)).topics.Find(x => x.Equals(topic)) != null)
                            //    {
                            //        Console.WriteLine("Found localy connected publisher sending seq message to " + lastHopAddress);
                            //        RemoteBroker lastHopBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), lastHopAddress);
                            //        lastHopBroker.SequenceUpdate(topic, configuration.processAddress, localPubInfo.publication_address, /* localPubInfo does not contain updated seqNum, find it in publication list */
                            //                                        publicationList.Find(x => x.publication_address.Equals(localPubInfo.publication_address)).LastSeqNumber);
                            //        Console.WriteLine("Sequence Update confirmed!");
                            //    }
                            //}
                        }
                        //Send filter update in the direction of all publishers for topic except last hop and parent!
                        foreach (NeighborForwardingFilter filter in neighborFilters)
                        {
                            bool sendFilterUpdate = false;
                            foreach (PublisherInfo info in filter.publishers)
                            {
                                foreach (string t in info.topics)
                                {
                                    if (t.Equals(topic) && !filter.neighborAddress.Equals(lastHopAddress) && !filter.neighborAddress.Equals(configuration.parentBrokerAddress))
                                    {
                                        sendFilterUpdate = true;
                                    }
                                }
                            }
                            if (sendFilterUpdate)
                            {
                                Console.WriteLine("Sending filter update message to " + filter.neighborAddress);
                                RemoteBroker filterBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), filter.neighborAddress);
                                filterBroker.FilterUpdate(topic, configuration.processAddress, true);
                            }
                        }
                        Console.WriteLine("Filter update is confirmed!");
                    }
                }
                //else
                //{
                //    lock (neighborFilters)
                //    {
                //        Console.WriteLine("Received Remove FilterUpdate message from " + lastHopAddress);
                //        SearchFilters(lastHopAddress).RemoveSubscriberTopic(topic);
                //        foreach (NeighborForwardingFilter filter in neighborFilters)
                //        {
                //            foreach (PublisherInfo pubInfo in filter.GetAllPublishersTopic(topic)) // Chekcs if publisher for topic is through this link
                //            {
                //                if (!filter.neighborAddress.Equals(lastHopAddress) && !filter.neighborAddress.Equals(configuration.parentBrokerAddress))
                //                {
                //                    Console.WriteLine("Sending filter remove update " + filter.neighborAddress + " for topic: " + topic);
                //                    RemoteBroker filterBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), filter.neighborAddress);
                //                    filterBroker.AdvertiseUnsub(topic, configuration.processAddress, subscriberAddress);
                //                    //filterBroker.FilterUpdate(topic, configuration.processAddress, false);
                //                    Console.WriteLine("Fitler remove update to " + filter.neighborAddress + " for topic: " + topic + ", confimed!");
                //                    break;
                //                }
                //            }
                //        }
                //        Console.WriteLine("Remove Filter update is confirmed!");
                //    }
                //}
            }).Start();
        }
        private void HandlePublisherPublication(PublicationEvent e)
        {
            lock (FIFOWaitQueue)
            {
                bool isNewTopic = CreateLocalPublisherInfo(e);
                switch (configuration.routingPolicy)
                {
                    case "flooding":
                        EventRouting(e);
                        break;
                    case "filter":
                        if (isNewTopic)
                        {
                            PublisherInfo messagePublisher = SearchLocalPublication(e.publisher);
                            Console.WriteLine("New publication topic detected! Sending advertisement for topic" + e.topic + "!");
                            if (localPublisherFIFO.Find(x => x.publication_address.Equals(e.publisher)) == null)
                            {
                                localPublisherFIFO.Add(new PublisherInfo(e.topic, e.publisher));
                            }
                            foreach (NeighborForwardingFilter filter in neighborFilters)
                            {
                                if (filter.MatchTopic(e.topic))
                                {
                                    RemoteBroker filterBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), filter.neighborAddress);
                                    filterBroker.SequenceUpdate(e.topic, configuration.processAddress, e.publisher, e.SequenceNumber);
                                }
                                else
                                {
                                    foreach (string publisherTopic in messagePublisher.topics)
                                        if (filter.MatchTopic(publisherTopic) && !publisherTopic.Equals(e.topic))
                                        {
                                            Console.WriteLine("Found topic " + publisherTopic + " that needs seq number! Sending to " + filter.neighborAddress);
                                            RemoteBroker filterBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), filter.neighborAddress);
                                            filterBroker.SequenceUpdate(publisherTopic, configuration.processAddress, e.publisher, e.SequenceNumber);
                                            break;
                                        }
                                }
                            }
                            if (parentBroker != null)
                                parentBroker.AdvertisePublisher(e.topic, configuration.processAddress, e.publisher, e.SequenceNumber);
                        }
                        EventRouting(e);
                        break;
                }
            }
        }
        private void HandleSubAdvertisement(string topic, string lastHopAddress, string subscriberAddress)
        {
            lock (FIFOWaitQueue)
            {
                Console.WriteLine("Got Sub advertisement from " + lastHopAddress + " on topic:" + topic);
                //Received Subscriber interest in topic! Add topic to filter!
                NeighborForwardingFilter lastHopFilter = SearchFilters(lastHopAddress);

                //If NEW topic being added to filter for last hop!
                if (lastHopFilter.AddInterestTopic(topic)/*, subscriberAddress)*/)
                {
                    //Send sequence update to last Hop for all publishers of this topic that are not already being sent!
                    RemoteBroker lastHopBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), lastHopAddress);
                    foreach (PublisherInfo pubInfo in publicationList)
                    {
                        //if publisher publishes topic ###and is directly connected (local).
                        if (pubInfo.topics.Find(x => x.Equals(topic)) != null) //&& localPublisherFIFO.Find(x => x.publication_address.Equals(pubInfo.publication_address)) != null)
                        {
                            bool doUpdate = true;
                            //For each publisher topic check if its already in link's interest, if yes then the sequence updates for this publisher are already sent!
                            foreach (string publisherTopic in pubInfo.topics)
                            {
                                if (!publisherTopic.Equals(topic) && lastHopFilter.MatchTopic(publisherTopic))
                                {
                                    doUpdate = false;
                                    break;
                                }
                            }
                            if (doUpdate)
                            {
                                Console.WriteLine("Sending seq update to " + lastHopAddress + " for publisher at " + pubInfo.publication_address);
                                lastHopBroker.SequenceUpdate(topic, configuration.processAddress, pubInfo.publication_address, pubInfo.LastSeqNumber);
                                Console.WriteLine("Sequence update confirmed!");
                            }
                        }
                    }
                }
                foreach (NeighborForwardingFilter neighborFilter in neighborFilters)
                {
                    List<PublisherInfo> publishersTopic = neighborFilter.GetAllPublishersTopic(topic); //All publishers that publish topic
                    // If publishers for this topic are found in link except for link with parent and lastHop!
                    if (publishersTopic.Any() && !neighborFilter.neighborAddress.Equals(lastHopAddress) && !neighborFilter.neighborAddress.Equals(configuration.parentBrokerAddress))
                    {
                        //Send filter update to neighbor that knows publisher for topic except parent broker
                        RemoteBroker filterBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), neighborFilter.neighborAddress);
                        filterBroker.FilterUpdate(topic, configuration.processAddress, true);
                    }
                }
                if (parentBroker != null)
                {
                    parentBroker.AdvertiseSubscriber(topic, configuration.processAddress, subscriberAddress);
                }
                Console.WriteLine("Sub Advertisement for topic " + topic + " confirmed!");
            }
        }
        private void HandleSeqUpdate(string topic, string lastHopAddress, string publisherAddress, int seqNum)
        {
            new Thread(() =>
            {
                lock (neighborFilters)
                {
                    Console.WriteLine("Received Seq Update from " + lastHopAddress + " for publisher at " + publisherAddress);
                    //Update sequence Number on filter and local list of publishers.
                    NeighborForwardingFilter upStreamFilter = SearchFilters(lastHopAddress);
                    UpdateLocalPublisher(publisherAddress, topic, seqNum);
                    PublisherInfo pubInfo = upStreamFilter.AddPublisher(topic, publisherAddress, seqNum);

                    //Send sequence update to all interested in topics for this publisher!
                    foreach (NeighborForwardingFilter filter in neighborFilters)
                    {
                        foreach (string publisherTopic in pubInfo.topics)
                        {
                            // If any link, except for last hop, has interest in topic, send seq update for this publisher topic.
                            if (filter.MatchTopic(publisherTopic) && !filter.neighborAddress.Equals(lastHopAddress))
                            {
                                //foreach (string t in filter.GetAllInterestTopics())
                                //{
                                //    if (pubInfo.MatchTopic(t))
                                //    {
                                Console.WriteLine("Sending seq update to " + filter.neighborAddress);
                                RemoteBroker filterBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), filter.neighborAddress);
                                filterBroker.SequenceUpdate(topic, configuration.processAddress, publisherAddress, seqNum);
                                //    }
                                //}
                            }
                        }
                    }
                    Console.WriteLine("Seq Update Confirmed!");
                }
            }).Start();
        }
        private void HandleUnsubAdvertisement(string topic, string lastHopAddress, string subscriberAddress)
        {
            Console.WriteLine("Got unsub advertisement from " + lastHopAddress);
            lock (FIFOWaitQueue)
            {
                //Received unsub advertisement! Remove subscription topic from filter
                SearchFilters(lastHopAddress).RemoveSubscriberTopic(topic);
                //Send to parent if parent is not last hop!
                if (parentBroker != null && !lastHopAddress.Equals(configuration.parentBrokerAddress))
                {
                    parentBroker.AdvertiseUnsub(topic, configuration.processAddress, subscriberAddress);
                }
                //foreach (SubscriptionInfo subInfo in subscriptionsList)
                //{
                //    foreach (string t in subInfo.interestedTopics)
                //    {
                //        //Checks if other local subs have interest
                //        if (t.Equals(topic) && !subInfo.subscription_address.Equals(lastHopAddress))
                //        {
                //            return;
                //        }
                //    }
                //}
                // Checks if other brokers except lastHop have interest in topic
                //foreach (NeighborForwardingFilter filter in neighborFilters)
                //{
                //    if (filter.MatchTopic(topic) && !filter.neighborAddress.Equals(lastHopAddress))
                //    {
                //        return;
                //    }
                //}

                //Propagate Unsub to links with publishers for topic
                new Thread(() =>
                {
                    lock (FIFOWaitQueue)
                    {
                        foreach (NeighborForwardingFilter filter in neighborFilters)
                        {
                            foreach (PublisherInfo pubInfo in filter.GetAllPublishersTopic(topic)) // Chekcs if publisher for topic is through this link
                            {
                                Console.WriteLine("Found publisher through " + filter.neighborAddress);
                                if (!filter.neighborAddress.Equals(lastHopAddress) && !filter.neighborAddress.Equals(configuration.parentBrokerAddress))
                                {
                                    Console.WriteLine("Sending Unsub advertisement to " + filter.neighborAddress + " for topic: " + topic);
                                    RemoteBroker filterBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), filter.neighborAddress);
                                    filterBroker.AdvertiseUnsub(topic, configuration.processAddress, subscriberAddress);
                                    //filterBroker.FilterUpdate(topic, configuration.processAddress, false);
                                    Console.WriteLine("Unsub advertisement to " + filter.neighborAddress + " for topic: " + topic + ", confimed!");
                                    break;
                                }
                            }
                        }
                    }
                }).Start();
                Console.WriteLine("Unsubscription advertisement on topic '" + topic + "' is confirmed!");
            }

        }
        private void HandlePubAdvertisement(string topic, string lastHopAddress, string publisherAddress, int seqNum)
        {
            lock (FIFOWaitQueue)
            {
                Console.WriteLine("Got publication advertisement on topic '" + topic + "' !");
                //Received Publisher advertisement! Add publisher to filter
                NeighborForwardingFilter filter = SearchFilters(lastHopAddress);
                filter.AddPublisher(topic, publisherAddress, seqNum);
                UpdateLocalPublisher(publisherAddress, topic, seqNum);
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
                    //Send Reverse Filter Update for each subscription found!
                    RemoteBroker lastHopBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), lastHopAddress);
                    foreach (NeighborForwardingFilter neighborFilter in neighborFilters)
                    {
                        if (!neighborFilter.neighborAddress.Equals(lastHopAddress) && neighborFilter.MatchTopic(topic))
                        {
                            for (int i = 0; i < neighborFilter.interestTopics[topic]; i++)
                            {
                                lastHopBroker.FilterUpdate(topic, configuration.processAddress, true);
                            }
                        }
                    }
                    foreach(SubscriptionInfo subInfo in SearchSubscriptionsTopic(topic))
                    {
                        lastHopBroker.FilterUpdate(topic, configuration.processAddress, true);
                    }
                    //RemoteBroker lastHopBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), lastHopAddress);
                    //lastHopBroker.FilterUpdate(topic, configuration.processAddress, true);
                    //Check each neighbor filter other than last hop and parent_broker for interest in topic
                    foreach (NeighborForwardingFilter filter1 in neighborFilters)
                    {
                        if (!filter1.neighborAddress.Equals(lastHopAddress) && filter1.MatchTopic(topic) && !filter1.neighborAddress.Equals(configuration.parentBrokerAddress))
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
                Console.WriteLine("Publication advertisement on topic '" + topic + "' is confirmed!");
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

        public bool AddInterestTopic(string topic)/*, string subscriberAddress)*/
        {

            if (interestTopics.ContainsKey(topic))
            {
                interestTopics[topic]++;
                return false;
            }
            else
            {
                interestTopics.Add(new KeyValuePair<string, int>(topic, 1));
                return true;
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

        /// <summary>
        /// Checks if "topic" is in the list of interested topics for this filter!
        /// </summary>
        /// <param name="topic"></param>
        /// <returns></returns>
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
