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
        List<PublisherInfo> publicationList = new List<PublisherInfo>();

        List<string> topicList = new List<string>();
        string name;

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
            Broker bro = new Broker(configuration);
            bro.configuration = configuration;
            bro.Channel = channel;           
            Console.WriteLine("press key to terminate...");
            Console.ReadLine();
            Console.WriteLine("Ending Broker Process: " + bro.name);
        }

        public Broker(SESDADBrokerConfig config)
        {
            Console.WriteLine("---Starting Broker---");
            Console.WriteLine("Creating remote broker on " + config.processAddress);
            configuration = config;
            remoteBroker = new RemoteBroker();
            remoteBroker.floodEvents += new NotifyEvent(Flood);
            remoteBroker.OnSubscribe += new PubSubEventDelegate(Subscription);
            remoteBroker.OnUnsubscribe += new PubSubEventDelegate(Unsubscription);
            remoteBroker.OnAdvertise += new PubSubEventDelegate(AdvertisePublish);
            eventQueue = new Queue<Event>();
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

        public void AdvertisePublish(string topic, string address)  // TODO: Send advertisement to root node
        {
            Thread thr = new Thread(() => AdvertisePublishWork(topic, address));
            thr.Start();
        }

        public void AdvertisePublishWork(string topic, string address)  // TODO: Send advertisement to root node
        {
            try
            {
                PublisherInfo info = SearchPublication(address);
                info.topics.Add(topic);
            }
            catch (NotImplementedException)
            {
                PublisherInfo info = new PublisherInfo(topic, address);
                publicationList.Add(info);
            }
            finally
            {
                if (!isRoot())
                {
                    parentBroker.OnAdvertise(topic, address);
                }
            }
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

        public bool checkTopicInterest(Event e, string topic)
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
            for(int i = 0; i < topicArgs.Count(); i++)
            {
                if (!eventArgs[i].Equals(topicArgs[i]))
                {
                    return false;
                }
            }
            return true;
        }

        public void Flood(Event e)
        {
            Thread thr = new Thread(() => FloodWork(e));
            thr.Start();
            if (subscriptionsList.Any())
            {
                foreach (SubscriptionInfo info in subscriptionsList)
                {
                    foreach (string topic in info.topics)
                    {
                        if (checkTopicInterest(e, topic))
                        {
                            Console.WriteLine("Subscriber Address: " + info.subscription_address);
                            RemoteSubsriber subscriber = (RemoteSubsriber)Activator.GetObject(typeof(RemoteSubsriber), info.subscription_address);
                            subscriber.NotifySubscriptionEvent(e);
                        }
                    }
                }
            }
        }

        public void FloodWork(Event e)
        {
            string lastHopName = e.lastHop;
            
            //Console.WriteLine("Flooding event: " + e.Message() + " from " + e.lastHop + " to all children!");
            e.lastHop = name;
            //remoteBroker.floodList.Enqueue(e);
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

    public class PublisherInfo
    {
        public string publication_address;
        public List<string> topics = new List<string>();

        public PublisherInfo(string topic, string address)
        {
            topics.Add(topic);
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
