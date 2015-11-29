using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System.Threading;

namespace SESDAD
{
    public delegate void NotifyEventDelegate(PublicationEvent e);
    public delegate string StatusRequestDelegate();
    public delegate void PublicationRequestDelegate(PublicationEvent e);
    public delegate void SubsRequestDelegate(string topic);
    public delegate void PubSubEventDelegate(string topic, string address);
    public delegate void UpdateFilterEventDelegate(string topic, string lasHopAddress, bool toAdd);
    public delegate void SubscriptionAdvertisementDelegate(string topic, string lastHopAddress, string subscriberAddress);
    public delegate void PubAdvertisementEventDelegate(string topic, string lastHopAddress, string publisherAddress, int seqNum);
    public delegate void PublicationEventDelegate(string topic, int numEvents, int interval);
    public delegate void ConfigurationEventDelegate(List<string> addresses);
    public delegate SESDADConfig SESDADconfigurationDelegate(string SiteName);
    public delegate SESDADProcessConfiguration SESDADProcessConfigurationDelegate();
    public delegate SESDADConfig SESDADSlaveConfigurationDelegate(string ip_address);
    public delegate string PuppetMasterLogEventDelegate(string message);
    public delegate void LogMessageDelegate(string message);

    public interface SESDADRemoteProcessControlInterface
    {
        string Status();
        void Freeze();
        void Unfreeze();
        void Crash();
    }

    public interface SESDADPublisherControlInterface : SESDADRemoteProcessControlInterface
    {
        void Publish(string topic, int numEvents, int interval);
    }

    public interface SESDADSubscriberControlInterface : SESDADRemoteProcessControlInterface
    {
        void Subscribe(string topic);
        void Unsubscribe(string topic);
    }

    /// <summary>
    /// 
    /// </summary>
    public class RemoteBroker : MarshalByRefObject, SESDADRemoteProcessControlInterface
    {
        private string slaveAddress;
        public PubSubEventDelegate OnSubscribe;
        public PubSubEventDelegate OnUnsubscribe;
        public PubAdvertisementEventDelegate OnAdvertisePublisher;
        public SubscriptionAdvertisementDelegate OnAdvertiseSubscriber;
        public SubscriptionAdvertisementDelegate OnAdvertiseUnsubscriber;
        public UpdateFilterEventDelegate OnFilterUpdate;
        public PubAdvertisementEventDelegate OnSequenceUpdate;
        public StatusRequestDelegate OnStatusRequest;
        public PublicationRequestDelegate OnPublication;
        public NotifyEventDelegate OnEventReceived;
        public Queue<PublicationEvent> floodList;
        public string name;
        public bool isFrozen = false;
        Queue<EventInterface> frozenEventsQueue = new Queue<EventInterface>();
        
        public RemoteBroker(string slaveAddress)
        {
            this.slaveAddress = slaveAddress;
        }

        public void PublishEvent(PublicationEvent e)
        {
            lock (frozenEventsQueue)
            {
                if (!isFrozen)
                {
                    e.isFirstHop = false;
                    OnPublication(e);
                }
                else
                    frozenEventsQueue.Enqueue(e);
            }
        }
        public void RouteEvent(PublicationEvent e)
        {
            lock (frozenEventsQueue)
            {
                if (!isFrozen)
                    OnEventReceived(e);
                else
                    frozenEventsQueue.Enqueue(e);
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="topic">Topic interested</param>
        /// <param name="address">Indicates last hop broker address. e.g. "reverse path"</param>
        public void AdvertiseSubscriber(string topic, string lastHopAddress, string subAddress)
        {
            OnAdvertiseSubscriber(topic, lastHopAddress, subAddress);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="lastHopAddress">Indicates last hop broker address. e.g. "reverse path"</param>
        public void AdvertisePublisher(string topic, string lastHopAddress, string publisherAddress, int seqNumber)
        {
            OnAdvertisePublisher(topic, lastHopAddress, publisherAddress, seqNumber);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="lastHopAddress">Indicates last hop broker address. e.g. "reverse path"</param>
        public void AdvertiseUnsub(string topic, string lastHopAddress, string subscriberAddress)
        {
            OnAdvertiseUnsubscriber(topic, lastHopAddress, subscriberAddress);
        }
        public void FilterUpdate(string topic, string lastHopAddress, bool toAdd)
        {
            OnFilterUpdate(topic, lastHopAddress, toAdd);
        }
        public void SequenceUpdate(string topic, string lastHopAddress, string publisherAddress, int seqNum)
        {
            OnSequenceUpdate(topic, lastHopAddress, publisherAddress, seqNum);
        }
        public void Subscribe(SubscriptionEvent e)
        {
            lock (frozenEventsQueue)
            {
                if (!isFrozen)
                    OnSubscribe(e.subUnsubTopic, e.subUnsubAddress);
                else
                    frozenEventsQueue.Enqueue(e);
            }
        }
        public void UnSubscribe(UnsubscriptionEvent e)
        {
            lock (frozenEventsQueue)
            {
                if (!isFrozen)
                    OnUnsubscribe(e.subUnsubTopic, e.subUnsubAddress);
                else
                    frozenEventsQueue.Enqueue(e);
            }
        }
        public void Freeze()
        {
            Console.WriteLine("Freezing... Collecting messages...");
            isFrozen = true;
        }
        public void Unfreeze()
        {
            lock (frozenEventsQueue)
            {
                isFrozen = false;
                Console.WriteLine("Unfreezing, replaying collected messages...");
                while (frozenEventsQueue.Count > 0)
                {
                    EventInterface e = frozenEventsQueue.Dequeue();
                    switch (e.GetEventType())
                    {
                        case EventType.Publication:
                            if (((PublicationEvent)e).isFirstHop)
                            {
                                PublishEvent((PublicationEvent)e);
                            }
                            else
                            {
                                RouteEvent((PublicationEvent)e);
                            }
                            break;
                        case EventType.Subscription:
                            Subscribe((SubscriptionEvent)e);
                            break;
                        case EventType.Unsubscription:
                            UnSubscribe((UnsubscriptionEvent)e);
                            break;
                    }
                }
            }
        }
        public string Status()
        {
            return OnStatusRequest() + "[Broker] Frozen: " + isFrozen.ToString();
        }
        public void Crash()
        {
            Process.GetCurrentProcess().Kill();
        }
        public override object InitializeLifetimeService()
        {
            return null;
        }
    }

    public class RemotePuppetSlave : MarshalByRefObject
    {
        public LogMessageDelegate OnLogMessage;
        public SESDADProcessConfigurationDelegate OnGetConfiguration;    // broker configuration delegate
        
        public SESDADProcessConfiguration GetConfiguration()
        {
            return OnGetConfiguration();
        }

        public void SendLog(string message)
        {
            OnLogMessage(message);
        }

        public override object InitializeLifetimeService()
        {
            return null;
        }
    }

    public class RemotePuppetMaster : MarshalByRefObject
    {
        public SESDADSlaveConfigurationDelegate slaveSignIn;
        public SESDADconfigurationDelegate configRequest;
        public PuppetMasterLogEventDelegate OnLogMessage;
        static int slaveStartPort = 9000;
        int portCounter = 0;

        public SESDADConfig RegisterSlave()
        {
            string clientIP = CallContext.GetData("ClientIPAddress").ToString();
            return slaveSignIn(clientIP);
        }

        public int GetNextPortNumber()
        {
            return ++portCounter + slaveStartPort;
        }
        
        public SESDADConfig GetConfiguration(string siteName)
        {
            return configRequest(siteName);
        }

        public void LogMessage(string message)
        {
            OnLogMessage(message);
        }

        public override object InitializeLifetimeService()
        {
            return null;
        }
    }

    public class RemotePublisher : MarshalByRefObject, SESDADPublisherControlInterface
    {
        public StatusRequestDelegate OnStatusRequest;
        public PublicationEventDelegate OnPublishRequest;
        private bool isFrozen = false;

        public string Status()
        {
            return OnStatusRequest();
        }

        public void Publish(string topic, int numEvents, int interval)
        {
            OnPublishRequest(topic, numEvents, interval);
        }

        public void Freeze()
        {
            Console.WriteLine("Freezing!");
            isFrozen = true;
        }

        public void Unfreeze()
        {
            Console.WriteLine("Unfreezing!");
            isFrozen = false;
        }

        public void Crash()
        {
            Process.GetCurrentProcess().Kill();
        }

        public override object InitializeLifetimeService()
        {
            return null;
        }
    }

    public class RemoteSubscriber : MarshalByRefObject, SESDADSubscriberControlInterface
    {
        private bool isFrozen = false;
        public NotifyEventDelegate OnNotifySubscription;
        public StatusRequestDelegate OnStatusRequest;
        public SubsRequestDelegate OnSubscriptionRequest;
        public SubsRequestDelegate OnUnsubscriptionRequest;
        public void NotifySubscriptionEvent(PublicationEvent e)
        {
            OnNotifySubscription(e);
        }

        public void Subscribe(string topic)
        {
            OnSubscriptionRequest(topic);
        }

        public void Unsubscribe(string topic)
        {
            OnUnsubscriptionRequest(topic);
        }

        public string Status()
        {
            return OnStatusRequest();
        }

        public void Freeze()
        {
            Console.WriteLine("Freezing!");
            isFrozen = true;
        }

        public void Unfreeze()
        {
            Console.WriteLine("Unfreezing!");
            isFrozen = false;
        }

        public void Crash()
        {
            Process.GetCurrentProcess().Kill();
        }

        public override object InitializeLifetimeService()
        {
            return null;
        }
    }

    [Serializable]
    public class SESDADProcessConfig
    {
        public string processParentAddress;
        public string processName;
        public string processType;
        public string processAddress;
    }

    [Serializable]
    public abstract class SESDADProcessConfiguration
    {
        public string processParentAddress;
        public string processName;
        public string processType;
        public string processAddress;
    }

    [Serializable]
    public class SESDADPubSubConfig : SESDADProcessConfiguration
    {
        public string brokerName;
        public string brokerAddress;
    }

    public enum OrderMode { NoOrder, FIFO, TotalOrder};

    [Serializable]
    public class SESDADBrokerConfig : SESDADProcessConfiguration
    {
        public OrderMode orderMode;
        public string routingPolicy;
        public string loggingLevel;
        public string parentBrokerAddress;
        public List<string> childrenBrokerAddresses = new List<string>();
    }

    [Serializable]
    public class SESDADConfig
    {
        public bool isDone = false;
        public OrderMode orderMode;
        public string routingPolicy;
        public string loggingLevel;
        public string siteName;
        public List<string> childrenSiteNames = new List<string>();
        public List<SESDADProcessConfig> processConfigList = new List<SESDADProcessConfig>();
        public string parentSiteName;
        public string parentBrokerAddress;
        public List<string> childBrokersAddresses = new List<string>();       
       
        public SESDADConfig(string siteName)
        {
            this.siteName = siteName;
        }

        public SESDADProcessConfig searchBroker()
        {
            foreach(SESDADProcessConfig conf in processConfigList)
            {
                if (conf.processType.Equals("broker"))
                {
                    return conf;
                }
            }
            throw new NotImplementedException();
        }
    }

    [Serializable]
    public enum EventType { Publication, Subscription, Unsubscription}

    public interface EventInterface
    {
        EventType GetEventType();
    }

    public interface PublicationEventInterface : EventInterface
    {
        string GetTopic();
        string GetMessage();
        string GetPublisher();
        int GetSeqNumber();
        string GetLastHop();
    }

    public interface SubUnsubEventInterface : EventInterface
    {
        string GetSubUnsubAddress();
        string GetSubUnsubTopic();
    }

    [Serializable]
    public class PublicationEvent : PublicationEventInterface
    {
        public string topic;
        public string eventMessage;
        public string publisher;
        public int SequenceNumber;
        public string lastHop;
        public bool isFirstHop = true;

        /// <summary>
        /// Use this contructor to create a "Publication" Event.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="topic"></param>
        /// <param name="lastHop"></param>
        public PublicationEvent(string message, string topic, string lastHop)
        {
            eventMessage = message;
            this.topic = topic;
            this.lastHop = lastHop;
        }

        public string Message()
        {
            return "Topic: " + topic + "||Message: " + eventMessage;
        }

        public string GetTopic()
        {
            return topic;
        }

        public string GetMessage()
        {
            return eventMessage;
        }

        public string GetPublisher()
        {
            return publisher;
        }

        public int GetSeqNumber()
        {
            return SequenceNumber;
        }

        public string GetLastHop()
        {
            return lastHop;
        }

        public EventType GetEventType()
        {
            return EventType.Publication;
        }
    }

    [Serializable]
    public abstract class SubUnsubEvent : SubUnsubEventInterface
    {
        public string subUnsubAddress;
        public string subUnsubTopic;

        public string GetSubUnsubAddress()
        {
            return subUnsubAddress;
        }
        public string GetSubUnsubTopic()
        {
            return subUnsubTopic;
        }
        public abstract EventType GetEventType();
    }

    [Serializable]
    public class SubscriptionEvent : SubUnsubEvent
    {
        public SubscriptionEvent(string topic, string address)
        {
            subUnsubAddress = address;
            subUnsubTopic = topic;
        }

        public override EventType GetEventType()
        {
            return EventType.Subscription;
        }
    }

    [Serializable]
    public class UnsubscriptionEvent : SubUnsubEvent
    {
        public UnsubscriptionEvent(string topic, string address)
        {
            subUnsubAddress = address;
            subUnsubTopic = topic;
        }

        public override EventType GetEventType()
        {
            return EventType.Unsubscription;
        }
    }
}
