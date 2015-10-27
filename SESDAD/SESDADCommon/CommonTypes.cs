using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace SESDAD
{
    public delegate void NotifyEvent(Event e);
    public delegate void SubscriptionEvent(string topic, string address);
    public delegate void ConfigurationEvent(List<string> addresses);
    public delegate SESDADConfig SESDADconfiguration(string SiteName);
    public delegate SESDADAbstractConfig SESDADBrokerConfiguration();
    public delegate string PuppetMasterEvent(PuppetMasterEventArgs args);
    public enum PMEType { Register, Notify, ConfigReq, Log }

    /// <summary>
    /// 
    /// </summary>
    public class RemoteBroker : MarshalByRefObject
    {
        public SubscriptionEvent OnSubscribe;
        public SubscriptionEvent OnUnsubscribe;
        public NotifyEvent floodEvents;
        public NotifyEvent sendToRoot;
        public ConfigurationEvent setParentEvent;
        public ConfigurationEvent setChildrenEvent;
        public Queue<Event> floodList;
        public string name;

        public RemoteBroker()
        {
            floodList = new Queue<Event>();
        }

        public void Flood(Event e)
        {
            Thread.Sleep(500);
            floodEvents(e);
        }

        public void Subscribe(string topic, string address)
        {
            OnSubscribe(topic, address);
        }

        public void UnSubscribe(string topic, string address)
        {
            OnUnsubscribe(topic, address);
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class RemotePuppetSlave : MarshalByRefObject
    {
        public SESDADBrokerConfiguration OnGetConfiguration;    // broker configuration delegate
        public SESDADAbstractConfig GetConfiguration()
        {
            return OnGetConfiguration();
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class RemotePuppetMaster : MarshalByRefObject
    {
        public PuppetMasterEvent brokerSignIn;
        public SESDADconfiguration configRequest;
        public PuppetMasterEvent OnLogMessage;
        static int startPort = 9000;
        int portCounter = 0;

        public string Register(string address, string broker_name)
        {
            PuppetMasterEventArgs args = new PuppetMasterEventArgs(address, broker_name);
            return brokerSignIn(args);
        }

        public int GetNextPortNumber()
        {
            return ++portCounter + startPort;
        }
        
        public SESDADConfig GetConfiguration(string siteName)
        {
            return configRequest(siteName);
        }

        public void LogMessage(string message, DateTime time)
        {
            PuppetMasterEventArgs args = new PuppetMasterEventArgs(PMEType.Log);
            args.LogMessage = "[" + time.ToShortTimeString() + "]: " + message;
            OnLogMessage(args);
        }

    }

    public class RemoteSubsriber : MarshalByRefObject
    {
        public NotifyEvent OnNotifySubscription;
        public void NotifySubscriptionEvent(Event e)
        {
            Console.WriteLine("Received event on topic: " + e.topic);
            OnNotifySubscription(e);
        }
    }

    /// <summary>
    /// 
    /// </summary>
    [Serializable]
    public class SESDADProcessConfig
    {
        string processParentAddress;
        string processName;
        string processType;
        string processAddress;

        public string ProcessParentAddress
        {
            get {   return processParentAddress;    }
            set {   processParentAddress = value;   }
        }
        public string ProcessName
        {
            get {   return processName; }
            set {   processName = value;}
        }
        public string ProcessType
        {
            get { return processType; }
            set { processType = value; }
        }
        public string ProcessAddress
        {
            get { return processAddress; }
            set { processAddress = value; }
        }
    }

    [Serializable]
    public abstract class SESDADAbstractConfig
    {
        public string brokerName;
        public string brokerAddress;
    }

    [Serializable]
    public class SESDADPubSubConfig : SESDADAbstractConfig
    {
        public string name;
        public string address;
    }

    [Serializable]
    public class SESDADBrokerConfig : SESDADAbstractConfig
    {      
        public string parentBrokerAddress;
        public List<string> childrenBrokerAddresses = new List<string>();
    }

    [Serializable]
    public class SESDADConfig
    {
        string siteName;
        List<string> childrenSiteNames = new List<string>();       
        List<SESDADProcessConfig> processConfigList = new List<SESDADProcessConfig>();
        string parentSiteName;
        string parentBrokerAddress;
        List<string> childBrokersAddresses = new List<string>();
       
        public string ParentBrokerAddress
        {
            get{ return parentBrokerAddress; }
            set{ parentBrokerAddress = value; }
        }
        public string SiteName
        {
            get { return siteName; }
            set { siteName = value; }
        }
        public string ParentSiteName
        {
            get { return parentSiteName; }
            set { parentSiteName = value; }
        }
        public List<string> ChildrenSiteNames
        {
            get { return childrenSiteNames; }
            set { childrenSiteNames = value; }
        }
        public List<SESDADProcessConfig> ProcessConfigList
        {
            get { return processConfigList; }
            set { processConfigList = value; }
        }
        public List<string> ChildBrokersAddresses
        {
            get { return childBrokersAddresses; }
            set { childBrokersAddresses = value; }
        }

        public SESDADConfig(string siteName)
        {
            this.SiteName = siteName;
        }

        public SESDADProcessConfig searchBroker()
        {
            foreach(SESDADProcessConfig conf in processConfigList)
            {
                if (conf.ProcessType.Equals("broker"))
                {
                    return conf;
                }
            }
            throw new NotImplementedException();
        }
    }

    [Serializable]
    public class PuppetMasterEventArgs
    {
        PMEType type;
        string siteName;
        string brokerName;
        string address;
        Event ev;
        string logMessage;

        public PMEType Type
        {
            get { return type; }
            set { type = value; }
        }
        public string SiteName
        {
            get { return siteName; }
            set { siteName = value; }
        }
        public string Address
        {
            get { return address; }
            set { address = value; }
        }
        public Event Ev
        {
            get{ return ev; }
            set{ ev = value; }
        }

        public string LogMessage
        {
            get{return logMessage;}
            set{logMessage = value;}
        }

        public string BrokerName
        {
            get{return brokerName;}
            set{brokerName = value;}
        }

        public PuppetMasterEventArgs(PMEType type)
        {
            this.Type = type;
        }

        public PuppetMasterEventArgs(string address, string broker_name)
        {
            this.Type = PMEType.Register;
            this.brokerName = broker_name;
            this.Address = address;
        }
        public PuppetMasterEventArgs(Event ev)
        {
            this.Type = PMEType.Notify;
            this.Ev = ev;
        }
    }

    //public class SESDadQueue : MarshalByRefObject
    //{
    //    private Queue<Event> eventQueue;
    //    public SESDadQueue()
    //    {
    //        eventQueue = new Queue<Event>();
    //    }
    //}

    [Serializable]
    public class Event
    {
        public string topic;
        public string eventMessage;
        public string lastHop;
        public Event(string message, string topic, string lastHop)
        {
            eventMessage = message;
            this.topic = topic;
            this.lastHop = lastHop;
        }
        public string Message()
        {
            return "Topic: " + topic + Environment.NewLine + "Message: " + eventMessage;
        }
    }
}
