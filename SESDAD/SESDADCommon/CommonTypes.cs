using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;

namespace SESDAD
{
    public delegate void NotifyEvent(Event e);
    public delegate string StatusRequestDelegate();
    public delegate void PubSubEventDelegate(string topic, string address);
    public delegate void ConfigurationEvent(List<string> addresses);
    public delegate SESDADConfig SESDADconfiguration(string SiteName);
    public delegate SESDADProcessConfiguration SESDADProcessConfigurationDelegate();
    public delegate SESDADConfig SESDADSlaveConfigurationDelegate();
    public delegate string PuppetMasterEvent(PuppetMasterEventArgs args);
    public delegate void LogMessageDelegate(string message);
    public enum PMEType { Register, Notify, ConfigReq, Log }



    public interface SESDADRemoteProcessControlInterface
    {
        string Status();
        void Freeze();
        void Unfreeze();
        void Crash();
    }

    /// <summary>
    /// 
    /// </summary>
    public class RemoteBroker : MarshalByRefObject, SESDADRemoteProcessControlInterface
    {
        private string slaveAddress;
        public PubSubEventDelegate OnSubscribe;
        public PubSubEventDelegate OnUnsubscribe;
        public PubSubEventDelegate OnAdvertise;
        public StatusRequestDelegate OnStatusRequest;
        public NotifyEvent floodEvents;
        public NotifyEvent sendToRoot;
        public ConfigurationEvent setParentEvent;
        public ConfigurationEvent setChildrenEvent;
        public Queue<Event> floodList;
        public string name;
        public bool isFrozen = false;
        Queue<Event> frozenEventsQueue = new Queue<Event>();
        
        public RemoteBroker(string slaveAddress)
        {
            this.slaveAddress = slaveAddress;
            floodList = new Queue<Event>();
        }

        public void Flood(Event e)
        {
            if (!isFrozen)
            {
                floodEvents(e);
            }
            else
            {
                frozenEventsQueue.Enqueue(e);
            }
        }

        public void Advertise(string topic, string address)
        {

            OnAdvertise(topic, address);
        }

        public void Subscribe(string topic, string address)
        {
            OnSubscribe(topic, address);
        }

        public void UnSubscribe(string topic, string address)
        {
            OnUnsubscribe(topic, address);
        }

        public void Freeze()
        {
            Console.WriteLine("Freezing... Collecting messages...");
            isFrozen = true;
        }

        public void Unfreeze()
        {
            Console.WriteLine("Unfreezing, replaying collected messages...");
            isFrozen = false;
            while(frozenEventsQueue.Count > 0)
            {
                Flood(frozenEventsQueue.Dequeue());
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
    }

    /// <summary>
    /// 
    /// </summary>
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
    }

    /// <summary>
    /// 
    /// </summary>
    public class RemotePuppetMaster : MarshalByRefObject
    {
        public SESDADSlaveConfigurationDelegate slaveSignIn;
        public SESDADconfiguration configRequest;
        public PuppetMasterEvent OnLogMessage;
        static int slaveStartPort = 9000;
        int portCounter = 0;

        public SESDADConfig RegisterSlave()
        {
            return slaveSignIn();
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
            PuppetMasterEventArgs args = new PuppetMasterEventArgs(PMEType.Log);
            args.LogMessage = message;
            OnLogMessage(args);
        }
    }

    public class RemotePublisher : MarshalByRefObject, SESDADRemoteProcessControlInterface
    {
        public StatusRequestDelegate OnStatusRequest;
        public PubSubEventDelegate OnPublishRequest;
        private bool isFrozen = false;

        public string Status()
        {
            return OnStatusRequest();
        }

        public void Publish(string topic, string message)
        {
            OnPublishRequest(topic, message);
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
    }

    public class RemoteSubscriber : MarshalByRefObject, SESDADRemoteProcessControlInterface
    {
        private bool isFrozen = false;
        public NotifyEvent OnNotifySubscription;
        public StatusRequestDelegate OnStatusRequest;
        public void NotifySubscriptionEvent(Event e)
        {
            OnNotifySubscription(e);
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
    }

    /// <summary>
    /// 
    /// </summary>
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

    [Serializable]
    public class SESDADBrokerConfig : SESDADProcessConfiguration
    {      
        public string parentBrokerAddress;
        public List<string> childrenBrokerAddresses = new List<string>();
    }

    [Serializable]
    public class SESDADConfig
    {
        public bool isDone = false;
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
        public string publisher;
        public int SequenceNumber;
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
