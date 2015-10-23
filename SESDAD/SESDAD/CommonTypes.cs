using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SESDAD
{
    public delegate void NotifyEvent(Event e);
    public delegate void ConfigurationEvent(List<string> addresses);
    public delegate SESDADConfig SESDADconfiguration(string SiteName);
    public delegate SESDADBrokerConfig SESDADBrokerConfiguration();
    public delegate string PuppetMasterEvent(PuppetMasterEventArgs args);
    public enum PMEType { Register, Notify, ConfigReq }

    public class RemoteBroker : MarshalByRefObject
    {
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
            floodEvents(e);
        }
    }

    public class RemotePuppetSlave : MarshalByRefObject
    {
        public SESDADBrokerConfiguration OnGetConfiguration;    // broker configuration delegate
        public SESDADBrokerConfig GetConfiguration()
        {
            return OnGetConfiguration();
        }
    }

    public class PuppetMasterRemote : MarshalByRefObject
    {
        public PuppetMasterEvent brokerSignIn;
        public SESDADconfiguration configRequest;
        static int startPort = 9000;
        int portCounter = 0;

        public string Register(string address)
        {
            PuppetMasterEventArgs args = new PuppetMasterEventArgs(address);
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

    }
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
    public class SESDADBrokerConfig
    {
        public string brokerName;
        public string brokerAddress;
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
        string address;
        Event ev;

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

        public PuppetMasterEventArgs(PMEType type)
        {
            this.Type = type;
        }

        public PuppetMasterEventArgs(string address)
        {
            this.Type = PMEType.Register;
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
            return "Topic: " + topic + " || Message: " + eventMessage;
        }
    }
}
