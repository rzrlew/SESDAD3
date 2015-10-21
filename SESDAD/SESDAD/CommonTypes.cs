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
        public string name;
        public Queue<Event> floodList;

        public RemoteBroker()
        {
            floodList = new Queue<Event>();
        }

        public void Flood(Event e)
        {
            floodEvents(e);
        }

        public void SetParent(string address)
        {
            List<string> parentList = new List<string>();
            parentList.Add(address);
            setParentEvent(parentList);
        }

        public void SetChildren(List<string> childrenAdresses)
        {
            setChildrenEvent(childrenAdresses);
        }
    }

    public class RemotePuppetSlave : MarshalByRefObject
    {
        public SESDADBrokerConfiguration OnGetConfiguration;
        public SESDADBrokerConfig GetConfiguration()
        {
            return OnGetConfiguration();
        }
    }

    public class PuppetMasterRemote : MarshalByRefObject
    {
        public PuppetMasterEvent brokerSignIn;
        public SESDADconfiguration configRequest;
        private static int startPort = 9000;
        private int portCounter = 0;
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
        public string ProcessParentAddress;
        public string ProcessName;
        public string ProcessType;
        public string ProcessAddress;
    }

    [Serializable]
    public class SESDADBrokerConfig
    {
        public string brokerName;
        public string brokerAddress;
        public string parentBrokerAddress;
        public List<string> childrenBrokerAddresses;
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
            get{return parentBrokerAddress;}
            set{parentBrokerAddress = value;}
        }


        public string SiteName
        {
            get {return siteName;}
            set {siteName = value;}
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
        public PMEType type;
        public string siteName;
        public string address;
        public Event ev;

        public PuppetMasterEventArgs(PMEType type)
        {
            this.type = type;
        }

        public PuppetMasterEventArgs(string address)
        {
            this.type = PMEType.Register;
            this.address = address;
        }
        public PuppetMasterEventArgs(Event ev)
        {
            this.type = PMEType.Notify;
            this.ev = ev;
        }
    }

    public class SESDadQueue : MarshalByRefObject
    {
        private Queue<Event> eventQueue;
        public SESDadQueue()
        {
            eventQueue = new Queue<Event>();
        }
    }

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
