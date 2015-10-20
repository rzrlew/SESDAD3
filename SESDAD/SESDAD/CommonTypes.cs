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
    public delegate SESDADConfig SESDADBrokerConfiguration();
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
        public SESDADConfig GetConfiguration()
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

    public class SESDADConfig
    {
        string siteName;
        string[] processList;
        string parentBrokerAddress;
        string[] childBrokersAddresses;
        public string[] ProcessList
        {
            get{return processList;}
            set{processList = value;}
        }

        public string ParentBrokerAddress
        {
            get{return parentBrokerAddress;}
            set{parentBrokerAddress = value;}
        }

        public string[] ChildBrokersAddresses
        {
            get{return childBrokersAddresses;}
            set{childBrokersAddresses = value;}
        }

        public string SiteName
        {
            get {return siteName;}

            set {siteName = value;}
        }

        public SESDADConfig(string siteName)
        {
            this.SiteName = siteName;
        }


    }

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
