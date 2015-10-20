using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using SESDAD;

namespace SESDADBroker
{
    public class Broker
    {
        SESDADConfig configuration;
        public TcpChannel channel;
        RemoteBroker remoteBroker;
        Queue<Event> eventQueue;
        RemoteBroker parentBroker = null;
        List<RemoteBroker> childBrokers = new List<RemoteBroker>();
        string name;

        public string Name
        {
            set{name = value;
                remoteBroker.name = name;}
        }

        public static void Main(string[] args)
        {
            TcpChannel channel = new TcpChannel();
            ChannelServices.RegisterChannel(channel, true);
            RemotePuppetSlave remotePuppetSlave = (RemotePuppetSlave) Activator.GetObject(typeof(PuppetMasterRemote), args[1]);
            SESDADConfig configuration = remotePuppetSlave.GetConfiguration();
            ChannelServices.UnregisterChannel(channel);
            channel = new TcpChannel(new Uri(configuration.SiteBrokerAddress).Port);
            ChannelServices.RegisterChannel(channel, true);
            Broker bro = new Broker(configuration.SiteName + "broker");
            bro.configuration = configuration;          
            bro.channel = channel;
            List<string> parentAddress = new List<string>();
            parentAddress.Add(configuration.ParentBrokerAddress);
            bro.SetParent(parentAddress);
            bro.SetChildren(configuration.ChildBrokersAddresses.ToList());

            //Test Prints
            Console.WriteLine("Name: " + bro.name + Environment.NewLine +"Running on address: " + configuration.SiteBrokerAddress);
            Console.WriteLine("press <any> key to flood...");
            Console.ReadLine();
            bro.Flood(new Event("lololollol", "hahahaha", bro.name));
            Console.WriteLine("Show queue: press <any> key!");
            Console.ReadLine();
            Event e = bro.remoteBroker.floodList.Dequeue();
            Console.WriteLine(e.Message());
            Console.ReadLine();
        }

        public Broker(string name)
        {
            Console.WriteLine("---Starting Broker---");
            Console.WriteLine("Creating remote broker...");
            remoteBroker = new RemoteBroker();
            remoteBroker.floodEvents += new NotifyEvent(Flood);
            remoteBroker.setParentEvent += new ConfigurationEvent(SetParent);
            remoteBroker.setChildrenEvent += new ConfigurationEvent(SetChildren);
            eventQueue = new Queue<Event>();
            RemotingServices.Marshal(remoteBroker, name);
            Console.WriteLine("Broker is listening...");
        }

        public bool isRoot()
        {
            return (parentBroker == null) ? true : false;
        }

        public void SetParent(List<string> parentAddress)
        {
            Console.WriteLine("Broker received message to set parent!");
            parentBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), parentAddress.ElementAt(0));
        }

        public void SetChildren(List<string> childrenAddresses)
        {
            Console.WriteLine("Broker received messsage to set children!");
            foreach (string childAddress in childrenAddresses)
            {
                childBrokers.Add((RemoteBroker)Activator.GetObject(typeof(RemoteBroker), childAddress));
            }
        }

        public void Flood(Event e)
        {
            string lastHopName = e.lastHop;
            Console.WriteLine("Flooding event: " + e.Message() + " from " + e.lastHop + " to all children!");
            e.lastHop = name;
            remoteBroker.floodList.Enqueue(e);
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
}
