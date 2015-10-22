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
        SESDADBrokerConfig configuration;
        public TcpChannel channel;
        RemoteBroker remoteBroker;
        Queue<Event> eventQueue;
        RemoteBroker parentBroker = null;
        List<RemoteBroker> childBrokers = new List<RemoteBroker>();
        string name;

        public string Name
        {
            set
            {
                name = value;
                remoteBroker.name = name;
            }
        }

        public static void Main(string[] args)
        {
            Console.WriteLine("Getting Broker configuration from site slave");
            TcpChannel temp_channel = new TcpChannel();
            ChannelServices.RegisterChannel(temp_channel, true);
            RemotePuppetSlave remotePuppetSlave = (RemotePuppetSlave) Activator.GetObject(typeof(RemotePuppetSlave), args[0]);
            SESDADBrokerConfig configuration = remotePuppetSlave.GetConfiguration();
            Console.WriteLine("Starting broker channel on port: " + new Uri(configuration.brokerAddress).Port);
            TcpChannel channel = new TcpChannel(new Uri(configuration.brokerAddress).Port);
            ChannelServices.UnregisterChannel(temp_channel);
            ChannelServices.RegisterChannel(channel, true);
            Broker bro = new Broker(configuration);
            bro.configuration = remotePuppetSlave.GetConfiguration();
            bro.channel = channel;
            Console.WriteLine("press <any> key to flood...");
            Console.ReadLine();
            bro.Flood(new Event("lololollol", "hahahaha", bro.name));
            Console.WriteLine("Show queue: press <any> key!");
            Console.ReadLine();
            Event e = bro.remoteBroker.floodList.Dequeue();
            Console.WriteLine(e.Message());
            Console.ReadLine();
        }

        public Broker(SESDADBrokerConfig config)
        {
            Console.WriteLine("---Starting Broker---");
            Console.WriteLine("Creating remote broker on " + config.brokerAddress);
            configuration = config;
            remoteBroker = new RemoteBroker();
            remoteBroker.floodEvents += new NotifyEvent(Flood);
            eventQueue = new Queue<Event>();
            Name = config.brokerName;
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

        public bool isRoot()
        {
            return (parentBroker == null) ? true : false;
        }

        public void SetParent(List<string> parentAddress)
        {
            Console.WriteLine("Adding parent broker at: " + parentAddress.ElementAt(0));
            parentBroker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), parentAddress.ElementAt(0));
        }

        public void SetChildren(List<string> childrenAddresses)
        {
            foreach (string childAddress in childrenAddresses)
            {
                Console.WriteLine("Adding child broker at: " + childAddress);
                childBrokers.Add((RemoteBroker)Activator.GetObject(typeof(RemoteBroker), childAddress));
            }
        }

        public void Flood(Event e)
        {
            string lastHopName = e.lastHop;
            Console.WriteLine("Flooding event: " + e.Message() + " from " + e.lastHop + " to all children!");
            e.lastHop = name;
            remoteBroker.floodList.Enqueue(e);
            if (parentBroker != null && parentBroker.GetName() != lastHopName)
            {
                Console.WriteLine("Sending event to " + parentBroker.GetName());
                parentBroker.Flood(e);
            }
            foreach (RemoteBroker child in childBrokers)
            {
                if (child.GetName() != lastHopName)
                {
                    Console.WriteLine("Sending event to " + child.GetName());
                    child.Flood(e);
                }
            }
        }
    }
}
