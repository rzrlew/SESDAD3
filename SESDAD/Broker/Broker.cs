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
        public TcpChannel channel;
        RemoteBroker remoteBroker;
        Queue<Event> eventQueue;
        RemoteBroker parentBroker = null;
        List<RemoteBroker> childBrokers = new List<RemoteBroker>();
        string name;

        public static void Main(string[] args)
        {
            TcpChannel channel = new TcpChannel();
            ChannelServices.RegisterChannel(channel, true);
            Broker bro = new Broker();
            PuppetMasterRemote remotePuppetMaster = (PuppetMasterRemote) Activator.GetObject(typeof(PuppetMasterRemote), "tcp://localhost:9000/puppetmaster");
            int portNum = remotePuppetMaster.GetNextPortNumber();
            bro.name = remotePuppetMaster.Register("tcp://localhost:" + portNum.ToString() + "/broker");
            ChannelServices.UnregisterChannel(channel);
            channel = new TcpChannel(portNum);
            ChannelServices.RegisterChannel(channel, true);
            Console.WriteLine("Name: " + bro.name + Environment.NewLine +"Running on port: " + portNum.ToString());
            bro.channel = channel;
            Console.WriteLine("press key to exit!!!");
            Console.ReadLine();
            bro.Flood(new Event("lololollol", "hahahaha"));
            Console.ReadLine();
        }

        public Broker()
        {
            Console.WriteLine("---Starting Broker---");
            Console.WriteLine("Creating remote broker...");
            remoteBroker = new RemoteBroker();
            remoteBroker.floodEvents += new NotifyEvent(Flood);
            remoteBroker.sendToRoot += new NotifyEvent(SendToParent);
            remoteBroker.setParentEvent += new ConfigurationEvent(SetParent);
            remoteBroker.setChildrenEvent += new ConfigurationEvent(SetChildren);
            eventQueue = new Queue<Event>();
            Console.WriteLine("Setup Complete! Registering with PuppetMaster!");
            RemotingServices.Marshal(remoteBroker, "broker");
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

        public void SendToParent(Event e)
        {
            Console.WriteLine("Sending event: " + e.ToString() + " to parent!");
            if (!isRoot())
            {
                parentBroker.SendToRoot(e);
            }
            else
            {
                Flood(e);
            }
        }

        public void Flood(Event e)
        {
            Console.WriteLine("Flooding event: " + e.ToString() + " to all children!");
            foreach (RemoteBroker child in childBrokers)
            {
                child.Flood(e);
            }
        }

        public void consume()
        {
            Event e = new Event("lol", "topic");
        }

        public void produce()
        {

        }
    }
}
