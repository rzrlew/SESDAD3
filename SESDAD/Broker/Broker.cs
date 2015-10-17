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
        TcpChannel channel;
        RemoteBroker remoteBroker;
        Queue<Event> eventQueue;
        RemoteBroker parentBroker = null;
        List<RemoteBroker> childBrokers;
        string name;

        public Broker(string name)
        {
            Console.WriteLine("---Starting Broker---");
            channel = new TcpChannel();
            ChannelServices.RegisterChannel(channel, true);
            Console.WriteLine("Creating remote broker...");
            remoteBroker = new RemoteBroker();
            remoteBroker.floodEvents += new NotifyEvent(Flood);
            remoteBroker.sendToRoot += new NotifyEvent(SendToParent);
            remoteBroker.setParentEvent += new ConfigurationEvent(SetParent);
            remoteBroker.setChildrenEvent += new ConfigurationEvent(SetChildren);
            eventQueue = new Queue<Event>();
            Console.WriteLine("Setup Complete! Registering with PuppetMaster!");

            RemotingServices.Marshal(remoteBroker, name);
            Console.WriteLine("Broker is listening...");
        }

        public void RegisterOnPuppetMaster()
        {
            Activator.GetObject(typeof(PuppetMasterRemote), "tcp://localhost:");
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
