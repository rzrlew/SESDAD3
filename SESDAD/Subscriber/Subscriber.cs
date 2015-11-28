using System;
using System.Collections.Generic;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting;
using System.Threading;
using SESDAD;

namespace SESDADSubscriber
{
    class Subscriber
    {
        private List<string> topicList = new List<string>();
        private RemoteBroker serviceBroker;
        private RemoteSubscriber remoteSubscriber;
        private RemotePuppetMaster remotePuppetMaster;
        private string puppetMasterAddress;
        private string brokerAddress;
        private string name;
        private string address;
        static void Main(string[] args)
        {
            Subscriber subs = new Subscriber(args[0], args[1], args[2], args[3]);  // arg[0] -> susbscriber address || arg[1] -> broker address || arg[2] -> name || arg[3] -> puppetMasterAddress
            Console.WriteLine("press key to terminate...");
            Console.ReadLine();
        }

        public Subscriber(string address, string broker_address, string name)
        {
            this.address = address;
            this.name = name;
            TcpChannel channel = new TcpChannel(new Uri(address).Port);//Creates the tcp channel on the port given in the config file....
            ChannelServices.RegisterChannel(channel, true);
            remoteSubscriber = new RemoteSubscriber();
            remoteSubscriber.OnNotifySubscription += new NotifyEventDelegate(ShowEvent);
            remoteSubscriber.OnStatusRequest = new StatusRequestDelegate(SendStatus);
            remoteSubscriber.OnSubscriptionRequest = new SubsRequestDelegate(Subscribe);
            remoteSubscriber.OnUnsubscriptionRequest = new SubsRequestDelegate(Unsubscribe);
            serviceBroker = (RemoteBroker) Activator.GetObject(typeof(RemoteBroker), broker_address);
            RemotingServices.Marshal(remoteSubscriber, new Uri(address).LocalPath.Split('/')[1]);
            brokerAddress = broker_address;
        }

        public Subscriber(string address, string broker_address, string name, string puppet_master_addr) : this(address, broker_address, name)
        {
            remotePuppetMaster = (RemotePuppetMaster)Activator.GetObject(typeof(RemotePuppetMaster), puppet_master_addr);
            this.puppetMasterAddress = puppet_master_addr;
        }

        private string SendStatus()
        {
            string msg = "[Subscriber - " + name + "] Broker: " + serviceBroker.name;
            msg += "||Topics:{";
            bool isFirstTopic = true;
            foreach (string topic in topicList)
            {
                if (isFirstTopic)
                {
                    msg += topic;
                    isFirstTopic = false;
                }
                else
                    msg += ", " + topic;
            }
            msg += "}";
            return msg;
        }
        private void ShowEvent(PublicationEvent e)
        {
            remotePuppetMaster.LogMessage("[Subscriber - " + name + "] " + e.Message());
            Console.WriteLine("[EVENT]" + e.Message());
        }
        private void Subscribe(string topic)
        {
            new Thread(() =>
            {
                lock (topicList)
                {
                    //If topic is NOT susbscribed
                    if (topicList.Find(x => x.Equals(topic) ? true : false) == null)
                    {
                        Console.WriteLine("Subscribing events on topic '" + topic + "' with broker at " + brokerAddress);
                        SubscriptionEvent subEvent = new SubscriptionEvent(topic, address);
                        serviceBroker.Subscribe(subEvent);
                        topicList.Add(topic);
                        remotePuppetMaster.LogMessage("[" + name + "] Broker confirmed subscription of topic '" + topic + "'");
                    } 
                }
            }).Start();
            
        }
        private void Unsubscribe(string topic)
        {
            new Thread(() =>
            {
                lock (topicList)
                {
                    //If topic is subscribed!
                    if (topicList.Find(x => x.Equals(topic) ? true : false) != null)
                    {
                        UnsubscriptionEvent unsubEvent = new UnsubscriptionEvent(topic, address);
                        serviceBroker.UnSubscribe(unsubEvent);
                        topicList.Remove(topic);
                        remotePuppetMaster.LogMessage("[" + name + "] Broker confirmed unsubscription of topic '" + topic + "'");
                    }
                }
            }).Start();
        }
    }
}
