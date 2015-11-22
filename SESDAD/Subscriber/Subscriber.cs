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
        public List<string> topicList = new List<string>();
        public RemoteBroker serviceBroker;
        public RemoteSubscriber remoteSubscriber;
        private string brokerAddress;
        public string address;
        static void Main(string[] args)
        {
            Subscriber subs = new Subscriber(args[0], args[1]);  // arg[0] -> susbscriber address || arg[1] -> broker address
            Console.WriteLine("press key to terminate...");
            Console.ReadLine();
        }

        public Subscriber(string address, string broker_address)
        {
            this.address = address;
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

        private string SendStatus()
        {
            string msg = "[Subscriber - " + new Uri(this.address).LocalPath.Split('/')[1] + "] Broker: " + serviceBroker.name + Environment.NewLine;
            msg += "[Subscriber - " + new Uri(this.address).LocalPath.Split('/')[1] + "]----Subscriptions----";
            foreach (string topic in topicList)
            {
                msg += "[Subscriber - " + new Uri(this.address).LocalPath.Split('/')[1] + "] Topic: " + topic + Environment.NewLine;
            }
            msg += "[Subscriber - " + new Uri(this.address).LocalPath.Split('/')[1] + "]----/Subscriptions----";
            return msg;
        }
        private void ShowEvent(PublicationEvent e)
        {
            Console.WriteLine("Receiving Subscription Event..." + Environment.NewLine + e.Message());
        }
        private void Subscribe(string topic)
        {
            Console.WriteLine("Subscribing events on topic '" + topic + "' with broker at " + brokerAddress);
            SubscriptionEvent subEvent = new SubscriptionEvent(topic, address);
            serviceBroker.Subscribe(subEvent);
        }
        private void Unsubscribe(string topic)
        {
            UnsubscriptionEvent unsubEvent = new UnsubscriptionEvent(topic, address);
            serviceBroker.UnSubscribe(unsubEvent);
        }
    }
}
