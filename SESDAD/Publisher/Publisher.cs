using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting;
using SESDAD;

namespace SESDADPublisher
{
    class Publisher
    {
        public string address;
        public string brokerAddress;
        public TcpChannel channel;
        public RemoteBroker remoteBroker;
        public RemotePublisher remotePublisher;
        private int SequenceNumber = 0;

        static void Main(string[] args)
        {
            Publisher pub = new Publisher(args[0], args[1]);
            Console.WriteLine("write [publish] to create and flood event or [quit] to exit...");
            string input = Console.ReadLine();
            while (!input.Equals("quit"))
            {
                if (input.Equals("publish"))
                {
                    Console.WriteLine("insert: [Topic]...");
                    string topic = Console.ReadLine();
                    Console.WriteLine("insert: [Message]...");
                    string message = Console.ReadLine();
                    PublicationEvent e = new PublicationEvent(message, topic, pub.remoteBroker.name);
                    pub.publishEvent(e);
                }
            }
        }

        public Publisher(string address, string brokerAddress)
        {
            this.address = address;
            this.brokerAddress = brokerAddress;
            remotePublisher = new RemotePublisher();
            remotePublisher.OnStatusRequest = new StatusRequestDelegate(SendStatus);
            remotePublisher.OnPublishRequest = new PubSubEventDelegate(HandlePublishEvent);
            channel = new TcpChannel(new Uri(address).Port);
            ChannelServices.RegisterChannel(channel, true);
            remoteBroker = (RemoteBroker) Activator.GetObject(typeof(RemoteBroker), brokerAddress);
            RemotingServices.Marshal(remotePublisher, new Uri(address).LocalPath.Split('/')[1]);
        }

        private void HandlePublishEvent(string topic, string message)
        {
            PublicationEvent e = new PublicationEvent(message, topic, remoteBroker.name);
            publishEvent(e);
        }

        public string SendStatus()
        {
            return "[Publisher - " + this.address + "] - Sequence number: " + SequenceNumber;
        }

        public void publishEvent(PublicationEvent e)
        {
            e.publisher = address;
            e.SequenceNumber = ++SequenceNumber;    // 
            //remoteBroker.Advertise(e.topic, address);
            remoteBroker.Flood(e);
        }
    }
}
