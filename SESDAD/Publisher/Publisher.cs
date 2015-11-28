using System;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting;
using System.Threading;
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
            Console.WriteLine("press to test fifo...");
            string input = Console.ReadLine();
            pub.testFIFO();
            Console.ReadLine();
        }

        public Publisher(string address, string brokerAddress)
        {
            this.address = address;
            this.brokerAddress = brokerAddress;
            remotePublisher = new RemotePublisher();
            remotePublisher.OnStatusRequest = new StatusRequestDelegate(SendStatus);
            remotePublisher.OnPublishRequest = new PublicationEventDelegate(HandlePublishEvent);
            channel = new TcpChannel(new Uri(address).Port);
            ChannelServices.RegisterChannel(channel, true);
            remoteBroker = (RemoteBroker) Activator.GetObject(typeof(RemoteBroker), brokerAddress);
            RemotingServices.Marshal(remotePublisher, new Uri(address).LocalPath.Split('/')[1]);
        }

        private void PublishEvent(PublicationEvent e)
        {
            lock(this)
            {
                e.publisher = address;
                e.SequenceNumber = ++SequenceNumber;
                remoteBroker.PublishEvent(e);
            }
        }

        private void HandlePublishEvent(string topic, int numEvents, int interval)
        {
            new Thread(() =>
            {
                Console.WriteLine("Sending " + numEvents + " events on topic " + topic);
                for (int i = 0; i < numEvents; i++)
                {
                    Console.WriteLine("Publishing event on topic '" + topic + "' sequence: " + SequenceNumber);
                    PublicationEvent e = new PublicationEvent(SequenceNumber.ToString(), topic, address);
                    PublishEvent(e);
                    Thread.Sleep(interval);
                }
            }).Start();
        }
        private string SendStatus()
        {
            string msg = "[Publisher - " + new Uri(this.address).LocalPath.Split('/')[1] + "] SeqNum: " + SequenceNumber;
            msg += " || Broker at:" + brokerAddress;
            return msg;
        }
        private void testFIFO()
        {
            for(int i = 5; i > 0; i--)
            {
                PublicationEvent e = new PublicationEvent(i.ToString(), "test", remoteBroker.name);
                e.SequenceNumber = i;
                e.publisher = address;
                remoteBroker.PublishEvent(e);
            }
        }

        
    }
}
