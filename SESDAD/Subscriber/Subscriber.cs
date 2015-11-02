using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting;
using SESDAD;

namespace SESDADSubscriber
{
    class Subscriber
    {
        public List<string> topicList = new List<string>();
        public RemoteBroker serviceBroker;
        public RemoteSubsriber remoteSubscriber;
        public string address;
        static void Main(string[] args)
        {
            string input = "";
            Subscriber subs = new Subscriber(args[0], args[1]);  // arg[0] -> susbscriber address || arg[1] -> broker address
            while (!input.Equals("quit"))
            {
                Console.WriteLine(  "write [topic] you wish to Subscribe..." 
                                    + Environment.NewLine + "write [unsubscribe] to remove subscription of events...");
                input = Console.ReadLine();        
                if (input.Equals("unsubscribe"))
                {
                    Console.WriteLine("insert [topic] to unsubscribe...");
                    string topic = Console.ReadLine();
                    subs.Unsubscribe(topic);
                }
                else
                {
                    subs.Subscribe(input);
                }  
                
            }
        }

        public Subscriber(string address, string broker_address)
        {
            this.address = address;
            TcpChannel channel = new TcpChannel(new Uri(address).Port);//Creates the tcp channel on the port given in the config file....
            ChannelServices.RegisterChannel(channel, true);
            remoteSubscriber = new RemoteSubsriber();
            remoteSubscriber.OnNotifySubscription += new NotifyEvent(ShowEvent);
            serviceBroker = (RemoteBroker) Activator.GetObject(typeof(RemoteBroker), broker_address);
            string[] args = address.Split(':');
            string[] portAndName = args[2].Split('/');
            RemotingServices.Marshal(remoteSubscriber, portAndName[1]);
        }
        public void ShowEvent(Event e)
        {
            Console.WriteLine("Receiving Subscription Event..." + Environment.NewLine + e.Message());
        }

        public void Subscribe(string topic)
        {
            serviceBroker.Subscribe(topic, address);
        }

        public void Unsubscribe(string topic)
        {
            serviceBroker.UnSubscribe(topic, address);
        }
    }
}
