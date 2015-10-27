using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Collections;
using System.Runtime.Remoting.Messaging;
using System.Net;
using System.Net.Sockets;

namespace SESDAD
{
    class PuppetSlave
    {
        int port;
        TcpChannel channel;
        SESDADConfig configuration;
        RemotePuppetMaster remotePuppetMaster;
        RemotePuppetSlave remotePuppetSlave;
        public static void Main(string[] args)
        {
            if (args.Count() == 1)
            {
                Console.WriteLine("---Starting Slave---");
                PuppetSlave pptSlave = new PuppetSlave(args[0]); //arg[0] -> puppet master address
            }
            else
            {
                throw new NotImplementedException();
            }
            Console.ReadLine();
        }

        private IPAddress LocalIPAddress()
        {
            if (!System.Net.NetworkInformation.NetworkInterface.GetIsNetworkAvailable())
            {
                return null;
            }

            IPHostEntry host = Dns.GetHostEntry(Dns.GetHostName());
            return host.AddressList.FirstOrDefault(ip => ip.AddressFamily == AddressFamily.InterNetwork);
        }

        public PuppetSlave(string puppetMasterAddress)
        {
            Console.WriteLine("Getting configuration from PuppetMaster on: " + puppetMasterAddress);
            remotePuppetSlave = new RemotePuppetSlave();
            remotePuppetSlave.OnGetConfiguration += new SESDADBrokerConfiguration(GetConfiguration);
            TcpChannel temp_channel = new TcpChannel();
            ChannelServices.RegisterChannel(temp_channel, true);
            remotePuppetMaster = (RemotePuppetMaster) Activator.GetObject(typeof(RemotePuppetMaster), puppetMasterAddress);
            port = remotePuppetMaster.GetNextPortNumber();
            channel = new TcpChannel(port);
            ChannelServices.UnregisterChannel(temp_channel);
            ChannelServices.RegisterChannel(channel, true);
            Console.WriteLine(LocalIPAddress().ToString());
            Console.WriteLine("Slave created Tcp Channel on port: " + port);
            configuration = remotePuppetMaster.Register();
            RemotingServices.Marshal(remotePuppetSlave, configuration.SiteName + "slave");
            Console.WriteLine("Site name: " + configuration.SiteName);
            Console.WriteLine("Press <Enter> to get configuration from puppetmaster...");
            Console.ReadLine();
            StartupConfiguration();
            Console.WriteLine("Press <Enter> to exit...");
            Console.ReadLine();
        }

        public SESDADAbstractConfig GetConfiguration()
        {
            Console.WriteLine("Got a configuration request from broker.");
            
            foreach(SESDADProcessConfig config in configuration.ProcessConfigList)
            {
                switch (config.ProcessType)
                {
                    case "broker":
                        {
                            SESDADBrokerConfig brokerConf = new SESDADBrokerConfig();
                            brokerConf.parentBrokerAddress = config.ProcessParentAddress;
                            brokerConf.brokerAddress = config.ProcessAddress;
                            brokerConf.brokerName = config.ProcessName;
                            brokerConf.childrenBrokerAddresses = configuration.ChildBrokersAddresses;
                            return brokerConf;
                        }
                    case "subscriber":
                        {
                            SESDADPubSubConfig pubSubConf = new SESDADPubSubConfig();
                            pubSubConf.address = config.ProcessAddress;
                            pubSubConf.name = config.ProcessName;
                            pubSubConf.brokerAddress = config.ProcessParentAddress;
                            return pubSubConf;
                        }
                    case default(string):
                        {
                            throw new NotImplementedException();
                        }
                }
            }
            throw new NotImplementedException();
        }

        private void StartupConfiguration()
        {
            foreach (SESDADProcessConfig processConf in configuration.ProcessConfigList)
            {
                Console.WriteLine("Type: " + processConf.ProcessType);
                switch (processConf.ProcessType)
                {
                    case "broker":
                        {
                            Console.WriteLine("Starting broker process...");
                            Process.Start(TestConstants.brokerPath, "tcp://localhost:" + port + "/" + configuration.SiteName + "slave");
                            break;
                        }
                        
                    case "subscriber":
                        {
                            Console.WriteLine("Starting subscriber process...");
                            Process.Start(TestConstants.subscriberPath, processConf.ProcessAddress + " " + processConf.ProcessParentAddress);
                            break;
                        }
                        
                }
            }
        }
    }
}