using System;
using System.Linq;
using System.Threading;
using System.Diagnostics;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;

namespace SESDAD
{
    class PuppetSlave
    {
        TcpChannel channel;
        int port;
        SESDADConfig configuration;
        RemotePuppetMaster remotePuppetMaster;
        RemotePuppetSlave remotePuppetSlave;
        private string puppetMasterAddress;
        public static void Main(string[] args)
        {
            if (args.Count() == 1)
            {
                Console.WriteLine("---Starting Slave---");
                PuppetSlave pptSlave = new PuppetSlave(args[0]);
            }
            else
            {
                throw new NotImplementedException("Number of arguments not expected!");
            }
            Console.ReadLine();
        }
        public PuppetSlave(string puppetMasterAddress)
        {
            Console.WriteLine("Contacting PuppetMaster on: " + puppetMasterAddress);
            remotePuppetSlave = new RemotePuppetSlave();
            remotePuppetSlave.OnGetConfiguration += new SESDADProcessConfigurationDelegate(GetConfiguration);
            remotePuppetSlave.OnLogMessage += new LogMessageDelegate(SendLogMessage);
            TcpChannel temp_channel = new TcpChannel();
            ChannelServices.RegisterChannel(temp_channel, true);
            this.puppetMasterAddress = puppetMasterAddress;
            remotePuppetMaster = (RemotePuppetMaster)Activator.GetObject(typeof(RemotePuppetMaster), puppetMasterAddress);
            try {
                port = remotePuppetMaster.GetNextPortNumber();
            }
            catch (SocketException)
            {
                Console.WriteLine("Could not connect to Puppet Master at address: " + puppetMasterAddress);
                Console.WriteLine("Press <Enter> to exit!");
                return;
            }

            ChannelServices.UnregisterChannel(temp_channel);
            channel = new TcpChannel(port);           
            ChannelServices.RegisterChannel(channel, true);
            try {
                configuration = remotePuppetMaster.RegisterSlave();
            }
            catch (NotImplementedException e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine("Press <Enter> to exit!");
                return;
            }
            StartupConfiguration(configuration);
        }
        public void SendLogMessage(string message)
        {
            new Thread(() =>{remotePuppetMaster.LogMessage(message);}).Start();
        }
        public SESDADProcessConfiguration GetConfiguration()
        {
            Console.WriteLine("Got a configuration request from broker.");           
            foreach(SESDADProcessConfig config in configuration.processConfigList)
            {
                switch (config.processType)
                {
                    case "broker":
                        SESDADBrokerConfig brokerConf = new SESDADBrokerConfig();
                        brokerConf.orderMode = configuration.orderMode;
                        brokerConf.routingPolicy = configuration.routingPolicy;
                        brokerConf.parentBrokerAddress = config.processParentAddress;
                        brokerConf.processAddress = config.processAddress;
                        brokerConf.processName = config.processName;
                        brokerConf.childrenBrokerAddresses = configuration.childBrokersAddresses;
                        return brokerConf;
                        
                    case "publisher":
                    case "subscriber":
                        SESDADPubSubConfig pubSubConf = new SESDADPubSubConfig();
                        pubSubConf.processAddress = config.processAddress;
                        pubSubConf.processName = config.processName;
                        pubSubConf.brokerAddress = config.processParentAddress;
                        return pubSubConf;

                    case default(string):
                        throw new NotImplementedException();                      
                }
            }
            throw new NotImplementedException();
        }
        private void StartupConfiguration(SESDADConfig config)
        {
            configuration = config;
            RemotingServices.Marshal(remotePuppetSlave, config.siteName + "slave");
            foreach (SESDADProcessConfig processConf in configuration.processConfigList)
            {
                Console.WriteLine("Type: " + processConf.processType);
                switch (processConf.processType)
                {
                    case "broker":
                        Console.WriteLine("Starting broker process...");
                        IPAddress[] localIPs = Dns.GetHostAddresses(Dns.GetHostName());
                        string ipAddress = "";
                        foreach (NetworkInterface nic in NetworkInterface.GetAllNetworkInterfaces())
                        {
                            if (nic.GetIPProperties().GatewayAddresses.Count() > 0)
                            {
                                foreach (UnicastIPAddressInformation addressInfo in nic.GetIPProperties().UnicastAddresses)
                                {
                                    if (addressInfo.Address.AddressFamily.Equals(AddressFamily.InterNetwork))
                                    {
                                        ipAddress = addressInfo.Address.ToString();
                                    }
                                }
                            }
                        }
                        Console.WriteLine("Slaves IP address is: " + ipAddress);
                        Process.Start(TestConstants.brokerPath, "tcp://" + ipAddress + ":" + port + "/" + configuration.siteName + "slave");
                        SendLogMessage("Slave started broker on '" + configuration.siteName + "'!");
                        break;

                    case "subscriber":
                        Console.WriteLine("Starting subscriber '" + processConf.processName + "' process linked with broker at " + processConf.processParentAddress);
                        Process.Start(TestConstants.subscriberPath, processConf.processAddress + " " + processConf.processParentAddress + " " + processConf.processName + " " + this.puppetMasterAddress);
                        SendLogMessage("Slave started subscriber on '" + configuration.siteName + "'!");
                        break;

                    case "publisher":
                        Console.WriteLine("Starting publisher process...");
                        Process.Start(TestConstants.publisherPath, processConf.processAddress + " " + processConf.processParentAddress);
                        SendLogMessage("Slave started publisher on '" + configuration.siteName + "'!");
                        break;

                }
            }
        }
    }
}