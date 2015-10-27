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


namespace SESDAD
{
    class PuppetSlave
    {
        TcpChannel channel;
        int port;
        SESDADConfig configuration;
        RemotePuppetMaster remotePuppetMaster;
        RemotePuppetSlave remotePuppetSlave;
        string siteName;
        public static void Main(string[] args)
        {
            if (args.Count() == 3)
            {
                Console.WriteLine("---Starting Slave---");
                PuppetSlave pptSlave = new PuppetSlave(args[0], args[1], args[2]);
            }
            else
            {
                throw new NotImplementedException();
            }
            Console.ReadLine();
        }

        public PuppetSlave(string puppetMasterAddress, string siteName, string portString)
        {
            this.siteName = siteName;
            remotePuppetSlave = new RemotePuppetSlave();
            remotePuppetSlave.OnGetConfiguration += new SESDADprocessConfiguration(GetConfiguration);
            port = int.Parse(portString);

            //To provide the ability to get the client ip address.
            BinaryServerFormatterSinkProvider bp = new BinaryServerFormatterSinkProvider();
            BinaryClientFormatterSinkProvider c_sink = new BinaryClientFormatterSinkProvider();
            ClientIPServerSinkProvider csp = new ClientIPServerSinkProvider();
            csp.Next = bp;
            Hashtable ht = new Hashtable();
            ht.Add("port", portString);
            channel = new TcpChannel(ht, c_sink, csp);
            ChannelServices.RegisterChannel(channel, true);

            Console.WriteLine("Slave created Tcp Channel on port: " + portString);
            Console.WriteLine("Contacting PuppetMaster on: " + puppetMasterAddress);
            remotePuppetMaster = (RemotePuppetMaster)Activator.GetObject(typeof(RemotePuppetMaster), puppetMasterAddress);
            SESDADConfig siteConfig = remotePuppetMaster.GetConfiguration(siteName);
            Console.WriteLine("Site name: " + siteConfig.siteName);
            StartupConfiguration(remotePuppetMaster.GetConfiguration(siteName));
        }

        public SESDADProcessConfiguration GetConfiguration()
        {
            Console.WriteLine("Got a configuration request from broker.");
            
            foreach(SESDADProcessConfig config in configuration.processConfigList)
            {
                switch (config.processType)
                {
                    case "broker":
                        {
                            SESDADBrokerConfig brokerConf = new SESDADBrokerConfig();
                            brokerConf.parentBrokerAddress = config.processParentAddress;
                            brokerConf.processAddress = config.processAddress;
                            brokerConf.processName = config.processName;
                            brokerConf.childrenBrokerAddresses = configuration.childBrokersAddresses;
                            return brokerConf;
                        }
                    case "subscriber":
                        {
                            SESDADPubSubConfig pubSubConf = new SESDADPubSubConfig();
                            pubSubConf.processAddress = config.processAddress;
                            pubSubConf.processName = config.processName;
                            pubSubConf.brokerAddress = config.processParentAddress;
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
                        {
                            Console.WriteLine("Starting broker process...");
                            Process.Start(TestConstants.brokerPath, "tcp://localhost:" + port + "/" + siteName + "slave");
                            break;
                        }
                        
                    case "subscriber":
                        {
                            Console.WriteLine("Starting subscriber process...");
                            Process.Start(TestConstants.subscriberPath, processConf.processAddress + " " + processConf.processParentAddress);
                            break;
                        }
                        
                }
            }
        }
    }
}