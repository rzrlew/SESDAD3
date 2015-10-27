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
            remotePuppetSlave.OnGetConfiguration += new SESDADBrokerConfiguration(GetConfiguration);
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
            Console.WriteLine("Site name: " + siteConfig.SiteName);
            StartupConfiguration(remotePuppetMaster.GetConfiguration(siteName));
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

        private void StartupConfiguration(SESDADConfig config)
        {
            configuration = config;
            RemotingServices.Marshal(remotePuppetSlave, config.SiteName + "slave");
            foreach (SESDADProcessConfig processConf in configuration.ProcessConfigList)
            {
                Console.WriteLine("Type: " + processConf.ProcessType);
                switch (processConf.ProcessType)
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
                            Process.Start(TestConstants.subscriberPath, processConf.ProcessAddress + " " + processConf.ProcessParentAddress);
                            // Process.Start(TestConstants.subscriberPath, "tcp://localhost:" + port + "/" + siteName + "slave");
                            break;
                        }
                        
                }
            }
        }
    }
}