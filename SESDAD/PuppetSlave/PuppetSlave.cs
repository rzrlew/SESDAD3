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
        PuppetMasterRemote remotePuppetMaster;
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
            remotePuppetMaster = (PuppetMasterRemote)Activator.GetObject(typeof(PuppetMasterRemote), puppetMasterAddress);
            SESDADConfig siteConfig = remotePuppetMaster.GetConfiguration(siteName);
            Console.WriteLine("Site name: " + siteConfig.SiteName);
            StartupConfiguration(remotePuppetMaster.GetConfiguration(siteName));
        }

        public SESDADBrokerConfig GetConfiguration()
        {
            Console.WriteLine("Got a configuration request from broker.");
            SESDADBrokerConfig brokerConf = new SESDADBrokerConfig();
            foreach(SESDADProcessConfig config in configuration.ProcessConfigList)
            {
                if(config.ProcessType == "broker")
                {
                    brokerConf.parentBrokerAddress = config.ProcessParentAddress;
                    brokerConf.brokerAddress = config.ProcessAddress;
                    brokerConf.brokerName = config.ProcessName;
                    break;
                }
            }
            brokerConf.childrenBrokerAddresses = configuration.ChildBrokersAddresses;
            Console.WriteLine("Sending broker " + brokerConf.brokerName + " it's configuration!");
            return brokerConf;
        }

        private void StartupConfiguration(SESDADConfig config)
        {
            configuration = config;
            RemotingServices.Marshal(remotePuppetSlave, config.SiteName + "slave");
            foreach (SESDADProcessConfig processConf in configuration.ProcessConfigList)
            {
                switch (processConf.ProcessType)
                {
                    case "broker":
                        Console.WriteLine("Starting broker process!");
                        Process.Start(TestConstants.brokerPath, "tcp://localhost:" + port + "/" + siteName + "slave");
                        break;
                }
            }
        }
    }
}