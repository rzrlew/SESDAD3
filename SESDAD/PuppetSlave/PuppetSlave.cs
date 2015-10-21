using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;

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
            channel = new TcpChannel(port);
            ChannelServices.RegisterChannel(channel, true);
            Console.WriteLine("Slave created Tcp Channel!");
            Console.WriteLine("Contacting PuppetMaster on: " + puppetMasterAddress);
            remotePuppetMaster = (PuppetMasterRemote)Activator.GetObject(typeof(PuppetMasterRemote), puppetMasterAddress);
            SESDADConfig config_bajoras = remotePuppetMaster.GetConfiguration(siteName);
            Console.WriteLine("Config-Bajoras: " + config_bajoras.SiteName);
            StartupConfiguration(remotePuppetMaster.GetConfiguration(siteName));
        }

        public SESDADBrokerConfig GetConfiguration()
        {
            SESDADBrokerConfig brokerConf = new SESDADBrokerConfig();
            foreach(SESDADProcessConfig config in configuration.ProcessConfigList)
            {
                if(config.ProcessType == "broker")
                {
                    brokerConf.parentBrokerAddress = config.ProcessParentAddress;
                    brokerConf.brokerAddress = config.ProcessAddress;
                    brokerConf.brokerName = config.ProcessName;
                }
            }
            brokerConf.childrenBrokerAddresses = configuration.ChildBrokersAddresses;
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