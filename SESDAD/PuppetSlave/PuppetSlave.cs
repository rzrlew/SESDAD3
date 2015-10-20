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
        SESDADConfig configuration;
        PuppetMasterRemote remotePuppetMaster;
        RemotePuppetSlave remotePuppetSlave;
        string siteName;
        public static void Main(string[] args)
        {
            if (args.Count() == 3)
            {
                Console.WriteLine("---Starting Slave---");
                PuppetSlave pptSlave = new PuppetSlave(args[1], args[2]);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public PuppetSlave(string puppetMasterAddress, string siteName)
        {
            this.siteName = siteName;
            remotePuppetSlave = new RemotePuppetSlave();
            remotePuppetSlave.OnGetConfiguration += new SESDADBrokerConfiguration(GetConfiguration);
            channel = new TcpChannel();
            ChannelServices.RegisterChannel(channel, true);
            Console.WriteLine("Slave created Tcp Channel on port: " + new Uri(((IChannelDataStore)channel.ChannelData).ChannelUris.Single()).Port);
            remotePuppetMaster = (PuppetMasterRemote)Activator.GetObject(typeof(PuppetMasterRemote), puppetMasterAddress);
            StartupConfiguration(remotePuppetMaster.GetConfiguration(siteName));
        }

        private SESDADConfig GetConfiguration()
        {
            return configuration;
        }

        private void StartupConfiguration(SESDADConfig config)
        {
            configuration = config;
            RemotingServices.Marshal(remotePuppetSlave, config.SiteName + "slave");
            Console.WriteLine("Remote Slave registered in address: " + ((IChannelDataStore)channel.ChannelData).ChannelUris.Single());
            foreach (string processString in configuration.ProcessList)
            {
                switch (processString)
                {
                    case "broker":
                        Process.Start(TestConstants.brokerPath + " " + config.SiteBrokerAddress);
                        break;
                }
            }
        }
    }
}