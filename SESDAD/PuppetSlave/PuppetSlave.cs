﻿using System;
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
            remotePuppetMaster = (RemotePuppetMaster)Activator.GetObject(typeof(RemotePuppetMaster), puppetMasterAddress);
            port = remotePuppetMaster.GetNextPortNumber();
            ChannelServices.UnregisterChannel(temp_channel);
            channel = new TcpChannel(port);           
            ChannelServices.RegisterChannel(channel, true);
            configuration = remotePuppetMaster.RegisterSlave();
            StartupConfiguration(configuration);
        }

        public void SendLogMessage(string message)
        {
            remotePuppetMaster.LogMessage(message);
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
                    case "publisher":
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
                            Process.Start(TestConstants.brokerPath, "tcp://localhost:" + port + "/" + configuration.siteName + "slave");
                            SendLogMessage("Slave started on '" + configuration.siteName + "'!");
                            break;
                        }
                        
                    case "subscriber":
                        {
                            Console.WriteLine("Starting subscriber process...");
                            Process.Start(TestConstants.subscriberPath, processConf.processAddress + " " + processConf.processParentAddress);
                            SendLogMessage("Slave started subscriber on '" + configuration.siteName + "'!");
                            break;
                        }

                    case "publisher":
                        {
                            Console.WriteLine(processConf.processAddress + Environment.NewLine + processConf.processParentAddress);
                            Console.WriteLine("Starting publisher process...");
                            Process.Start(TestConstants.publisherPath, processConf.processAddress + " " + processConf.processParentAddress);
                            SendLogMessage("Slave started subscriber on '" + configuration.siteName + "'!");
                            break;
                        }
                }
            }
        }
    }
}