﻿using System;
using System.IO;
using System.Windows.Forms;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Diagnostics;
using System.Net.Sockets;
using System.Collections;
using System.Net;

namespace SESDAD
{
    public delegate void PuppetMasterFormEventDelegate(string msg);
    
    public class PuppetMaster
    {
        PuppetMasterForm form;
        IDictionary<string, string> processesAddressHash = new Dictionary<string, string>();
        List<SESDADConfig> configList = new List<SESDADConfig>();
        private List<string> toShowMessages = new List<string>();
        private int puppetMasterPort = 9000;

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        public static void Main(string[] args)
        {
            if (args.Count() < 2)
            {
                throw new NotImplementedException("Must indicate True or False and puppetmaster address in arguments!");
            }
            if (bool.Parse(args[0]))
            {
                Application.EnableVisualStyles();
                Application.SetCompatibleTextRenderingDefault(false);
                PuppetMaster puppetMaster = new PuppetMaster(new Uri(args[1]).Port);
                Process.Start(TestConstants.puppetSlavePath, args[1]);
                Application.Run(puppetMaster.form);
            }
            else
            {
                Process.Start(TestConstants.puppetSlavePath, args[1]);
            }
        }

        public PuppetMaster(int port)
        {
            puppetMasterPort = port;      
            form = new PuppetMasterForm();
            form.OnBajorasPrint += new PuppetMasterFormEventDelegate(ShowMessage);
            form.OnSingleCommand = new PuppetMasterFormEventDelegate(RunSingleCommand);
            form.OnScriptCommands = new PuppetMasterFormEventDelegate(RunScript);
            //--To receive the slave's IP address--
            BinaryServerFormatterSinkProvider bp = new BinaryServerFormatterSinkProvider();
            ClientIPServerSinkProvider csp = new ClientIPServerSinkProvider();
            csp.Next = bp;
            IDictionary ht = new Hashtable();
            ht.Add("port", puppetMasterPort);
            TcpChannel channel = new TcpChannel(ht, null, csp);
            //--////--
            ChannelServices.RegisterChannel(channel, true);
            RemotePuppetMaster remotePM = new RemotePuppetMaster();
            remotePM.slaveSignIn += new SESDADSlaveConfigurationDelegate(RegisterSlave);
            remotePM.configRequest += new SESDADconfigurationDelegate(SearchConfigList);
            remotePM.OnLogMessage += new PuppetMasterLogEventDelegate(ShowLog);
            RemotingServices.Marshal(remotePM, "puppetmaster");
            ParseConfigFile(TestConstants.configFilePath);
            PopulateAddressHash();
        }

        private void PopulateAddressHash()
        {
            foreach(SESDADConfig siteConfig in configList)
            {
                foreach(SESDADProcessConfig processConfig in siteConfig.processConfigList)
                {
                    processesAddressHash.Add(processConfig.processName, processConfig.processAddress);
                }
            }
        }
        private string ShowLog(string logMessage)
        {
            ShowMessage(logMessage);
            return "Message Logged!";
        }
        private SESDADConfig RegisterSlave(string ip_address)
        {
            lock (configList)
            {
                foreach (SESDADConfig config in configList)
                {
                    string configHost = new Uri(config.processConfigList.FirstOrDefault().processAddress).Host;
                    if (configHost.Equals("localhost"))
                    {
                        configHost = "127.0.0.1";
                    }
                    if (!config.isDone && configHost.Equals(ip_address))
                    {
                        config.isDone = true;
                        ShowMessage("Slave for '" + config.siteName + "' is registered!");
                        return config;
                    }
                }
            }
            throw new NotImplementedException("No configuration left for slave!");
        }
        private SESDADConfig SearchConfigList(string SiteName)  // returns the Config Class of specified Site
        {
            foreach(SESDADConfig c in configList)
            {
                if (c.siteName.Equals(SiteName))
                {
                    return c;
                }
            }
            throw new NotImplementedException();
        }
        private void ParseConfigFile(string filename)
        {
            StreamReader file = OpenConfigFile(filename);
            OrderMode order_mode = OrderMode.NoOrder;
            string routing_policy = "flooding";
            string log_level = "full";
            while (true)
            {
                string line = file.ReadLine();
                if (line == null) { break; }    // ends parse if end of file
                string[] args = line.Split(' ');
                switch (args[0])
                {
                    case "Site":
                        SESDADConfig conf = new SESDADConfig(args[1]);
                        conf.parentSiteName = args[3];
                        foreach (SESDADConfig c in configList)
                        {
                            if (c.siteName.Equals(args[3])) // search for Parent Site
                            {
                                c.childrenSiteNames.Add(conf.siteName); // add siteName to parent node child Name List 
                            }
                        }
                        configList.Add(conf);
                        break;

                    case "Process":
                        SESDADProcessConfig processConf = new SESDADProcessConfig();
                        processConf.processName = args[1];
                        processConf.processType = args[3];
                        processConf.processAddress = args[7];
                        switch (args[3])
                        {
                            case "broker":
                                string parentName = null;
                                SESDADConfig config = SearchConfigList(args[5]); // search for site Configuration using Site Name
                                parentName = config.parentSiteName;
                                config.processConfigList.Add(processConf); // adds Process Config to Site Configuration Class 
                                if (parentName != "none") // only needed if not Root Node
                                {
                                    SESDADConfig parentConf = SearchConfigList(config.parentSiteName); // Parent Configuration Class
                                    parentConf.childBrokersAddresses.Add(args[7]); // Add process address to parent list
                                    processConf.processParentAddress = parentConf.searchBroker().processAddress; // address of 'first' broker of Site
                                }
                                break;

                            case "publisher":
                            case "subscriber":
                                processConf.processParentAddress = SearchConfigList(args[5]).searchBroker().processAddress;
                                SearchConfigList(args[5]).processConfigList.Add(processConf);
                                break;

                        }
                        break;

                    case "Ordering":
                        switch (args[1])
                        {
                            case "NO":
                                order_mode = OrderMode.NoOrder;
                                break;

                            case "FIFO":
                                order_mode = OrderMode.FIFO;
                                break;

                            case "TOTAL":
                                order_mode = OrderMode.TotalOrder;
                                break;

                        }
                        break;
                    case "RoutingPolicy":
                        routing_policy = args[1];
                        break;

                    case "LoggingLevel":
                        log_level = args[1];
                        break;

                }
            }
            foreach (SESDADConfig config in configList)
            {
                config.orderMode = order_mode;
                config.routingPolicy = routing_policy;
                config.loggingLevel = log_level;
            }
            file.Close();
        }
        private void RunSingleCommand(string command)
        {
            string[] commandTokens = command.Split(' ');
            SESDADRemoteProcessControlInterface remoteProcess;
            switch (commandTokens[0])
            {               
                case "Status":
                    foreach (KeyValuePair<string, string> entry in processesAddressHash)
                    {
                        remoteProcess = (SESDADRemoteProcessControlInterface)Activator.GetObject(typeof(SESDADRemoteProcessControlInterface), entry.Value);
                        ShowMessage(remoteProcess.Status());
                    }
                    break;

                case "Freeze":
                    ShowMessage("Freezing process '" + commandTokens[1] + "' !");
                    remoteProcess = (SESDADRemoteProcessControlInterface)Activator.GetObject(typeof(SESDADRemoteProcessControlInterface), processesAddressHash[commandTokens[1]]);
                    remoteProcess.Freeze();
                    break;

                case "Unfreeze":
                    ShowMessage("Unfreezing process '" + commandTokens[1] + "' !");
                    remoteProcess = (SESDADRemoteProcessControlInterface)Activator.GetObject(typeof(SESDADRemoteProcessControlInterface), processesAddressHash[commandTokens[1]]);
                    remoteProcess.Unfreeze();
                    break;

                case "Crash":
                    ShowMessage("Crashing process '" + commandTokens[1] + "' !");
                    remoteProcess = (SESDADRemoteProcessControlInterface)Activator.GetObject(typeof(SESDADRemoteProcessControlInterface), processesAddressHash[commandTokens[1]]);
                    try {
                        remoteProcess.Crash();
                    }
                    catch (IOException)
                    {
                        ShowMessage("Process '" + commandTokens[1] + "' has crashed!");
                    }
                    break;

                case "Publisher":
                    ShowMessage("Asking publisher '" + commandTokens[1] + "' to publish on topic '" + commandTokens[5]  + "' !");
                    RemotePublisher remotePublisher = (RemotePublisher)Activator.GetObject(typeof(RemotePublisher), processesAddressHash[commandTokens[1]]);
                    remotePublisher.Publish(commandTokens[5], int.Parse(commandTokens[3]), int.Parse(commandTokens[7]));
                    break;

                case "Subscriber":
                    RemoteSubscriber remoteSubscriber = (RemoteSubscriber)Activator.GetObject(typeof(RemoteSubscriber), processesAddressHash[commandTokens[1]]);
                    switch (commandTokens[2])
                    {
                        case "Subscribe":
                            ShowMessage("Asking subscriber " + commandTokens[1] + " to subscribe topic " + commandTokens[3]);
                            remoteSubscriber.Subscribe(commandTokens[3]);
                            break;

                        case "Unsubscribe":
                            ShowMessage("ASking subscriber " + commandTokens[1] + " to unsubscribe topic " + commandTokens[3]);
                            remoteSubscriber.Unsubscribe(commandTokens[3]);
                            break;
                    }
                    break;
                                 
                case "Wait":
                    Console.WriteLine("Sleeping for " + commandTokens[1] + " milliseconds!");
                    Thread.Sleep(int.Parse(commandTokens[1]));
                    break;

            }
        }
        private void RunScript(string script)
        {
            string[] lines = script.Split(Environment.NewLine.ToCharArray());
            foreach(string line in lines)
            {
                RunSingleCommand(line);
            }
        }
        private StreamReader OpenConfigFile(string fileName)
        {
            return new StreamReader(File.Open(fileName, FileMode.Open));
        }
        private void ShowMessage(string msg)    // print given string in puppet master form
        {
            object[] arguments = new object[1];
            arguments[0] = "[" + DateTime.Now.Hour + ":" + DateTime.Now.Minute + ":" + DateTime.Now.Second + "] " + msg + Environment.NewLine;
            form.Invoke(new PuppetMasterFormEventDelegate(form.appendToOutputWindow), arguments);
        }
    }
}
