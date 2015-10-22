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

namespace SESDAD
{
    public delegate void PuppetMasterFormEvent(string msg);
    
    public class PuppetMaster
    {
        ObjRef remoteMasterRef;
        Tree SESDADTree;
        PuppetMasterForm form;
        IDictionary<string, string> brokerHash = new Dictionary<string, string>();
        List<SESDADConfig> configList = new List<SESDADConfig>();
        private int puppetMasterPort = 9000;
        private int slavePortCounter = 5000;

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        public static void Main()
        {
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            PuppetMaster puppetMaster = new PuppetMaster();
            puppetMaster.BootStrapSystem();
            Application.Run(puppetMaster.form);
        }

        public PuppetMaster()
        {
            form = new PuppetMasterForm();
            form.OnBajorasPrint += new PuppetMasterFormEvent(ShowRemoteMasterRef);
            SESDADTree = new Tree();
            TcpChannel channel = new TcpChannel(puppetMasterPort);
            ChannelServices.RegisterChannel(channel, true);
            PuppetMasterRemote remotePM = new PuppetMasterRemote();
            remotePM.brokerSignIn += new PuppetMasterEvent(RegisterBroker);
            remotePM.configRequest += new SESDADconfiguration(searchConfigList);
            ///RemotingConfiguration.RegisterWellKnownServiceType(typeof(PuppetMasterRemote), "puppetmaster", WellKnownObjectMode.Singleton);
            RemotingServices.Marshal(remotePM, "puppetmaster");
            parseConfigFile(TestConstants.configFilePath);
        }

        private void BootStrapSystem()
        {
            foreach(SESDADConfig slaveConfig in configList)
            {
                int nextSlavePort = ++slavePortCounter + 1000;
                Console.WriteLine("Starting slave on port: " + nextSlavePort);
                Process.Start(TestConstants.puppetSlavePath, "tcp://localhost:" + puppetMasterPort + "/puppetmaster " + slaveConfig.SiteName + " " +  nextSlavePort);
            }
        }

        private void ShowRemoteMasterRef(string bajoras)
        {
            ShowMessage(remoteMasterRef.URI);
        }

        private SESDADConfig searchConfigList(string SiteName)
        {
            foreach(SESDADConfig c in configList)
            {
                if (c.SiteName.Equals(SiteName))
                {
                    return c;
                }
            }
            throw new NotImplementedException();
        }

        public void parseConfigFile(string filename)
        {
            StreamReader file = openConfigFile(filename);
            
            while (true)
            {
                string line = file.ReadLine();
                if (line == null) { break; }
                string[] args = line.Split(' ');
                switch (args[0])
                {
                    case "Site":
                        {
                            SESDADConfig conf = new SESDADConfig(args[1]);
                            conf.ParentSiteName = args[3];
                            // SESDADConfig ParentConfig = searchConfigList(args[3]);
                            // ParentConfig.ChildrenSiteNames.Add(conf.SiteName);
                            foreach (SESDADConfig c in configList)
                            {
                                if (c.SiteName.Equals(args[3])) // search for Parent Site
                                {
                                    c.ChildrenSiteNames.Add(conf.SiteName); // add siteName to parent node childList 
                                }
                            }
                            configList.Add(conf);
                            break;
                        }

                    case "Process":
                        {
                            string parentName = null;
                            //SESDADConfig conf = null;
                            SESDADProcessConfig ProcessConf = new SESDADProcessConfig();
                            ProcessConf.ProcessName = args[1];
                            ProcessConf.ProcessType = args[3];
                            ProcessConf.ProcessAddress = args[7];

                            SESDADConfig conf = searchConfigList(args[5]);
                            parentName = conf.ParentSiteName;
                            conf.ProcessConfigList.Add(ProcessConf);

                            if (parentName != "none") // only needed if not Root Node
                            {
                                SESDADConfig parentConf = searchConfigList(conf.ParentSiteName); // Parent Configuration Class
                                parentConf.ChildBrokersAddresses.Add(args[7]); // Add process address to parent list
                                ProcessConf.ProcessParentAddress = parentConf.searchBroker().ProcessAddress;
                            }
                            break;
                        }
                }
            }
        }

        private StreamReader openConfigFile(string fileName)
        {
            return new StreamReader(File.Open(fileName, FileMode.Open));
        }

        private Site GetNextSite(StreamReader file)
        {
            string line = file.ReadLine();
            char[] siteName = new char[10];
            line.CopyTo(line.LastIndexOf("Site") + 2, siteName, 0, 5);
            Site site = new Site();
            site.siteConfig.SiteName = siteName.ToString();

            return site;
        }

        string RegisterBroker(PuppetMasterEventArgs args)
        {
            ShowMessage("Broker at \"" + args.address + "\" signing in!");
            string brokerName = "broker" + brokerHash.Count.ToString();
            brokerHash.Add(brokerName , args.address);
            return brokerName;
        }

        void ShowMessage(string msg)
        {
            object[] arguments = new object[1];
            arguments[0] = msg + Environment.NewLine;
            form.Invoke(new PuppetMasterFormEvent(form.appendToOutputWindow), arguments);
        }
    }

    public class Tree
    {
        Site rootNode = null;
        public void CreateNextNode()
        {

        }
    }

    class Site
    {
        List<Site> children;
        Site parent;
        public SESDADConfig siteConfig;

        public List<Site> Children
        {
            get{return children;}
            set{children = value;}
        }

        public Site Parent
        {
            get{return parent;}
            set{parent = value;}
        }
    }

    public class ConfigurationParser
    {
        StreamReader file;

        public ConfigurationParser(string fileName)
        {
            file = new StreamReader(File.Open(fileName, FileMode.Open));
        }

        public Tree BuildInfoTree()
        {
            Tree tree = new Tree();
            ///First Setup Phase

            return tree;
        }

        private Site GetNextSite()
        {
            string line = file.ReadLine();
            char[] siteName = new char[10];
            line.CopyTo(line.LastIndexOf("Site") + 2, siteName, 0, 5);
            Site site = new Site();
            site.siteConfig.SiteName = siteName.ToString();

            return site;
        }
    }

    abstract class Worker
    {
        string name;
        string url;
    }
}
