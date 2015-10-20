using System;
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

namespace SESDAD
{
    public delegate void PuppetMasterFormEvent(string msg);
    
    public class PuppetMaster
    {
        Tree SESDADTree;
        PuppetMasterForm form;
        IDictionary<string, string> brokerHash = new Dictionary<string, string>();
        List<SESDADConfig> configList = new List<SESDADConfig>();
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        public static void Main()
        {
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            PuppetMaster puppetMaster = new PuppetMaster();
            Application.Run(puppetMaster.form);
        }

        public PuppetMaster()
        {
            form = new PuppetMasterForm();
            SESDADTree = new Tree();
            TcpChannel channel = new TcpChannel(9000);
            ChannelServices.RegisterChannel(channel, true);
            PuppetMasterRemote remotePM = new PuppetMasterRemote();
            remotePM.brokerSignIn += new PuppetMasterEvent(RegisterBroker);
            RemotingServices.Marshal(remotePM, "puppetmaster");
            new Thread(new ThreadStart(RegisteringLoop)).Start();
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
                if (line.Count() == 0) { break; }
                string[] args = line.Split(' ');
                switch (args[0])
                {
                    case "Site":
                        {
                            SESDADConfig conf = new SESDADConfig(args[1]);
                            conf.ParentSiteName = args[3];
                            foreach (SESDADConfig c in configList)
                            {
                                if (c.SiteName.Equals(args[3]))
                                {
                                    c.ChildrenSiteNames.Add(conf.SiteName);
                                }
                            }
                            configList.Add(conf);
                            break;
                        }

                    case "Process":
                        {
                            SESDADProcessConfig ProcessConf = new SESDADProcessConfig();
                            ProcessConf.ProcessName = args[1];
                            ProcessConf.ProcessType = args[3];
                            ProcessConf.ProcessAddress = args[7];

                            SESDADConfig conf = searchConfigList(args[5]); 
                            conf.ProcessConfigList.Add(ProcessConf);
                            if (conf.ParentSiteName != "none") // only needed if not Root Node
                            {
                                SESDADConfig parentConf = searchConfigList(conf.ParentSiteName); // Parent Configuration Class
                                parentConf.ChildBrokersAddresses.Add(args[7]); // Add process address to parent list
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

        public void SetBrokerParent(string brokerAddress, string parentAddress)
        {
            RemoteBroker broker = (RemoteBroker) Activator.GetObject(typeof(RemoteBroker), brokerAddress);
            broker.SetParent(parentAddress);
        }

        public void SetBrokerChildren(string brokerAddress, string[] childrenAddresses)
        {
            RemoteBroker broker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), brokerAddress);
            broker.SetChildren(childrenAddresses.ToList<string>());
        }

        private void RegisteringLoop()
        {
            ShowMessage("Running Registering Loop");
            while(true)
            {
                if(brokerHash.Count == 2)
                {
                    ShowMessage("Setting broker connections!");
                    RemoteBroker broker = (RemoteBroker) Activator.GetObject(typeof(RemoteBroker), brokerHash["broker0"]);
                    List<string> addressList = new List<string>();
                    addressList.Add(brokerHash["broker1"]);
                    broker.SetChildren(addressList);
                    broker = (RemoteBroker)Activator.GetObject(typeof(RemoteBroker), brokerHash["broker1"]);
                    broker.SetParent(brokerHash["broker0"]);
                    break;
                }
            }
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
