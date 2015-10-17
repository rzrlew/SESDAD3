using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace SESDAD
{
    public partial class PuppetMasterForm : Form
    {
        Tree SESDADTree;
        public PuppetMasterForm()
        {
            InitializeComponent();
            appendToOutputWindow("---Puppet Master Starting---" + Environment.NewLine);
        }

        public void appendToOutputWindow(string message)
        {
            outputWindow.AppendText(message);
        }
    }
}
