using System;
using System.IO;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace SESDAD
{
    public partial class PuppetMasterForm : Form
    {
        public PuppetMasterFormEventDelegate OnBajorasPrint;
        public PuppetMasterFormEventDelegate OnSingleCommand;
        public PuppetMasterFormEventDelegate OnScriptCommands;
        public PuppetMasterForm()
        {
            InitializeComponent();
            appendToOutputWindow("---Puppet Master Starting---" + Environment.NewLine);
        }

        public void appendToOutputWindow(string message)
        {
            outputWindow.AppendText(message);
        }

        private void saveLogButton_Click(object sender, EventArgs e)
        {
            SaveFileDialog saveFileDialog1 = new SaveFileDialog();
            saveFileDialog1.Filter = "Text Document|*.txt";
            saveFileDialog1.Title = "Save the PuppetMaster log messages";
            saveFileDialog1.ShowDialog();
            // If the file name is not an empty string open it for saving.
            if (saveFileDialog1.FileName != "")
            {
                StreamWriter fs = new StreamWriter(saveFileDialog1.OpenFile());
                string logText = outputWindow.Text;
                fs.WriteLine(logText);
                fs.Close();
            }
        }

        private void single_command_button_Click(object sender, EventArgs e)
        {
            new Thread(() => OnSingleCommand(single_command_box.Text)).Start();
        }

        private void script_run_button_Click(object sender, EventArgs e)
        {
            new Thread(() => OnScriptCommands(script_text_box.Text)).Start();
        }

        private void groupBox1_Enter(object sender, EventArgs e)
        {

        }

        private void saveFileDialog1_FileOk(object sender, CancelEventArgs e)
        {

        }

        private void single_command_box_PreviewKeyDown(object sender, PreviewKeyDownEventArgs e)
        {
            if (e.KeyCode.Equals(Keys.Enter))
            {
                new Thread(() => OnSingleCommand(single_command_box.Text)).Start();
            }
        }
    }
}
