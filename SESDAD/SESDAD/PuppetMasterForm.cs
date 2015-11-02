﻿using System;
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
        public PuppetMasterFormEvent OnBajorasPrint;
        public PuppetMasterFormEvent OnSingleCommand;
        public PuppetMasterForm()
        {
            InitializeComponent();
            appendToOutputWindow("---Puppet Master Starting---" + Environment.NewLine);
        }

        public void appendToOutputWindow(string message)
        {
            outputWindow.AppendText(message);
        }

        private void bajorasPrintButton_Click(object sender, EventArgs e)
        {
            OnBajorasPrint("");
        }

        private void single_command_button_Click(object sender, EventArgs e)
        {
            new Thread(() => OnSingleCommand(single_command_box.Text)).Start();
        }
    }
}
