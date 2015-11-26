namespace SESDAD
{
    partial class PuppetMasterForm
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.outputWindow = new System.Windows.Forms.TextBox();
            this.saveLogButton = new System.Windows.Forms.Button();
            this.single_command_box = new System.Windows.Forms.TextBox();
            this.single_command_button = new System.Windows.Forms.Button();
            this.single_command_group_box = new System.Windows.Forms.GroupBox();
            this.script_text_box = new System.Windows.Forms.TextBox();
            this.script_group_box = new System.Windows.Forms.GroupBox();
            this.script_run_button = new System.Windows.Forms.Button();
            this.logGroupBox = new System.Windows.Forms.GroupBox();
            this.single_command_group_box.SuspendLayout();
            this.script_group_box.SuspendLayout();
            this.logGroupBox.SuspendLayout();
            this.SuspendLayout();
            // 
            // outputWindow
            // 
            this.outputWindow.BackColor = System.Drawing.SystemColors.MenuText;
            this.outputWindow.ForeColor = System.Drawing.Color.LawnGreen;
            this.outputWindow.Location = new System.Drawing.Point(6, 19);
            this.outputWindow.Multiline = true;
            this.outputWindow.Name = "outputWindow";
            this.outputWindow.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.outputWindow.Size = new System.Drawing.Size(757, 559);
            this.outputWindow.TabIndex = 0;
            // 
            // saveLogButton
            // 
            this.saveLogButton.Location = new System.Drawing.Point(6, 580);
            this.saveLogButton.Name = "saveLogButton";
            this.saveLogButton.Size = new System.Drawing.Size(75, 22);
            this.saveLogButton.TabIndex = 1;
            this.saveLogButton.Text = "Save Log";
            this.saveLogButton.UseVisualStyleBackColor = true;
            this.saveLogButton.Click += new System.EventHandler(this.saveLogButton_Click);
            // 
            // single_command_box
            // 
            this.single_command_box.Location = new System.Drawing.Point(5, 19);
            this.single_command_box.Name = "single_command_box";
            this.single_command_box.Size = new System.Drawing.Size(286, 20);
            this.single_command_box.TabIndex = 2;
            // 
            // single_command_button
            // 
            this.single_command_button.Location = new System.Drawing.Point(249, 45);
            this.single_command_button.Name = "single_command_button";
            this.single_command_button.Size = new System.Drawing.Size(43, 23);
            this.single_command_button.TabIndex = 3;
            this.single_command_button.Text = "Run";
            this.single_command_button.UseVisualStyleBackColor = true;
            this.single_command_button.Click += new System.EventHandler(this.single_command_button_Click);
            // 
            // single_command_group_box
            // 
            this.single_command_group_box.Controls.Add(this.single_command_button);
            this.single_command_group_box.Controls.Add(this.single_command_box);
            this.single_command_group_box.Location = new System.Drawing.Point(797, 12);
            this.single_command_group_box.Name = "single_command_group_box";
            this.single_command_group_box.Size = new System.Drawing.Size(298, 73);
            this.single_command_group_box.TabIndex = 4;
            this.single_command_group_box.TabStop = false;
            this.single_command_group_box.Text = "Single Command";
            // 
            // script_text_box
            // 
            this.script_text_box.Location = new System.Drawing.Point(6, 19);
            this.script_text_box.Multiline = true;
            this.script_text_box.Name = "script_text_box";
            this.script_text_box.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.script_text_box.Size = new System.Drawing.Size(285, 368);
            this.script_text_box.TabIndex = 5;
            // 
            // script_group_box
            // 
            this.script_group_box.Controls.Add(this.script_run_button);
            this.script_group_box.Controls.Add(this.script_text_box);
            this.script_group_box.Location = new System.Drawing.Point(797, 91);
            this.script_group_box.Name = "script_group_box";
            this.script_group_box.Size = new System.Drawing.Size(298, 422);
            this.script_group_box.TabIndex = 6;
            this.script_group_box.TabStop = false;
            this.script_group_box.Text = "Script";
            // 
            // script_run_button
            // 
            this.script_run_button.Location = new System.Drawing.Point(218, 393);
            this.script_run_button.Name = "script_run_button";
            this.script_run_button.Size = new System.Drawing.Size(73, 23);
            this.script_run_button.TabIndex = 4;
            this.script_run_button.Text = "Run Script";
            this.script_run_button.UseVisualStyleBackColor = true;
            this.script_run_button.Click += new System.EventHandler(this.script_run_button_Click);
            // 
            // logGroupBox
            // 
            this.logGroupBox.Controls.Add(this.saveLogButton);
            this.logGroupBox.Controls.Add(this.outputWindow);
            this.logGroupBox.Location = new System.Drawing.Point(6, 9);
            this.logGroupBox.Name = "logGroupBox";
            this.logGroupBox.Size = new System.Drawing.Size(769, 608);
            this.logGroupBox.TabIndex = 7;
            this.logGroupBox.TabStop = false;
            this.logGroupBox.Text = "Log";
            this.logGroupBox.Enter += new System.EventHandler(this.groupBox1_Enter);
            // 
            // PuppetMasterForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1107, 623);
            this.Controls.Add(this.script_group_box);
            this.Controls.Add(this.single_command_group_box);
            this.Controls.Add(this.logGroupBox);
            this.Name = "PuppetMasterForm";
            this.Text = "PuppetMasterForm";
            this.single_command_group_box.ResumeLayout(false);
            this.single_command_group_box.PerformLayout();
            this.script_group_box.ResumeLayout(false);
            this.script_group_box.PerformLayout();
            this.logGroupBox.ResumeLayout(false);
            this.logGroupBox.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.TextBox outputWindow;
        private System.Windows.Forms.Button saveLogButton;
        private System.Windows.Forms.TextBox single_command_box;
        private System.Windows.Forms.Button single_command_button;
        private System.Windows.Forms.GroupBox single_command_group_box;
        private System.Windows.Forms.TextBox script_text_box;
        private System.Windows.Forms.GroupBox script_group_box;
        private System.Windows.Forms.Button script_run_button;
        private System.Windows.Forms.GroupBox logGroupBox;
    }
}