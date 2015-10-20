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
            this.bajorasPrintButton = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // outputWindow
            // 
            this.outputWindow.BackColor = System.Drawing.SystemColors.MenuText;
            this.outputWindow.ForeColor = System.Drawing.Color.LawnGreen;
            this.outputWindow.Location = new System.Drawing.Point(5, 5);
            this.outputWindow.Multiline = true;
            this.outputWindow.Name = "outputWindow";
            this.outputWindow.Size = new System.Drawing.Size(267, 612);
            this.outputWindow.TabIndex = 0;
            // 
            // bajorasPrintButton
            // 
            this.bajorasPrintButton.Location = new System.Drawing.Point(278, 12);
            this.bajorasPrintButton.Name = "bajorasPrintButton";
            this.bajorasPrintButton.Size = new System.Drawing.Size(75, 23);
            this.bajorasPrintButton.TabIndex = 1;
            this.bajorasPrintButton.Text = "Bajoras Print";
            this.bajorasPrintButton.UseVisualStyleBackColor = true;
            this.bajorasPrintButton.Click += new System.EventHandler(this.bajorasPrintButton_Click);
            // 
            // PuppetMasterForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(659, 619);
            this.Controls.Add(this.bajorasPrintButton);
            this.Controls.Add(this.outputWindow);
            this.Name = "PuppetMasterForm";
            this.Text = "PuppetMasterForm";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox outputWindow;
        private System.Windows.Forms.Button bajorasPrintButton;
    }
}