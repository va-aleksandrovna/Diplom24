using FastReport;
using FastReport.Data;
using FastReport.Export.PdfSimple;
using FastReport.Export.Image;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Windows.Forms;

namespace Fart_new
{
    public partial class Form1 : Form
    {
        public static string Server2 = @"127.0.0.1"; // PostgreSQL Server
        public static string Catalog2 = "aissng"; // PostgreSQL имя БД
        public static string Username2 = @"postgres"; // PostgreSQL имя пользователя
        public static string Password2 = @"123"; // PostgreSQL пароль
        public static int DatabasePort2 = 5432; // PostgreSQL порт БД

        public List<string> otch_sved = new List<string>() { "101", "102", "103", "104", "105", "106" };

        public Form1()
        {
            InitializeComponent();

            Start_Form();
        }

        private void Start_Form()
        {
            button1.Image = Properties.Resources.button_play_15000;
            treeView1.SelectedNode = null;
            label1.Visible = true;
            radioButton1.Visible = true;
            radioButton2.Visible = true;
            radioButton3.Visible = true;
            radioButton1.Checked = false;
            radioButton2.Checked = false;
            radioButton3.Checked = false;
            label2.Text = DateTime.Now.ToShortDateString();
            textBox1.Text = DateTime.Now.Year.ToString();
            textBox1.Visible = false;
            button2.Visible = false;
            button3.Visible = false;
        }

        private void treeView1_AfterSelect(object sender, TreeViewEventArgs e) // список отчетов
        {
            if (treeView1.SelectedNode.Parent != null && otch_sved.Contains(treeView1.SelectedNode.Name))
            {
                label1.Visible = false;
                radioButton1.Visible = false;
                radioButton2.Visible = false;
                radioButton3.Visible = false;
                textBox1.Visible = true;
                button2.Visible = true;
                button3.Visible = true;
            }
            //////////if другой список отчетов
            else
            {
                Start_Form();
            }
        }

        //изменение текста в textBox1(год)
        //можновводить только цифры
        private void textBox1_KeyPress(object sender, KeyPressEventArgs e) 
        {
            if (!char.IsControl(e.KeyChar) && !char.IsDigit(e.KeyChar))
            {
                e.Handled = true;
            }
        }

        private void button2_Click(object sender, EventArgs e) //кнопка +
        {
            if (int.TryParse(textBox1.Text, out int year))
            {
                textBox1.Text = (year + 1).ToString();
            }
        }

        private void button3_Click(object sender, EventArgs e) // кнопка -
        {
            if (int.TryParse(textBox1.Text, out int year))
            {
                textBox1.Text = (year - 1).ToString();
            }
        }

        private bool TimeInterval_oneyear_check() // проверка правильности заполнения textBox1(год)
        {
            if (!int.TryParse(textBox1.Text, out int year) || year < 2011 || year > DateTime.Now.Year)
            {
                MessageBox.Show("Пожалуйста, введите значение года в диапазоне от 2011 до текущего года");
                return false;
            }
            else
            {
                return true;
            }
        }

        private void button1_Click(object sender, EventArgs e) // кнопка запуска отчета
        {
            button1.Image = Properties.Resources.button_blank_yellow_14988;
            if (treeView1.SelectedNode!=null && otch_sved.Contains(treeView1.SelectedNode.Name))
            {
                bool result = TimeInterval_oneyear_check();

                if (result== true)
                {
                    report();
                    Viewer_pdf();
                    Start_Form();
                }
            }

            ///if какой другой список отчетов then
            
            button1.Image = Properties.Resources.button_play_15000;
        }

        private void Viewer_pdf() // предпросмотр отчета в формате pdf
        {
            try
            {
                string filep = @"..\report.pdf";
                Process myProcess = System.Diagnostics.Process.Start(filep);
                myProcess.WaitForExit();

                DialogResult result2 = MessageBox.Show("Сохранить файл?", "Сохранение...",
                    MessageBoxButtons.YesNo, MessageBoxIcon.Question);
                myProcess.Close();
                if (result2 == DialogResult.Yes)
                {
                    SaveFileDialog saveFileDialog1 = new SaveFileDialog();
                    saveFileDialog1.InitialDirectory = @"D:\";
                    saveFileDialog1.Title = "Save the PDF Files";
                    saveFileDialog1.DefaultExt = ".pdf";
                    int.TryParse(textBox1.Text, out int year);
                    saveFileDialog1.FileName = "report_" + treeView1.SelectedNode.Name + "_" + year;
                    saveFileDialog1.Filter = "PDF files (*.pdf)|*.pdf|All files (*.*)|*.*";
                    saveFileDialog1.FilterIndex = 1;
                    saveFileDialog1.RestoreDirectory = true;
                    if (saveFileDialog1.ShowDialog() == DialogResult.OK)
                    {
                        File.Copy(filep, saveFileDialog1.FileName);
                    }
                    MessageBox.Show("Файл сохранен");
                }
                File.Delete(filep);
            }
            catch (Exception exp)
            {
                MessageBox.Show(exp.Message);
            }
        }

        private void report() // запуск подготовки отчетов
        {
            Report report = new Report();

            FastReport.Utils.RegisteredObjects.AddConnection(typeof(PostgresDataConnection));
            PostgresDataConnection conn = new PostgresDataConnection();
            string connectionString = string.Format("Server={0};Port={1};User Id={2};Password={3};Database={4}", Server2, DatabasePort2, Username2, Password2, Catalog2);
            conn.ConnectionString = connectionString;
            report.Dictionary.Connections.Add(conn);
            conn.CreateAllTables();

            foreach (FastReport.Data.DataConnectionBase item in report.Dictionary.Connections)
            {
                item.ConnectionString = connectionString;
            }

            string s = "Fart_new.in.c" + treeView1.SelectedNode.Name + ".frx";
            //report.Load("..\\in\\c"+s+".frx");
            var asr = Assembly.GetExecutingAssembly();
            report.Load(asr.GetManifestResourceStream(s));

            int.TryParse(textBox1.Text, out int year);
            report.SetParameterValue("Parameter", year);

            typeof(Microsoft.CSharp.RuntimeBinder.RuntimeBinderException).GetType();
            report.Prepare();

            // сохраняем для предпросмотра
            string filep = @"..\report.pdf";
            PDFSimpleExport export = new PDFSimpleExport();
            report.Export(export, filep);
        }

        //private void Save_pdf(Report report)
        //{
        //    string filep = @"..\report.pdf";//////////
        //    FastReport.Export.PdfSimple.PDFSimpleExport export = new FastReport.Export.PdfSimple.PDFSimpleExport();
        //    report.Export(export, filep);
        //}
        //private void Save_html(Report report)
        //{
        //    // Export in Jpeg
        //    ImageExport image = new ImageExport();
        //    image.ImageFormat = ImageExportFormat.Jpeg;
        //    // Set up the quality
        //    image.JpegQuality = 90;
        //    // Decrease a resolution
        //    image.Resolution = 72;
        //    // We need all pages in one big single file
        //    image.SeparateFiles = false;
        //    report.Export(image, "C:\\Users\\DNS\\Desktop\\report.jpg");
        //    //report.Dispose();
        //}
        //private void Save_jpg(Report report)
        //{
        //    FastReport.Export.Html.HTMLExport export2 = new FastReport.Export.Html.HTMLExport();
        //    // показываем диалог с настройками экспорта и экспортируем отчет
        //    export2.Export(report, "C:\\Users\\DNS\\Desktop\\result.html");
        //}

        private void radioButton_CheckedChanged(object sender, EventArgs e)
        {
            if (radioButton1.Checked)
            {
                label1.Visible = true;
                radioButton1.Visible = true;
                radioButton2.Visible = true;
                radioButton3.Visible = true;
                textBox1.Visible = true;
                button2.Visible = true;
                button3.Visible = true;
            }
            if (radioButton2.Checked)
            {
                label1.Visible = true;
                radioButton1.Visible = true;
                radioButton2.Visible = true;
                radioButton3.Visible = true;
                //textBox1.Visible = true;
                //button2.Visible = true;
                //button3.Visible = true;
            }
            if (radioButton3.Checked)
            {
                label1.Visible = true;
                radioButton1.Visible = true;
                radioButton2.Visible = true;
                radioButton3.Visible = true;
                //textBox1.Visible = true;
                //button2.Visible = true;
                //button3.Visible = true;
            }
        }
    }
}
