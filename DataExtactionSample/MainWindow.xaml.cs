using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace DataExtactionSample
{
    /// <summary>
    /// MainWindow.xaml 的交互逻辑
    /// </summary>
    public partial class MainWindow : Window
    {

        ConcurrentQueue<string> pagelists = new ConcurrentQueue<string>();
        ConcurrentQueue<DocInfo> docinfos = new ConcurrentQueue<DocInfo>();
        ConcurrentQueue<string> failed = new ConcurrentQueue<string>();
        public MainWindow()
        {
            InitializeComponent();
            // fs = File.OpenRead(@"D:\Users\SoconaL\Desktop\part-m-00000\part-m-00000");
        }

        private async void Button_Click(object sender, RoutedEventArgs e)
        {
            Stopwatch sw = new Stopwatch();
            bool readEnd = false;
            sw.Start();
            int cnt = 0; ;
            Regex regex = new Regex(@"<docno>(?<no>.+)</docno>[.\W]*<url>(?<url>.+)</url>[.\W\w]*?(<title>(?<title>.+)</title>[.\W\w]*?)?(<meta[.\W\w]*""keywords""[.\W\w]*?""(?<key>.+)""\W*?>[.\W\w]*?)?(<meta[.\W\w]*""description""[.\W\w]*?""(?<abs>.+)""\W*>[\W\w]*?)?</head>", RegexOptions.IgnoreCase | RegexOptions.Compiled);

            // Create the memory-mapped file.
            //using (var fs = File.OpenRead(@"D:\Users\SoconaL\Desktop\part-m-00000\part-m-00000"))
            //{
            FileInfo info = new FileInfo(@"D:\Users\SoconaL\Desktop\part-m-00000\part-m-00000");
            using (MemoryMappedFile mmf = MemoryMappedFile.CreateFromFile(@"D:\Users\SoconaL\Desktop\part-m-00000\part-m-00000", FileMode.Open, "MAXA")) //,info.Length,MemoryMappedFileAccess.Read))
            {
                using (Stream stream = mmf.CreateViewStream())
                {
                    StreamReader sr = new StreamReader(stream);
                    Task taskRead = new Task(() =>
                    {
                        while (sr.BaseStream.Position < sr.BaseStream.Length)
                        {
                            var str = ReadADoc(sr);
                            pagelists.Enqueue(str);
                            if (pagelists.Count > 1000)
                            {
                                Task.Delay(200);
                            }
                        }
                        readEnd = true;
                    });



                    Task taskmatch1 = new Task(() =>
                    {
                        while (pagelists.Count > 0 || !readEnd)
                        {
                            string page;
                            if (pagelists.TryDequeue(out page))
                            {
                                var match = regex.Match(page);
                                Interlocked.Increment(ref cnt);
                                if (match.Success)
                                {
                                    var docInfo = new DocInfo()
                                    {
                                        Id = match.Groups["no"].Value,
                                        Url = match.Groups["url"].Value,
                                        Title = match.Groups["title"]?.Value,
                                        Keywords = match.Groups["key"]?.Value,
                                        Abstract = match.Groups["abs"]?.Value

                                    };
                                    docinfos.Enqueue(docInfo);
                                }
                                else
                                {
                                    failed.Enqueue(page);
                                }
                                if (cnt % 20 == 0)
                                {
                                    AddInfo($"1-{sw.ElapsedMilliseconds / 1000.0}-{cnt}-\t\t");
                                }
                            }
                            else
                            {
                                Task.Delay(100);
                            }
                        }

                    });
                    Task taskmatch2 = new Task(() =>
                   {
                       while (pagelists.Count > 0 || !readEnd)
                       {
                           string page;
                           if (pagelists.TryDequeue(out page))
                           {
                               var match = regex.Match(page);
                               Interlocked.Increment(ref cnt);
                               if (match.Success)
                               {
                                   var docInfo = new DocInfo()
                                   {
                                       Id = match.Groups["no"].Value,
                                       Url = match.Groups["url"].Value,
                                       Title = match.Groups["title"]?.Value,
                                       Keywords = match.Groups["key"]?.Value,
                                       Abstract = match.Groups["abs"]?.Value

                                   };
                                   docinfos.Enqueue(docInfo);
                               }
                               else
                               {
                                   failed.Enqueue(page);
                               }
                               if (cnt % 20 == 0)
                               {
                                   AddInfo($"2-{sw.ElapsedMilliseconds / 1000.0}-{cnt}-\t\t");
                               }
                           }
                           else
                           {
                               Task.Delay(100);
                           }
                       }

                   });

                    Task taskWrite = new Task(() =>
                      {
                          FileStream wfs = new FileStream("output.txt", FileMode.OpenOrCreate, FileAccess.Write);
                          FileStream ewfs = new FileStream("failed.txt", FileMode.OpenOrCreate, FileAccess.Write);
                          StreamWriter osw = new StreamWriter(wfs);
                          StreamWriter esw = new StreamWriter(ewfs);
                          var javaScriptSerializer = new System.Web.Script.Serialization.JavaScriptSerializer();


                          while (docinfos.Count > 0 || !readEnd)
                          {

                              DocInfo doc;
                              if (docinfos.TryDequeue(out doc))
                              {
                                  string jsonString = javaScriptSerializer.Serialize(doc);
                                  osw.WriteLine(jsonString);
                              }
                              else
                              {
                                  Task.Delay(100);
                              }
                          }
                          wfs.Close();
                          while (failed.Count > 0 || !readEnd)
                          {

                              string str;
                              if (failed.TryDequeue(out str))
                              {

                                  esw.WriteLine(str);
                              }
                              else
                              {
                                  Task.Delay(100);
                              }
                          }
                          wfs.Close();
                          ewfs.Close();
                      });
                    taskRead.Start();
                    taskmatch1.Start();
                    taskmatch2.Start();
                    taskWrite.Start();
                    //  var tasks =new Task[] { taskRead, taskmatch1, taskmatch2 };

                    await taskRead;
                    await taskmatch1;
                    await taskmatch2;
                    await taskWrite;
                    sw.Stop();

                }
            }

            MessageBox.Show((sw.ElapsedMilliseconds / 1000.0).ToString());
        }
        private void AddInfo(string str)
        {
            this.Dispatcher.InvokeAsync(() =>
            {
                listResult.AppendText(str);
                listResult.ScrollToEnd();
            });
            // Console.WriteLine(str);
        }

        private string ReadADoc(StreamReader sr)
        {
            StringBuilder sb = new StringBuilder();
            int status = 0;
            string line = sr.ReadLine();
            while (!line.Contains("<doc>") && sr.BaseStream.Position < sr.BaseStream.Length)
            {
                line = sr.ReadLine();
            }
            if (line.Contains("<doc>"))
            {
                status = 1;
                while (status == 1 && sr.BaseStream.Position < sr.BaseStream.Length)
                {
                    if (line.Contains("</head>") || line.Contains("</HEAD>"))
                    {
                        var idx = line.IndexOf("</head>");
                        if (idx < 0) idx = line.IndexOf("</HEAD>");
                        if (idx >= 0)
                        {
                            line = line.Substring(0, idx + 7);
                        }
                        status = 0;
                    }
                    sb.AppendLine(line);
                    line = sr.ReadLine();
                }
                if (status == 0)
                {
                    return sb.ToString();
                }
            }

            return "";
        }
    }
    public class DocInfo
    {
        public string Id { get; set; }
        public string Url { get; set; }
        public string Title { get; set; }
        public string Keywords { get; set; }
        public string Abstract { get; set; }
    }
}
