using ICSharpCode.SharpZipLib.BZip2;
using ICSharpCode.SharpZipLib.Core;
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

        BufferManager<StringBuilder> sbpool;
        BufferManager<DocInfo> docpool;

        public MainWindow()
        {
            InitializeComponent();
            const int bufcnt = 4000;
            StringBuilder[] sbs = new StringBuilder[bufcnt];
            DocInfo[] docs = new DocInfo[bufcnt];
            for (int i = 0; i < bufcnt; i++)
            {
                sbs[i] = new StringBuilder(1000);
                docs[i] = new DocInfo();
            }
            sbpool = new BufferManager<StringBuilder>(sbs);
            docpool = new BufferManager<DocInfo>(docs);

        }
        bool readEnd = false;
        bool[] stopmatch = { false, false };

        int cnt = 0;
        int fullRead = 50;
        Stopwatch sw = new Stopwatch();
        Regex regex = new Regex(@"<docno>(?<no>.+)</docno>[.\W]*<url>(?<url>.+)</url>[.\W\w]*?(<title>(?<title>.+)</title>[.\W\w]*?)?(<meta[.\W\w]*""keywords""[.\W\w]*?""(?<key>.+)""\W*?>[.\W\w]*?)?(<meta[.\W\w]*""description""[.\W\w]*?""(?<abs>.+)""\W*>[\W\w]*?)?</head>", RegexOptions.IgnoreCase | RegexOptions.Compiled);
        private async void Button_Click(object sender, RoutedEventArgs e)
        {
            btnGo.IsEnabled = false;


            sw.Start();


            using (Stream stream = File.OpenRead(@"D:\SoconaP\Desktop\part-m-00000.bz2"))
            {
                MemoryStream ms = new MemoryStream();
                StreamUtils.Copy(stream, ms, new byte[2048000]);
                ms.Seek(0, SeekOrigin.Begin);
                using (var archive = new BZip2InputStream(ms))
                {
                    StreamReader sr = new StreamReader(archive);

                    Thread taskRead = new Thread(() =>
                    {
                        while (sr.BaseStream.Position < sr.BaseStream.Length)
                        {
                            var str = ReadADoc(sr);
                            pagelists.Enqueue(str);
                            fullRead += 2;
                            if (pagelists.Count > 4000)
                            {
                                Debug.WriteLine("READ QUEUE FULL!");
                                fullRead += 10;
                                Thread.Sleep(500);
                            }
                        }
                        readEnd = true;
                    });
                    taskRead.Priority = ThreadPriority.AboveNormal;


                    Thread taskmatch1;
                    Thread taskmatch2;

                    Thread taskWrite = new Thread(() =>
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
                                  docpool.CheckIn(doc);
                                  osw.WriteLine(jsonString);
                              }
                              else
                              {
                                  Thread.Sleep(200);
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
                                  Thread.Sleep(200);
                              }
                          }
                          wfs.Close();
                          ewfs.Close();
                      });
                    taskWrite.Priority = ThreadPriority.BelowNormal;

                    Task taskCtrl = new Task(() =>
                     {

                         taskRead.Start();
                         Thread.Sleep(3000);
                         taskWrite.Start();
                         bool run1 = false, run2 = false;
                         stopmatch[0] = true;
                         stopmatch[1] = true;
                         while (!readEnd)
                         {
                             if (fullRead > 400 && stopmatch[0])
                             {
                                 stopmatch[0] = false;
                                 run1 = true;

                                 ThreadPool.UnsafeQueueUserWorkItem(TaskMatchProcedure, 0);
                                 //taskmatch1 = new Thread(TaskMatchProcedure);
                                 //taskmatch1.Priority = ThreadPriority.BelowNormal;
                                 //taskmatch1.Start(0);

                             }

                             if (fullRead > 800 && stopmatch[1])
                             {
                                 stopmatch[1] = false;
                                 run2 = true;
                                 ThreadPool.UnsafeQueueUserWorkItem(TaskMatchProcedure, 1);
                                 //taskmatch2 = new Thread(TaskMatchProcedure);
                                 //taskmatch2.Priority = ThreadPriority.BelowNormal;
                                 //taskmatch2.Start(1);
                             }
                             if (fullRead < 400 && !stopmatch[1] && run2)
                             {
                                 //stopmatch[1] = true;
                                 // run2 = false;
                                 //Thread.Sleep(200);
                             }
                             if (fullRead < 0 && !stopmatch[0] && run1)
                             {
                                 //stopmatch[0] = true;
                                 //run1 = false;
                                 //  Thread.Sleep(200);

                             }
                             Thread.Sleep(200);
                         }

                     });

                    //  var tasks =new Task[] { taskRead, taskmatch1, taskmatch2 };
                    taskCtrl.Start();

                    await taskCtrl;

                    sw.Stop();

                }
            }
            btnGo.IsEnabled = true;
            MessageBox.Show((sw.ElapsedMilliseconds / 1000.0).ToString());
        }
        private void AddInfo(string str)
        {
            this.Dispatcher.InvokeAsync(() =>
            {
                listResult.Text = (str);
                //listResult.ScrollToEnd();
            });
            // Console.WriteLine(str);
        }

        private string ReadADoc(StreamReader sr)
        {
            StringBuilder sb = sbpool.CheckOut();
            try
            {
                sb.Clear();
                int status = 0;
                string line = sr.ReadLine();
                while (!line.Contains("<doc>") && !sr.EndOfStream)
                {
                    line = sr.ReadLine();
                }
                if (line.Contains("<doc>"))
                {
                    sb.AppendLine(line);
                    line = sr.ReadLine();
                    sb.AppendLine(line);
                    line = sr.ReadLine();
                    sb.AppendLine(line);
                    line = sr.ReadLine();
                    status = 1;
                    while (status == 1 && !sr.EndOfStream)
                    {
                        int idx = line.IndexOf("</head>");
                        if (idx < 0) idx = line.IndexOf("</HEAD>");
                        if (idx >= 0)
                        {
                            line = line.Substring(0, idx + 7);
                            status = 0;
                        }
                        string[] keywords = new[] { "meta", "title", "</head>" };
                        foreach (var kw in keywords)
                        {
                            if (line.Contains(kw) || line.Contains(kw.ToUpper()))
                            {
                                sb.AppendLine(line);
                                break;
                            }
                        }
                        line = sr.ReadLine();
                    }
                    if (status == 0)
                    {

                        return sb.ToString();
                    }
                }
            }
            finally
            {
                sbpool.CheckIn(sb);
            }
            return "";
        }
        private void TaskMatchProcedure(object para)
        {
            int id = (int)para;
            Debug.WriteLine($"{id} - STARTED !");
            while (!stopmatch[id] && (pagelists.Count > 0 || !readEnd))
            {
                string page;
                if (pagelists.TryDequeue(out page))
                {
                    fullRead -= 1;
                    var match = regex.Match(page);
                    Interlocked.Increment(ref cnt);
                    if (match.Success)
                    {
                        var docInfo = docpool.CheckOut() ?? new DocInfo();

                        docInfo.Id = match.Groups["no"].Value;
                        docInfo.Url = match.Groups["url"].Value;
                        docInfo.Title = match.Groups["title"]?.Value;
                        docInfo.Keywords = match.Groups["key"]?.Value;
                        docInfo.Abstract = match.Groups["abs"]?.Value;
                        docinfos.Enqueue(docInfo);
                    }
                    else
                    {
                        failed.Enqueue(page);
                    }
                    if (cnt % 20 == 0)
                    {
                        AddInfo($"{id} - {sw.ElapsedMilliseconds / 1000.0} - {cnt} - \t\t");
                    }
                }
                else
                {
                    Debug.WriteLine("READ QUEUE EMPTY!");
                    fullRead -= 40;
                    Thread.Sleep(200);
                }
            }
            Debug.WriteLine($"{id} - STOPPED !");
        }

        private void Window_Closing(Object sender, System.ComponentModel.CancelEventArgs e)
        {
            stopmatch[0] = true;
            stopmatch[1] = true;
            readEnd = true;
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
