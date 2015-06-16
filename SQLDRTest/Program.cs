using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OSIsoft.AF;
using OSIsoft.AF.Asset;
using OSIsoft.AF.Data;
using System.Threading;

namespace SQLDRTest
{
    class Program
    {
        static void Main(string[] args)
        {
            PISystem myAF = new PISystems()["DNG-AF2014"];
            AFDatabase myDB = myAF.Databases["Dev Support"];
           
            AFElement myElement = myDB.Elements["CDR"];
            AFAttribute myAttribute1 = myElement.Attributes["cdt158"];
            AFAttribute myAttribute2 = myElement.Attributes["SQLDRTest"];
            AFAttribute myAttribute3 = myElement.Attributes["SQLDRTest2"];
            IList<AFAttribute> attrList = new List<AFAttribute>();
            attrList.Add(myAttribute1);
            attrList.Add(myAttribute2);
            attrList.Add(myAttribute3);
            
            using (AFDataPipe myDataPipe = new AFDataPipe())
            {
                myDataPipe.AddSignups(attrList);
                IObserver<AFDataPipeEvent> observer = new DataPipeObserver();
                myDataPipe.Subscribe(observer);
                bool more = false;

                // create a cancellation source to terminate the update thread when the user is done  
                CancellationTokenSource cancellationSource = new CancellationTokenSource();
                Task task = Task.Run(() =>
                {
                    // keep polling while the user hasn't requested cancellation  
                    while (!cancellationSource.IsCancellationRequested)
                    {
                        // Get updates from pipe and process them  
                        AFErrors<AFAttribute> myErrors = myDataPipe.GetObserverEvents(out more);
                        
                        // wait for 1 second using the handle provided by the cancellation source  
                        cancellationSource.Token.WaitHandle.WaitOne(1000);
                    }

                }, cancellationSource.Token);
                
                Console.ReadKey(); 
                Console.WriteLine("Exiting updates");
                cancellationSource.Cancel();

                // wait for the task to complete before taking down the pipe  
                task.Wait(); 
            }
               
        }
    }
}
