using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OSIsoft.AF.Data;

namespace SQLDRTest
{
    class DataPipeObserver : IObserver<AFDataPipeEvent>
    {
        public void OnCompleted()
        {
            Console.WriteLine("Completed");
        }

        public void OnError(Exception error)
        {
            Console.WriteLine("Error");
        }

        public void OnNext(AFDataPipeEvent value)
        {
            Console.WriteLine("\n{0} NEW VALUE from Attribute: {1}\n => Value: {2} and Timestamp is {3}.", DateTime.Now.ToString(), value.Value.Attribute.Name, value.Value.Value, value.Value.Timestamp.LocalTime);
        }
    }
}
