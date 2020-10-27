using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace COME.Models
{
   public class Statistic
    {  
        public Statistic()
        {
            this.InitTime = DateTime.UtcNow;
        }
        public int Submission { get; set; }
        public int StopActivation { get; set; }
        public int Trades { get; set; }
        public int Cancellation { get; set; }
        public int Book { get; set; }
        public int ActiveOrders { get; set; }
        public int StopOrders { get; set; }
        public int DustOrders { get; set; }
        public decimal TPS { get; set; }
        public decimal ATPS { get; set; }
        public DateTime InitTime { get; set; } 
        public DateTime LastEventTime { get; set; } 
        public decimal CurrentMarketPrice { get; set; } 
    }
}
