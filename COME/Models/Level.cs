using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace COME.Models
{
    public class Level
    {
       public LinkedList<Order> Orders { get; set; }
       public decimal TotalSize { get; set; }
    }
}
