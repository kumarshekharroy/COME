using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace COME.Models
{
    class OrderPointer
    {
        public decimal Price { get; set; }
        public OrderSide Side { get; set; } 
        public bool IsStopOrder { get; set; }
    }
}
