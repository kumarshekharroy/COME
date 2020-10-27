using System;
using System.Collections.Generic;
using System.Text;

namespace COME.Models
{
   public class Order :ICloneable
    {
        public string ID { get; set; }
        public string UserID { get; set; }
        public string Symbol { get; set; }
        public OrderSide Side { get; set; }
        public OrderType Type { get; set; }
        public decimal Quantity { get; set; }
        public decimal Price { get; set; }
        public decimal TriggerPrice { get; set; }
        public bool IsStopActivated { get; set; }
        public decimal PendingQuantity { get; set; }
        public DateTime AcceptedOn { get; set; }
        public DateTime ModifiedOn { get; set; }
        public OrderStatus Status { get; set; }
        public OrderTimeInForce TimeInForce { get; set; }

        public object Clone()
        {
            return this.MemberwiseClone();
        }
    }

    public class BulkOrder
    {
        public List<Order> orders { get; set; }
        public string Pair { get; set; }
    }

    public class UpdateOrder 
    {
        public string ID { get; set; }
        public string Symbol { get; set; } 
        public decimal Quantity { get; set; }
        public decimal Price { get; set; } 
        public decimal TriggerPrice { get; set; }  
    }
}
