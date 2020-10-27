using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace COME.Models
{
    public class Trade
    {
        public Trade()
        {

        }
        public Trade(DateTime? dateTime)
        {
            this.TimeStamp = dateTime ?? DateTime.UtcNow;
        }
        //public string ID { get; set; }
       // public string Symbol { get; set; }
        public string BuyerID { get; set; }
        public string SellerID { get; set; }
        public string BuyOrderID { get; set; }
        public string SellOrderID { get; set; }
        public OrderSide ExecutionSide { get; set; }
        public decimal Quantity { get; set; }
        public decimal Price { get; set; }
        public DateTime TimeStamp { get; set; }
    } 
    public class Custom_ConcurrentQueue<T> : ConcurrentQueue<T>
    {
        public delegate void ItemAddedDelegate(T item);
        public event ItemAddedDelegate ItemAdded;

        public new virtual void Enqueue(T item)
        {
            base.Enqueue(item);
            if (ItemAdded != null)
            {
                ItemAdded(item);
            }
        } 

    }
}
