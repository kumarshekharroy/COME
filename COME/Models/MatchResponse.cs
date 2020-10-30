using System;
using System.Collections.Generic;
using System.Text;

namespace COME.Models
{
  public class MatchResponse
    {
        public string Symbol { get; set; }
        public DateTime EventTS { get; set; }
        public string To { get; set; }
        public string EventType { get; set; }
        public string EventID { get; set; }
        public List<Order> UpdatedBuyOrders { get; set; } = new List<Order>();
        public List<Order> UpdatedSellOrders { get; set; } = new List<Order>();
        public List<Trade> NewTrades { get; set; } = new List<Trade>();
        public Dictionary<decimal, decimal> UpdatedBuyOrderBook { get; set; } = new Dictionary<decimal, decimal>();
        public Dictionary<decimal, decimal> UpdatedSellOrderBook { get; set; } = new Dictionary<decimal, decimal>(); 
    }
}
