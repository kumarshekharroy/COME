using System;
using System.Collections.Generic;
using System.Text;

namespace COME.Models
{
    public class MatchResponse
    {
        public MatchResponse(string symbol)
        {
            this.Symbol = symbol;
        }
        public readonly string Symbol;
        public DateTime EventTS { get; set; }
        public string To { get; set; } = "all";
        public string EventType { get; set; }
        public string EventID { get; set; }
        public readonly List<Order> UpdatedBuyOrders = new List<Order>(100);
        public readonly List<Order> UpdatedSellOrders = new List<Order>(100);
        public readonly List<Trade> NewTrades = new List<Trade>(100);
        public readonly Dictionary<decimal, decimal> UpdatedBuyOrderBook = new Dictionary<decimal, decimal>(100);
        public readonly Dictionary<decimal, decimal> UpdatedSellOrderBook = new Dictionary<decimal, decimal>(100);
    }
}
