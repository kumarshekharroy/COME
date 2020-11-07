using COME.Utilities;

using System;
using System.Collections.Generic;
using System.Text;

namespace COME.Models
{
    public class Order : ICloneable
    {
        static readonly HashSet<OrderType> LimitOrderTypes = new HashSet<OrderType> { OrderType.StopLimit, OrderType.Limit };
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
        public bool IsPostOnly { get; set; }

        public object Clone()
        {
            return this.MemberwiseClone();
        }

        public (bool isValid, string errorMessage) Validate()
        {

            if (string.IsNullOrWhiteSpace(this.ID))
                return (false, "invalid order `id` supplied");

            if (string.IsNullOrWhiteSpace(this.UserID))
                return (false, "invalid order `userid` supplied");

            if (this.Type == OrderType.Unknown)
                return (false, "invalid order `type` supplied");

            if (this.IsPostOnly && !LimitOrderTypes.Contains(this.Type))
                return (false, "invalid order `type` supplied. PostOnly order for the order type is not supported.");

            if (this.Side == OrderSide.Unknown)
                return (false, "invalid order `side` supplied");

            if (string.IsNullOrWhiteSpace(this.Symbol))
                return (false, "invalid order `symbol` supplied");


            this.Symbol = this.Symbol.ToUpper().Trim();

            return (true, string.Empty);

        }

        public (bool senitized, string errorMessage) Senitize(ME me)
        {
            this.Price = this.Price.TruncateDecimal(me.decimal_precision);
            this.Quantity = this.Quantity.TruncateDecimal(me.decimal_precision);
            this.TriggerPrice = this.TriggerPrice.TruncateDecimal(me.decimal_precision);


            if (this.Price <= ME.Zero)
                return (false, $"invalid order `price` supplied. val : {this.Price}");

            if (this.Quantity <= ME.Zero)
                return (false, $"invalid order `quantity` supplied. val : {this.Quantity}");

            if ((this.Price * this.Quantity).TruncateDecimal(me.decimal_precision) <= ME.Zero) //optional check
                return (false, "order `amount` too low.");

            if (me.StopOrderTypes.Contains(this.Type) && this.TriggerPrice <= ME.Zero) //optional check
                return (false, $"invalid order `triggerprice` supplied. val : {this.TriggerPrice}");

            this.PendingQuantity = this.Quantity;

            return (true, string.Empty);

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
        public decimal? Quantity { get; set; }
        public decimal? Price { get; set; }
        public decimal? TriggerPrice { get; set; }
    }
}
