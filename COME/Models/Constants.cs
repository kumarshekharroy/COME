using System;
using System.Collections.Generic;
using System.Text;

namespace COME.Models
{
    public enum OrderSide
    {
        Unknown = 0,
        Buy = 1,
        Sell = 2
    }

    public enum OrderType
    {
        Unknown = 0,
        Limit = 1,
        StopLimit = 2,
        Market = 3,
        StopMarket = 4,
    }

    public enum OrderStatus
    {
        Unknown = 0,
        Accepted = 1,
        PartiallyFilled = 2,
        FullyFilled = 3,
        Rejected = 4,
        PartiallyCancelled = 5,
        FullyCancelled = 6,
    }
    public enum RequestStatus
    {
        Unknown = 0,
        Processed = 1,
        Rejected = 1,
        Exception = 2,
        Timeout = 3,
    }

    public enum OrderTimeInForce
    {
        GTC = 0,
        DO = 1,
        IOC = 2,
        FOK = 3,
    }

    public static class EnumHelper
    {
        public static bool IsDeadOrder(OrderStatus orderStatus) => orderStatus switch
        {
            OrderStatus.Unknown => true,
            OrderStatus.FullyFilled => true,
            OrderStatus.Rejected => true,
            OrderStatus.FullyCancelled => true,
            OrderStatus.PartiallyCancelled => true,

            OrderStatus.Accepted => false,
            OrderStatus.PartiallyFilled => false,
            _ => false
        };
       
    }
}
