using COME.Models;
using COME.Utilities;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace COME.Services
{
    public sealed class MEService
    {
        readonly ConcurrentDictionary<string, ME> Symbol_ME = new ConcurrentDictionary<string, ME>();
        readonly object me_creation_lock = new object();
        readonly HashSet<OrderType> StopOrderTypes = new HashSet<OrderType> { OrderType.StopLimit, OrderType.StopMarket };
        const decimal Zero = 0M;
        public MEService()
        {

        }


        public void StartMatchingEngine(string symbol, int precision = 8, decimal dustSize = 0.00000001M)
        {
            if (string.IsNullOrWhiteSpace(symbol))
                throw new ArgumentException("invalid `symbol`.");
            if (precision > 10 || precision < Zero)
                throw new ArgumentException("invalid `precision`.");
            if (dustSize < Zero)
                throw new ArgumentException("invalid `dustSize`.");

            lock (me_creation_lock)
            {
                symbol = symbol.ToUpper().Trim();

                if (Symbol_ME.ContainsKey(symbol))
                    throw new ArgumentException($"matching engine for `{symbol}` is already running.");

                Symbol_ME[symbol] = new ME(symbol: symbol, precision: precision, dustSize: dustSize);
            }
        }


        public void StopMatchingEngine(string symbol)
        {
            if (string.IsNullOrWhiteSpace(symbol))
                throw new ArgumentException("invalid `symbol`.");

            lock (me_creation_lock)
            {
                symbol = symbol.ToUpper().Trim();

                if (!Symbol_ME.TryRemove(symbol, out var _))
                    throw new ArgumentException($"matching engine for `{symbol}` is not running.");
            }
        }


        public async Task<(bool isProcessed, RequestStatus requestStatus, string message)> ValidateAndSubmitOrderAsync(Order order)
        {
            try
            {
                if (order == null)
                    return (false, RequestStatus.Rejected, "no order payload supplied");


                var validationResult = order.Validate();
                if (!validationResult.isValid)
                    return (false, RequestStatus.Rejected, validationResult.errorMessage);

                if (!Symbol_ME.TryGetValue(order.Symbol, out var ME) || ME == null)
                    return (false, RequestStatus.Rejected, $"matching engine for `{order.Symbol}` is not running.");


                var senitizationResult = order.Senitize(ME);
                if (!senitizationResult.senitized)
                    return (false, RequestStatus.Rejected, senitizationResult.errorMessage);

                var match_res = await ME.AcceptOrderAndProcessMatchAsync(order);
                return (match_res.isProcessed, match_res.requestStatus, match_res.message);

            }
            catch (Exception ex)
            {
                Console.WriteLine($"ValidateAndSubmitOrderAsync : {ex.Message}");

                return (false, RequestStatus.Exception, $"ValidateAndSubmitOrderAsync : {ex.Message}");

            }
        }

        public async Task<(bool isProcessed, RequestStatus requestStatus, string message)> CancelOrderAsync(string orderID, string symbol)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(orderID))
                    return (false, RequestStatus.Rejected, "invalid `orderID` supplied");

                if (string.IsNullOrWhiteSpace(symbol))
                    return (false, RequestStatus.Rejected, "invalid `symbol` supplied");


                symbol = symbol.ToUpper().Trim();

                if (!Symbol_ME.TryGetValue(symbol, out var ME))
                    return (false, RequestStatus.Rejected, $"matching engine for `{symbol}` is not running.");


                var cancellation_res = await ME.CancleOrderAsync(orderID);
                return (cancellation_res.isProcessed, cancellation_res.requestStatus, cancellation_res.message);

            }
            catch (Exception ex)
            {
                Console.WriteLine($"CancelOrderAsync : {ex.Message}");

                return (false, RequestStatus.Exception, $"CancelOrderAsync : {ex.Message}");

            }
        }

    }
}
