using COME.Models;
using COME.Utilities;

using StackExchange.Redis;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Order = COME.Models.Order;

namespace COME
{
    public class ME
    {
        public readonly int decimal_precision;
        public readonly decimal dust_size;
        readonly string symbol;
        readonly string response_channel;
        readonly string request_cancellation_channel;
        readonly string request_neworder_channel;
        readonly System.Threading.Timer zero_O_Clock_Timer = null;
        readonly System.Threading.Timer one_Sec_Timer = null;


        readonly HashSet<OrderType> StopOrderTypes = new HashSet<OrderType> { OrderType.StopLimit, OrderType.StopMarket };
        readonly HashSet<OrderType> MarketOrderTypes = new HashSet<OrderType> { OrderType.StopMarket, OrderType.Market };

        DateTime LastEventTime = DateTime.MinValue;
        decimal last_Trade_Price = 0M;
        long last_Txn_Count = 0, txn_Count = 0;

        readonly SemaphoreSlim match_semaphore = new SemaphoreSlim(1, 1);
        readonly int timeout_In_Millisec = 5000;
        const decimal Zero = 0M;


        readonly SortedDictionary<decimal, Level> BuyPrice_Level = new SortedDictionary<decimal, Level>();
        readonly SortedDictionary<decimal, Level> SellPrice_Level = new SortedDictionary<decimal, Level>();


        readonly SortedDictionary<decimal, LinkedList<Order>> Stop_BuyOrdersDict = new SortedDictionary<decimal, LinkedList<Order>>();
        readonly SortedDictionary<decimal, LinkedList<Order>> Stop_SellOrdersDict = new SortedDictionary<decimal, LinkedList<Order>>();


        readonly Dictionary<string, OrderPointer> OrderIndexer = new Dictionary<string, OrderPointer>();
        readonly Statistic statistic = new Statistic();

        readonly ConnectionMultiplexer Connection;

        public ME(string symbol, int precision, decimal dustSize)
        {
            this.symbol = symbol;
            this.decimal_precision = precision;
            this.dust_size = dustSize;

            this.request_cancellation_channel = string.Concat(this.symbol, ".request.cancellation");
            this.request_neworder_channel = string.Concat(this.symbol, ".request.neworder");
            this.response_channel = string.Concat(this.symbol, ".response");

            ConfigurationOptions options = new ConfigurationOptions()
            {
                ClientName = string.Concat(this.symbol, ".", Environment.GetEnvironmentVariable("Redis_ClientName")),
                SyncTimeout = 5000,
                AbortOnConnectFail = false,
                ConnectRetry = 25,
                EndPoints = { Environment.GetEnvironmentVariable("Redis_EndPoints") },
                Password = Environment.GetEnvironmentVariable("Redis_Password")
            };


            this.Connection = ConnectionMultiplexer.Connect(options);

            this.Connection.ConnectionFailed += (obj, args) =>
            {
                Console.WriteLine($"Redis {options.ClientName} : {DateTime.UtcNow} => ConnectionFailed : {new { Type = args.ConnectionType.ToString(), Exception = args.Exception?.Message, FailureType = args.FailureType.ToString() }.SerializeObject()}");
            };

            this.Connection.ConnectionRestored += (obj, args) =>
            {
                Console.WriteLine($"Redis {options.ClientName} : {DateTime.UtcNow} => ConnectionRestored : {new { Type = args.ConnectionType.ToString(), Exception = args.Exception?.Message, FailureType = args.FailureType.ToString() }.SerializeObject()}");
            };

            this.Connection.ErrorMessage += (obj, args) =>
            {
                Console.WriteLine($"Redis {options.ClientName} : {DateTime.UtcNow} => ErrorMessage : { args.Message}");
            };

            var currentTime = DateTime.UtcNow;
            var dueDay = currentTime == currentTime.Date ? currentTime.Date : currentTime.Date.AddDays(1);
            zero_O_Clock_Timer = new Timer(CancelAll_DO_Orders_Callback, null, TimeSpan.FromMilliseconds((dueDay - currentTime).TotalMilliseconds), TimeSpan.FromDays(1)); //every day 
            one_Sec_Timer = new Timer(One_Sec_Timer_Callback, null, TimeSpan.FromMilliseconds(timeout_In_Millisec), TimeSpan.FromSeconds(1)); //every sec 
        }

        public async Task<(bool isProcessed, RequestStatus requestStatus, string message)> AcceptOrderAndProcessMatchAsync(Order order, bool force = false,bool accuireLock=true)
        {
            if (accuireLock && !await match_semaphore.WaitAsync(force ? timeout_In_Millisec * 10 : timeout_In_Millisec)) //for overload protection
                return (false, RequestStatus.Timeout, $"rejected : timeout of {timeout_In_Millisec} millisec elapsed.");


            try
            {
                this.statistic.Submission++;
                var currentTimestamp = DateTime.UtcNow;

                MatchResponse response = new MatchResponse
                {
                    Symbol = this.symbol,
                    EventTS = currentTimestamp
                };


                if (StopOrderTypes.Contains(order.Type))
                {
                    if (order.IsStopActivated == false) //new stop order
                    {
                        if (order.Side == OrderSide.Buy)
                        {
                            order.Status = OrderStatus.Accepted;
                            order.AcceptedOn = currentTimestamp;
                            order.ModifiedOn = currentTimestamp;

                            response.UpdatedBuyOrders.Add((Order)order.Clone());

                            if (order.TriggerPrice <= last_Trade_Price) // buy order :  market price is already breaching trigger price 
                            {
                                order.IsStopActivated = true;

                                //   _ = EnqueueForMatchAsync(order);

                            }
                            else
                            {
                                LinkedList<Order> orderList;
                                if (Stop_BuyOrdersDict.TryGetValue(order.TriggerPrice, out orderList))
                                    orderList.AddLast(order);
                                else
                                    Stop_BuyOrdersDict[order.TriggerPrice] = new LinkedList<Order>(new List<Order> { order });

                                OrderIndexer.TryAdd(order.ID, new OrderPointer { IsStopOrder = true, Price = order.TriggerPrice, Side = order.Side });

                                goto Finished;
                            }
                        }
                        else
                        {
                            order.Status = OrderStatus.Accepted;
                            order.AcceptedOn = currentTimestamp;
                            order.ModifiedOn = currentTimestamp;

                            response.UpdatedSellOrders.Add((Order)order.Clone());

                            if (order.TriggerPrice >= last_Trade_Price) // sell order :  market price is already breaching trigger price 
                            {
                                order.IsStopActivated = true;

                                //  _ = EnqueueForMatchAsync(order);

                            }
                            else
                            {
                                LinkedList<Order> orderList;
                                if (Stop_SellOrdersDict.TryGetValue(order.TriggerPrice, out orderList))
                                    orderList.AddLast(order);
                                else
                                    Stop_SellOrdersDict[order.TriggerPrice] = new LinkedList<Order>(new List<Order> { order });

                                OrderIndexer.TryAdd(order.ID, new OrderPointer { IsStopOrder = true, Price = order.TriggerPrice, Side = order.Side });

                                goto Finished;
                            }
                        }
                    }
                    else
                    {
                        this.statistic.StopActivation++;
                        //recently activated stop order
                    }

                }
                else
                {
                    order.Status = OrderStatus.Accepted;
                    order.AcceptedOn = currentTimestamp;
                    order.ModifiedOn = currentTimestamp;
                }

                decimal min_trade_price = Zero, max_trade_price = Zero;
                var isMarketOrder = MarketOrderTypes.Contains(order.Type);

                if (order.Side == OrderSide.Buy)
                {
                    response.UpdatedBuyOrders.Add((Order)order.Clone());

                    if (!(order.TimeInForce == OrderTimeInForce.FOK && SellPrice_Level.TakeWhile(x => x.Key <= order.Price).Sum(x => x.Value.TotalSize) < order.PendingQuantity))
                        while (order.PendingQuantity > Zero && SellPrice_Level.Count > 0)
                        {
                            var possibleMatches = SellPrice_Level.FirstOrDefault();
                            if (possibleMatches.Key > order.Price && !isMarketOrder)
                                break;  //Break as No Match Found for New

                            var level = possibleMatches.Value;
                            var sellOrder_Node = level.Orders.First;

                            while (sellOrder_Node != null)
                            {
                                var currentTime = DateTime.UtcNow;

                                var next_Node = sellOrder_Node.Next;

                                var sellOrder = sellOrder_Node.Value;
                                if ((sellOrder.PendingQuantity * sellOrder.Price).TruncateDecimal(decimal_precision) < dust_size)
                                {
                                    sellOrder.Status = OrderStatus.PartiallyCancelled;
                                    OrderIndexer.Remove(sellOrder.ID);
                                    response.UpdatedSellOrders.Add(sellOrder);

                                    level.TotalSize -= sellOrder.PendingQuantity;
                                    response.UpdatedSellOrderBook[sellOrder.Price] = level.TotalSize;

                                    this.statistic.DustOrders++;
                                }
                                else
                                {
                                    var tradeSize = Math.Min(sellOrder.PendingQuantity, order.PendingQuantity);

                                    sellOrder.Status = sellOrder.PendingQuantity == tradeSize ? OrderStatus.FullyFilled : OrderStatus.PartiallyFilled;
                                    sellOrder.PendingQuantity -= tradeSize;
                                    sellOrder.ModifiedOn = currentTime;

                                    order.Status = order.PendingQuantity == tradeSize ? OrderStatus.FullyFilled : OrderStatus.PartiallyFilled;
                                    order.PendingQuantity -= tradeSize;
                                    order.ModifiedOn = currentTime;

                                    level.TotalSize -= tradeSize;
                                    response.UpdatedSellOrderBook[sellOrder.Price] = level.TotalSize;

                                    var trade = new Trade
                                    {
                                        TimeStamp = currentTime,
                                        BuyOrderID = order.ID,
                                        SellOrderID = sellOrder.ID,
                                        ExecutionSide = order.Side,
                                        Price = sellOrder.Price,
                                        Quantity = tradeSize,
                                        BuyerID = order.UserID,
                                        SellerID = sellOrder.UserID,
                                    };
                                    response.UpdatedBuyOrders.Add((Order)order.Clone());
                                    response.UpdatedSellOrders.Add((Order)sellOrder.Clone());
                                    response.NewTrades.Add(trade);

                                    last_Trade_Price = trade.Price;

                                    if (min_trade_price > last_Trade_Price || min_trade_price == Zero)
                                        min_trade_price = last_Trade_Price;

                                    if (max_trade_price < last_Trade_Price)
                                        max_trade_price = last_Trade_Price;

                                    this.statistic.Trades++;
                                }

                                if (EnumHelper.IsDeadOrder(sellOrder.Status))
                                {
                                    OrderIndexer.Remove(sellOrder.ID);

                                    if (level.Orders.Count == 1)
                                    {
                                        SellPrice_Level.Remove(sellOrder.Price); //The Order was Only Order at the given rate;
                                        break;//Break from foreach as No Pending Order at given rate
                                    }
                                    else
                                    {
                                        level.Orders.Remove(sellOrder_Node);
                                    }
                                }

                                if (order.Status == OrderStatus.FullyFilled)
                                    break;  //Break as Completely Matched New

                                sellOrder_Node = next_Node;

                            }
                        }

                    if (!EnumHelper.IsDeadOrder(order.Status))
                    {
                        if (isMarketOrder || order.TimeInForce == OrderTimeInForce.IOC || order.TimeInForce == OrderTimeInForce.FOK)
                        {
                            order.Status = order.PendingQuantity == order.Quantity ? OrderStatus.FullyCancelled : OrderStatus.PartiallyCancelled;
                            response.UpdatedBuyOrders.Add(order);
                        }
                        else
                        {

                            if (BuyPrice_Level.TryGetValue(order.Price, out var level))
                            {
                                level.Orders.AddLast(order);

                                level.TotalSize += order.PendingQuantity;
                                response.UpdatedBuyOrderBook[order.Price] = level.TotalSize;
                            }
                            else
                            {
                                level = new Level { Orders = new LinkedList<Order>(new List<Order> { order }), TotalSize = order.PendingQuantity };

                                BuyPrice_Level[order.Price] = level;
                                response.UpdatedBuyOrderBook[order.Price] = level.TotalSize;
                            }

                            OrderIndexer.TryAdd(order.ID, new OrderPointer { IsStopOrder = false, Price = order.Price, Side = order.Side });
                        }

                    }

                }
                else
                {
                    response.UpdatedSellOrders.Add((Order)order.Clone());
                    if (!(order.TimeInForce == OrderTimeInForce.FOK && BuyPrice_Level.SkipWhile(x => x.Key < order.Price).Sum(x => x.Value.TotalSize) < order.PendingQuantity))
                        while (order.PendingQuantity > Zero && BuyPrice_Level.Count > 0)
                        {
                            var possibleMatches = BuyPrice_Level.Reverse().FirstOrDefault();
                            if (possibleMatches.Key < order.Price && !isMarketOrder)
                                break;  //Break as No Match Found for New


                            var level = possibleMatches.Value;

                            var buyOrder_Node = level.Orders.First;

                            while (buyOrder_Node != null)
                            {
                                var currentTime = DateTime.UtcNow;

                                var next_Node = buyOrder_Node.Next;

                                var buyOrder = buyOrder_Node.Value;
                                if ((buyOrder.PendingQuantity * buyOrder.Price).TruncateDecimal(decimal_precision) < dust_size)
                                {
                                    buyOrder.Status = OrderStatus.PartiallyCancelled;
                                    OrderIndexer.Remove(buyOrder.ID);
                                    response.UpdatedBuyOrders.Add(buyOrder);

                                    level.TotalSize -= buyOrder.PendingQuantity;
                                    response.UpdatedBuyOrderBook[buyOrder.Price] = level.TotalSize;

                                    this.statistic.DustOrders++;
                                }
                                else
                                {
                                    var tradeSize = Math.Min(buyOrder.PendingQuantity, order.PendingQuantity);

                                    buyOrder.Status = buyOrder.PendingQuantity == tradeSize ? OrderStatus.FullyFilled : OrderStatus.PartiallyFilled;
                                    buyOrder.PendingQuantity -= tradeSize;
                                    buyOrder.ModifiedOn = currentTime;

                                    order.Status = order.PendingQuantity == tradeSize ? OrderStatus.FullyFilled : OrderStatus.PartiallyFilled;
                                    order.PendingQuantity -= tradeSize;
                                    order.ModifiedOn = currentTime;


                                    level.TotalSize -= tradeSize;
                                    response.UpdatedBuyOrderBook[buyOrder.Price] = level.TotalSize;

                                    var trade = new Trade
                                    {
                                        TimeStamp = currentTime,
                                        SellOrderID = order.ID,
                                        BuyOrderID = buyOrder.ID,
                                        ExecutionSide = order.Side,
                                        Price = buyOrder.Price,
                                        Quantity = tradeSize,
                                        SellerID = order.UserID,
                                        BuyerID = buyOrder.UserID,
                                    };

                                    response.UpdatedSellOrders.Add((Order)order.Clone());
                                    response.UpdatedBuyOrders.Add((Order)buyOrder.Clone());
                                    response.NewTrades.Add(trade);

                                    last_Trade_Price = trade.Price;

                                    if (min_trade_price > last_Trade_Price || min_trade_price == Zero)
                                        min_trade_price = last_Trade_Price;

                                    if (max_trade_price < last_Trade_Price)
                                        max_trade_price = last_Trade_Price;

                                    this.statistic.Trades++;
                                }

                                if (EnumHelper.IsDeadOrder(buyOrder.Status))
                                {
                                    OrderIndexer.Remove(buyOrder.ID);

                                    if (level.Orders.Count == 1 || level.TotalSize == Zero)
                                    {
                                        BuyPrice_Level.Remove(buyOrder.Price); //The Order was Only Order at the given rate;
                                        break;//Break from foreach as No Pending Order at given rate
                                    }
                                    else
                                    {
                                        level.Orders.Remove(buyOrder_Node);
                                    }

                                }

                                if (order.Status == OrderStatus.FullyFilled)
                                    break;  //Break as Completely Matched New

                                buyOrder_Node = next_Node;

                            }
                        }
                    if (!EnumHelper.IsDeadOrder(order.Status))
                    {
                        if (isMarketOrder || order.TimeInForce == OrderTimeInForce.IOC || order.TimeInForce == OrderTimeInForce.FOK)
                        {
                            order.Status = order.PendingQuantity == order.Quantity ? OrderStatus.FullyCancelled : OrderStatus.PartiallyCancelled; 
                            response.UpdatedSellOrders.Add(order);
                        }
                        else
                        {
                            if (SellPrice_Level.TryGetValue(order.Price, out var level))
                            {
                                level.Orders.AddLast(order);

                                level.TotalSize += order.PendingQuantity;
                                response.UpdatedSellOrderBook[order.Price] = level.TotalSize;
                            }
                            else
                            {
                                level = new Level { Orders = new LinkedList<Order>(new List<Order> { order }), TotalSize = order.PendingQuantity };

                                SellPrice_Level[order.Price] = level;
                                response.UpdatedSellOrderBook[order.Price] = level.TotalSize;
                            }

                            OrderIndexer.TryAdd(order.ID, new OrderPointer { IsStopOrder = false, Price = order.Price, Side = order.Side });

                        }

                    }
                }

                if (min_trade_price != Zero && max_trade_price != Zero)
                    ActivateStopOrdersAndEnqueueForMatch(min_trade_price, max_trade_price, response);

                Finished:
                this.statistic.CurrentMarketPrice = this.last_Trade_Price;
                this.LastEventTime = currentTimestamp;
                await PublishMatchAsync(response);
                return (true, RequestStatus.Processed, string.Empty);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ProcessMatchAsync : {ex.Message}");

                return (false, RequestStatus.Exception, $"ProcessMatchAsync : {ex.Message}");

            }
            finally
            {
                match_semaphore.Release();
            }

        }

        public void ActivateStopOrdersAndEnqueueForMatch(decimal min_trade_price, decimal max_trade_price, MatchResponse response)
        {

            var currentTime = DateTime.UtcNow;

            //market price going down
            do
            {
                var stopprice = Stop_SellOrdersDict.Keys.Reverse().FirstOrDefault();
                if (min_trade_price > stopprice)
                    break;  //Break as No StopSell Order above Current/Min Trading price;

                foreach (var order in Stop_SellOrdersDict[stopprice])
                {
                    order.IsStopActivated = true;
                    order.ModifiedOn = currentTime;

                    OrderIndexer.Remove(order.ID);

                    _ = EnqueueForMatchAsync(order);
                }

                Stop_SellOrdersDict.Remove(stopprice);
            } while (true);

            //market price going up
            do
            {
                var stopprice = Stop_BuyOrdersDict.Keys.FirstOrDefault();
                if (stopprice == 0 || stopprice > max_trade_price)
                    break;  //Break as No StopLimitBuy Order below Current/Max Trading price;

                foreach (var order in Stop_BuyOrdersDict[stopprice])
                {
                    order.IsStopActivated = true;
                    order.ModifiedOn = currentTime;

                    OrderIndexer.Remove(order.ID);

                    _ = EnqueueForMatchAsync(order);
                }

                Stop_BuyOrdersDict.Remove(stopprice);

            } while (true);

        }

        public async Task<(bool isProcessed, RequestStatus requestStatus, string message)> EnqueueForMatchAsync(Order order)
        {
            return await Task.Run(async () => await AcceptOrderAndProcessMatchAsync(order, force: true));
        }

        public async Task<(bool isProcessed, RequestStatus requestStatus, string message)> CancleOrderAsync(string orderID, bool takeLock = true)
        {
            if (takeLock && !await match_semaphore.WaitAsync(timeout_In_Millisec)) //for overload protection
                return (false, RequestStatus.Timeout, $"rejected : timeout of {timeout_In_Millisec} millisec elapsed.");
            try
            {
                if (string.IsNullOrWhiteSpace(orderID))
                    return (false, RequestStatus.Rejected, $"rejected : invalid `orderID` supplied.");
                if (!OrderIndexer.TryGetValue(orderID, out var pointer) || pointer == null)
                    return (false, RequestStatus.Rejected, $"rejected : no order with `orderID` {orderID} found.");


                this.statistic.Cancellation++;
                var currentTimestamp = DateTime.UtcNow;

                MatchResponse response = new MatchResponse
                {
                    Symbol = this.symbol,
                    EventTS = currentTimestamp
                };

                if (pointer.IsStopOrder)
                {

                    if (pointer.Side == OrderSide.Buy)
                    {
                        LinkedList<Order> gropued_list;
                        if (Stop_BuyOrdersDict.TryGetValue(pointer.Price, out gropued_list))
                        {
                            var orderToBeCancelled = gropued_list.FirstOrDefault(x => x.ID == orderID);
                            if (orderToBeCancelled != null)
                            {
                                if (gropued_list.Count == 1)
                                    Stop_BuyOrdersDict.Remove(pointer.Price);
                                else
                                    gropued_list.Remove(orderToBeCancelled);

                                orderToBeCancelled.Status = orderToBeCancelled.PendingQuantity == orderToBeCancelled.Quantity ? OrderStatus.FullyCancelled : OrderStatus.PartiallyCancelled;
                                orderToBeCancelled.ModifiedOn = currentTimestamp;
                                response.UpdatedBuyOrders.Add(orderToBeCancelled);
                            }
                        }
                    }
                    else
                    {
                        LinkedList<Order> gropued_list;
                        if (Stop_SellOrdersDict.TryGetValue(pointer.Price, out gropued_list))
                        {
                            var orderToBeCancelled = gropued_list.FirstOrDefault(x => x.ID == orderID);
                            if (orderToBeCancelled != null)
                            {
                                if (gropued_list.Count == 1)
                                    Stop_SellOrdersDict.Remove(pointer.Price);
                                else
                                    gropued_list.Remove(orderToBeCancelled);

                                orderToBeCancelled.Status = orderToBeCancelled.PendingQuantity == orderToBeCancelled.Quantity ? OrderStatus.FullyCancelled : OrderStatus.PartiallyCancelled;
                                orderToBeCancelled.ModifiedOn = currentTimestamp;
                                response.UpdatedSellOrders.Add(orderToBeCancelled);
                            }
                        }
                    }
                }
                else
                {
                    if (pointer.Side == OrderSide.Buy)
                    {

                        if (BuyPrice_Level.TryGetValue(pointer.Price, out var level))
                        {
                            var orderToBeCancelled = level.Orders.FirstOrDefault(x => x.ID == orderID);
                            if (orderToBeCancelled != null)
                            {
                                if (level.Orders.Count == 1 || level.TotalSize == orderToBeCancelled.PendingQuantity)
                                    BuyPrice_Level.Remove(pointer.Price);
                                else
                                {
                                    level.TotalSize -= orderToBeCancelled.PendingQuantity;
                                    level.Orders.Remove(orderToBeCancelled);
                                }

                                orderToBeCancelled.Status = orderToBeCancelled.PendingQuantity == orderToBeCancelled.Quantity ? OrderStatus.FullyCancelled : OrderStatus.PartiallyCancelled;
                                orderToBeCancelled.ModifiedOn = currentTimestamp;
                                response.UpdatedBuyOrders.Add(orderToBeCancelled);
                            }
                        }
                    }
                    else
                    {
                        if (SellPrice_Level.TryGetValue(pointer.Price, out var level))
                        {
                            var orderToBeCancelled = level.Orders.FirstOrDefault(x => x.ID == orderID);
                            if (orderToBeCancelled != null)
                            {
                                if (level.Orders.Count == 1 || level.TotalSize == orderToBeCancelled.PendingQuantity)
                                    SellPrice_Level.Remove(pointer.Price);
                                else
                                {
                                    level.TotalSize -= orderToBeCancelled.PendingQuantity;
                                    level.Orders.Remove(orderToBeCancelled);
                                }

                                orderToBeCancelled.Status = orderToBeCancelled.PendingQuantity == orderToBeCancelled.Quantity ? OrderStatus.FullyCancelled : OrderStatus.PartiallyCancelled;
                                orderToBeCancelled.ModifiedOn = currentTimestamp;
                                response.UpdatedSellOrders.Add(orderToBeCancelled);
                            }
                        }
                    }
                }
                this.LastEventTime = currentTimestamp;
                await PublishMatchAsync(response);

                OrderIndexer.Remove(orderID); //// remove from index as order was cancelled 

                return (true, RequestStatus.Processed, string.Empty);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"CancleAsync : {ex.Message}");

                return (false, RequestStatus.Exception, $"CancleAsync : {ex.Message}");

            }
            finally
            {
                if (takeLock)
                    match_semaphore.Release();
            }
        }

        //public async Task<(bool isProcessed, RequestStatus requestStatus, string message)> UpdateOrderAsync(UpdateOrder updateOrder)
        //{
        //    if (!await match_semaphore.WaitAsync(timeout_In_Millisec)) //for overload protection
        //        return (false, RequestStatus.Timeout, $"rejected : timeout of {timeout_In_Millisec} millisec elapsed.");
        //    try
        //    {
        //        if (string.IsNullOrWhiteSpace(updateOrder.ID))
        //            return (false, RequestStatus.Rejected, $"rejected : invalid order `id` supplied.");
        //        if (!OrderIndexer.TryGetValue(updateOrder.ID, out var pointer) || pointer == null) 
        //            return (false, RequestStatus.Rejected, $"rejected : no order with `id` {updateOrder.ID} found.");


        //        this.statistic.Cancellation++;
        //        var currentTimestamp = DateTime.UtcNow;

        //        MatchResponse response = new MatchResponse
        //        {
        //            Symbol = this.symbol,
        //            EventTS = currentTimestamp
        //        };

        //        if (pointer.IsStopOrder)
        //        {

        //            if (pointer.Side == OrderSide.Buy)
        //            {
        //                LinkedList<Order> gropued_list;
        //                if (Stop_BuyOrdersDict.TryGetValue(pointer.Price, out gropued_list))
        //                {
        //                    var orderToBeCancelled = gropued_list.FirstOrDefault(x => x.ID == orderID);
        //                    if (orderToBeCancelled != null)
        //                    {
        //                        if (gropued_list.Count == 1)
        //                            Stop_BuyOrdersDict.Remove(pointer.Price);
        //                        else
        //                            gropued_list.Remove(orderToBeCancelled);

        //                        orderToBeCancelled.Status = orderToBeCancelled.PendingQuantity == orderToBeCancelled.Quantity ? OrderStatus.FullyCancelled : OrderStatus.PartiallyCancelled;
        //                        orderToBeCancelled.ModifiedOn = currentTimestamp;
        //                        response.UpdatedBuyOrders.Add(orderToBeCancelled);
        //                    }
        //                }
        //            }
        //            else
        //            {
        //                LinkedList<Order> gropued_list;
        //                if (Stop_SellOrdersDict.TryGetValue(pointer.Price, out gropued_list))
        //                {
        //                    var orderToBeCancelled = gropued_list.FirstOrDefault(x => x.ID == orderID);
        //                    if (orderToBeCancelled != null)
        //                    {
        //                        if (gropued_list.Count == 1)
        //                            Stop_SellOrdersDict.Remove(pointer.Price);
        //                        else
        //                            gropued_list.Remove(orderToBeCancelled);

        //                        orderToBeCancelled.Status = orderToBeCancelled.PendingQuantity == orderToBeCancelled.Quantity ? OrderStatus.FullyCancelled : OrderStatus.PartiallyCancelled;
        //                        orderToBeCancelled.ModifiedOn = currentTimestamp;
        //                        response.UpdatedSellOrders.Add(orderToBeCancelled);
        //                    }
        //                }
        //            }
        //        }
        //        else
        //        {
        //            if (pointer.Side == OrderSide.Buy)
        //            {

        //                if (BuyPrice_Level.TryGetValue(pointer.Price, out var level))
        //                {
        //                    var orderToBeCancelled = level.Orders.FirstOrDefault(x => x.ID == orderID);
        //                    if (orderToBeCancelled != null)
        //                    {
        //                        if (level.Orders.Count == 1 || level.TotalSize == orderToBeCancelled.PendingQuantity)
        //                            BuyPrice_Level.Remove(pointer.Price);
        //                        else
        //                        {
        //                            level.TotalSize -= orderToBeCancelled.PendingQuantity;
        //                            level.Orders.Remove(orderToBeCancelled);
        //                        }

        //                        orderToBeCancelled.Status = orderToBeCancelled.PendingQuantity == orderToBeCancelled.Quantity ? OrderStatus.FullyCancelled : OrderStatus.PartiallyCancelled;
        //                        orderToBeCancelled.ModifiedOn = currentTimestamp;
        //                        response.UpdatedBuyOrders.Add(orderToBeCancelled);
        //                    }
        //                }
        //            }
        //            else
        //            {
        //                if (SellPrice_Level.TryGetValue(pointer.Price, out var level))
        //                {
        //                    var orderToBeCancelled = level.Orders.FirstOrDefault(x => x.ID == orderID);
        //                    if (orderToBeCancelled != null)
        //                    {
        //                        if (level.Orders.Count == 1 || level.TotalSize == orderToBeCancelled.PendingQuantity)
        //                            SellPrice_Level.Remove(pointer.Price);
        //                        else
        //                        {
        //                            level.TotalSize -= orderToBeCancelled.PendingQuantity;
        //                            level.Orders.Remove(orderToBeCancelled);
        //                        }

        //                        orderToBeCancelled.Status = orderToBeCancelled.PendingQuantity == orderToBeCancelled.Quantity ? OrderStatus.FullyCancelled : OrderStatus.PartiallyCancelled;
        //                        orderToBeCancelled.ModifiedOn = currentTimestamp;
        //                        response.UpdatedSellOrders.Add(orderToBeCancelled);
        //                    }
        //                }
        //            }
        //        }
        //        this.LastEventTime = currentTimestamp;
        //        await PublishMatchAsync(response);
        //        return (true, RequestStatus.Processed, string.Empty);
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine($"CancleAsync : {ex.Message}");

        //        return (false, RequestStatus.Exception, $"CancleAsync : {ex.Message}");

        //    }
        //    finally
        //    {
        //        if (takeLock)
        //            match_semaphore.Release();
        //    }
        //}

        async Task PublishMatchAsync(MatchResponse response)
        {
            await this.Connection.GetSubscriber().PublishAsync(this.response_channel, response.SerializeObject(isFormattingIntended: false));
        }

        async void CancelAll_DO_Orders_Callback(object state)
        {
            await match_semaphore.WaitAsync();
            Console.WriteLine($"{DateTime.UtcNow} : Cancelling DO Orders : Init");
            try
            {
                do
                {
                    var currentDateTime = DateTime.UtcNow;
                    try
                    {
                        foreach (var level in this.BuyPrice_Level.Values.Concat(this.SellPrice_Level.Values))
                        {
                            foreach (var order in level.Orders)
                            {
                                if (order.TimeInForce != OrderTimeInForce.DO)
                                    continue;
                                if (order.AcceptedOn.Date >= currentDateTime.Date)
                                    continue;

                                await CancleOrderAsync(order.ID, takeLock: false);
                            }
                        }
                        break;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"CancelAll_DO_Orders_Callback (while loop) : {ex.Message}");
                    }
                } while (true);
                Console.WriteLine($"{DateTime.UtcNow} : Cancelling DO Orders : Complete");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"CancelAll_DO_Orders_Callback : {ex.Message}");
            }
            finally
            {
                match_semaphore.Release();
            }

        }

        void One_Sec_Timer_Callback(object state)
        {
            last_Txn_Count = txn_Count;

            txn_Count = (this.statistic.Submission + this.statistic.Cancellation);

            this.statistic.TPS = (decimal)(txn_Count - last_Txn_Count) / 2;
            this.statistic.ATPS = txn_Count / (decimal)(DateTime.UtcNow - this.statistic.InitTime).TotalSeconds;
        }

        public Statistic GetStatistic()
        {
            match_semaphore.Wait();
            try
            {
                this.statistic.Book = this.BuyPrice_Level.Count + this.SellPrice_Level.Count;
                this.statistic.LastEventTime = this.LastEventTime;
               
                int stopCount = 0, activeCount = 0;
                foreach (var index in this.OrderIndexer.Values)
                {
                    if (index.IsStopOrder)
                        stopCount++;
                    else
                        activeCount++;
                } 
                this.statistic.StopOrders = stopCount;
                this.statistic.ActiveOrders = activeCount;

                return this.statistic;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"GetStatistic : {ex.Message}");
                throw ex;
            }
            finally
            {
                match_semaphore.Release();
            }

        }


    }
}
