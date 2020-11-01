using COME.Models;
using COME.Utilities;

using Newtonsoft.Json;

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
        readonly System.Threading.Timer zero_O_Clock_Timer = null;
        readonly System.Threading.Timer one_Sec_Timer = null;

        const string channel_cancellation_req_suffix = ".request.cancellation";
        const string channel_neworder_req_suffix = ".request.neworder";
        const string channel_dump_req_suffix = ".request.dump";

        public readonly HashSet<OrderType> StopOrderTypes = new HashSet<OrderType> { OrderType.StopLimit, OrderType.StopMarket };
        public readonly HashSet<OrderType> MarketOrderTypes = new HashSet<OrderType> { OrderType.StopMarket, OrderType.Market };

        DateTime LastEventTime = DateTime.MinValue;
        decimal last_Trade_Price = 0M;
        long last_Txn_Count = 0, txn_Count = 0;

        readonly SemaphoreSlim match_semaphore = new SemaphoreSlim(1, 1);
        readonly int timeout_In_Millisec = 5000;
        public const decimal Zero = 0M;
        const string all = "all";

        const string match = "match";
        const string cancellation = "cancellation";
        const string updation = "updation";
        const string dump_init = "dump_init";
        const string dump = "dump";
        const string dump_complete = "dump_complete";

        readonly MatchResponse ResponseBuffer;

        readonly List<string> RequestChannels = new List<string>(3);
        readonly SortedDictionary<decimal, Level> BuyPrice_Level = new SortedDictionary<decimal, Level>(new DescendingComparer<decimal>());
        readonly SortedDictionary<decimal, Level> SellPrice_Level = new SortedDictionary<decimal, Level>();


        readonly SortedDictionary<decimal, LinkedList<Order>> Stop_BuyOrdersDict = new SortedDictionary<decimal, LinkedList<Order>>();
        readonly SortedDictionary<decimal, LinkedList<Order>> Stop_SellOrdersDict = new SortedDictionary<decimal, LinkedList<Order>>(new DescendingComparer<decimal>());


        readonly Dictionary<string, OrderPointer> OrderIndexer = new Dictionary<string, OrderPointer>();
        readonly Statistic statistic = new Statistic();

        readonly ConnectionMultiplexer Connection;

        public ME(string symbol, int precision, decimal dustSize)
        {
            this.symbol = symbol;
            this.decimal_precision = precision;
            this.dust_size = dustSize;

            this.ResponseBuffer = new MatchResponse(this.symbol);

            RequestChannels.Add(channel_cancellation_req_suffix);
            RequestChannels.Add(channel_neworder_req_suffix);
            RequestChannels.Add(channel_dump_req_suffix);

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

            foreach (var channelsuffix in RequestChannels)
            {
                this.Connection.GetSubscriber().SubscribeAsync(string.Concat(this.symbol, channelsuffix), async (chan, msg) =>
                 {
                     try
                     {
                         switch (channelsuffix)
                         {
                             case channel_neworder_req_suffix:
                                 {
                                     var order = JsonConvert.DeserializeObject<Order>(msg);

                                     if (order == null)
                                         return;

                                     var validationResult = order.Validate();
                                     if (!validationResult.isValid)
                                         return;

                                     var senitizationResult = order.Senitize(this);
                                     if (!senitizationResult.senitized)
                                         return;

                                     await this.AcceptOrderAndProcessMatchAsync(order);
                                 }
                                 break;

                             case channel_cancellation_req_suffix:
                                 {
                                     await CancleOrderAsync(msg);
                                 }
                                 break;

                             case channel_dump_req_suffix:
                                 //ToDoDump
                                 break;

                             default:
                                 Console.WriteLine($"SubscribeAsync : Unknown Channel : {chan} : \nMsg : {msg} ");
                                 break;
                         }
                     }
                     catch (Exception ex)
                     {
                         Console.WriteLine($"SubscribeAsync : Channel : {chan} : \nMsg : {msg}  : \nEx : {ex.Message}");
                     }



                 }).ConfigureAwait(false).GetAwaiter().GetResult();
            }


            var currentTime = DateTime.UtcNow;


            var dueDay = currentTime == currentTime.Date ? currentTime.Date : currentTime.Date.AddDays(1);
            zero_O_Clock_Timer = new Timer(CancelAll_DO_Orders_Callback, null, TimeSpan.FromMilliseconds((dueDay - currentTime).TotalMilliseconds), TimeSpan.FromDays(1)); //every day 
            one_Sec_Timer = new Timer(One_Sec_Timer_Callback, null, TimeSpan.FromMilliseconds(timeout_In_Millisec), TimeSpan.FromSeconds(1)); //every sec 
        }



        public async Task<(bool isProcessed, RequestStatus requestStatus, string message)> AcceptOrderAndProcessMatchAsync(Order order, bool accuireLock = true, bool force = false)
        {
            if (accuireLock && !await match_semaphore.WaitAsync(force ? timeout_In_Millisec * 10 : timeout_In_Millisec)) //for overload protection
                return (false, RequestStatus.Timeout, $"rejected : timeout of {timeout_In_Millisec} millisec elapsed.");


            try
            {

                if (OrderIndexer.ContainsKey(order.ID))
                    return (false, RequestStatus.Rejected, $"rejected : duplicate order id : {order.ID}.");


                this.statistic.Submission++;
                var currentTimestamp = DateTime.UtcNow;


                this.ResponseBuffer.EventTS = currentTimestamp;
                this.ResponseBuffer.EventID = order.ID;
                this.ResponseBuffer.EventType = match;

                if (StopOrderTypes.Contains(order.Type))
                {
                    if (order.IsStopActivated == false) //new stop order
                    {
                        if (order.Side == OrderSide.Buy)
                        {
                            order.Status = OrderStatus.Accepted;
                            order.AcceptedOn = currentTimestamp;
                            order.ModifiedOn = currentTimestamp;

                            this.ResponseBuffer.UpdatedBuyOrders.Add((Order)order.Clone());

                            if (order.TriggerPrice <= last_Trade_Price) // buy order :  market price is already breaching trigger price 
                            {
                                order.IsStopActivated = true;
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

                            this.ResponseBuffer.UpdatedSellOrders.Add((Order)order.Clone());

                            if (order.TriggerPrice >= last_Trade_Price) // sell order :  market price is already breaching trigger price 
                            {
                                order.IsStopActivated = true;
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
                    this.ResponseBuffer.UpdatedBuyOrders.Add((Order)order.Clone());

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
                                    this.ResponseBuffer.UpdatedSellOrders.Add(sellOrder);

                                    level.TotalSize -= sellOrder.PendingQuantity;
                                    this.ResponseBuffer.UpdatedSellOrderBook[sellOrder.Price] = level.TotalSize;

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
                                    this.ResponseBuffer.UpdatedSellOrderBook[sellOrder.Price] = level.TotalSize;

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
                                    this.ResponseBuffer.UpdatedBuyOrders.Add((Order)order.Clone());
                                    this.ResponseBuffer.UpdatedSellOrders.Add((Order)sellOrder.Clone());
                                    this.ResponseBuffer.NewTrades.Add(trade);

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
                            this.ResponseBuffer.UpdatedBuyOrders.Add(order);
                        }
                        else
                        {

                            if (BuyPrice_Level.TryGetValue(order.Price, out var level))
                            {
                                level.Orders.AddLast(order);

                                level.TotalSize += order.PendingQuantity;
                                this.ResponseBuffer.UpdatedBuyOrderBook[order.Price] = level.TotalSize;
                            }
                            else
                            {
                                level = new Level { Orders = new LinkedList<Order>(new List<Order> { order }), TotalSize = order.PendingQuantity };

                                BuyPrice_Level[order.Price] = level;
                                this.ResponseBuffer.UpdatedBuyOrderBook[order.Price] = level.TotalSize;
                            }

                            OrderIndexer.TryAdd(order.ID, new OrderPointer { IsStopOrder = false, Price = order.Price, Side = order.Side });
                        }

                    }

                }
                else
                {
                    this.ResponseBuffer.UpdatedSellOrders.Add((Order)order.Clone());
                    if (!(order.TimeInForce == OrderTimeInForce.FOK && BuyPrice_Level.TakeWhile(x => x.Key >= order.Price).Sum(x => x.Value.TotalSize) < order.PendingQuantity))
                        while (order.PendingQuantity > Zero && BuyPrice_Level.Count > 0)
                        {
                            var possibleMatches = BuyPrice_Level.FirstOrDefault();
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
                                    this.ResponseBuffer.UpdatedBuyOrders.Add(buyOrder);

                                    level.TotalSize -= buyOrder.PendingQuantity;
                                    this.ResponseBuffer.UpdatedBuyOrderBook[buyOrder.Price] = level.TotalSize;

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
                                    this.ResponseBuffer.UpdatedBuyOrderBook[buyOrder.Price] = level.TotalSize;

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

                                    this.ResponseBuffer.UpdatedSellOrders.Add((Order)order.Clone());
                                    this.ResponseBuffer.UpdatedBuyOrders.Add((Order)buyOrder.Clone());
                                    this.ResponseBuffer.NewTrades.Add(trade);

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
                            this.ResponseBuffer.UpdatedSellOrders.Add(order);
                        }
                        else
                        {
                            if (SellPrice_Level.TryGetValue(order.Price, out var level))
                            {
                                level.Orders.AddLast(order);

                                level.TotalSize += order.PendingQuantity;
                                this.ResponseBuffer.UpdatedSellOrderBook[order.Price] = level.TotalSize;
                            }
                            else
                            {
                                level = new Level { Orders = new LinkedList<Order>(new List<Order> { order }), TotalSize = order.PendingQuantity };

                                SellPrice_Level[order.Price] = level;
                                this.ResponseBuffer.UpdatedSellOrderBook[order.Price] = level.TotalSize;
                            }

                            OrderIndexer.TryAdd(order.ID, new OrderPointer { IsStopOrder = false, Price = order.Price, Side = order.Side });

                        }

                    }
                }

                if (min_trade_price != Zero && max_trade_price != Zero)
                    ActivateStopOrdersAndEnqueueForMatch(min_trade_price, max_trade_price);

                Finished:
                this.statistic.CurrentMarketPrice = this.last_Trade_Price;
                this.LastEventTime = currentTimestamp;
                if (accuireLock)
                    await PublishResponseAsync();
                return (true, RequestStatus.Processed, string.Empty);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ProcessMatchAsync : {ex.Message}");

                return (false, RequestStatus.Exception, $"ProcessMatchAsync : {ex.Message}");

            }
            finally
            {
                if (accuireLock)
                    match_semaphore.Release();
            }

        }

        public void ActivateStopOrdersAndEnqueueForMatch(decimal min_trade_price, decimal max_trade_price)
        {

            var currentTime = DateTime.UtcNow;

            //market price going down
            do
            {
                var stopprice_orders = Stop_SellOrdersDict.FirstOrDefault();
                if (min_trade_price > stopprice_orders.Key)
                    break;  //Break as No StopSell Order above Current/Min Trading price;

                foreach (var order in stopprice_orders.Value)
                {
                    order.IsStopActivated = true;
                    order.ModifiedOn = currentTime;

                    OrderIndexer.Remove(order.ID);

                    _ = EnqueueForMatchAsync(order);
                }

                Stop_SellOrdersDict.Remove(stopprice_orders.Key);
            } while (true);

            //market price going up
            do
            {
                var stopprice_orders = Stop_BuyOrdersDict.FirstOrDefault();
                if (stopprice_orders.Key == 0 || stopprice_orders.Key > max_trade_price)
                    break;  //Break as No StopLimitBuy Order below Current/Max Trading price;

                foreach (var order in stopprice_orders.Value)
                {
                    order.IsStopActivated = true;
                    order.ModifiedOn = currentTime;

                    OrderIndexer.Remove(order.ID);

                    _ = EnqueueForMatchAsync(order);
                }

                Stop_BuyOrdersDict.Remove(stopprice_orders.Key);

            } while (true);

        }

        async Task<(bool isProcessed, RequestStatus requestStatus, string message)> EnqueueForMatchAsync(Order order)
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

                this.ResponseBuffer.EventTS = currentTimestamp;
                this.ResponseBuffer.EventID = orderID;
                this.ResponseBuffer.EventType = cancellation;

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
                                this.ResponseBuffer.UpdatedBuyOrders.Add(orderToBeCancelled);
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
                                this.ResponseBuffer.UpdatedSellOrders.Add(orderToBeCancelled);
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
                                    level.Orders.Remove(orderToBeCancelled);
                                }

                                level.TotalSize -= orderToBeCancelled.PendingQuantity;

                                this.ResponseBuffer.UpdatedBuyOrderBook[orderToBeCancelled.Price] = level.TotalSize;

                                orderToBeCancelled.Status = orderToBeCancelled.PendingQuantity == orderToBeCancelled.Quantity ? OrderStatus.FullyCancelled : OrderStatus.PartiallyCancelled;
                                orderToBeCancelled.ModifiedOn = currentTimestamp;
                                this.ResponseBuffer.UpdatedBuyOrders.Add(orderToBeCancelled);
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
                                    level.Orders.Remove(orderToBeCancelled);
                                }

                                level.TotalSize -= orderToBeCancelled.PendingQuantity;
                                this.ResponseBuffer.UpdatedSellOrderBook[orderToBeCancelled.Price] = level.TotalSize;

                                orderToBeCancelled.Status = orderToBeCancelled.PendingQuantity == orderToBeCancelled.Quantity ? OrderStatus.FullyCancelled : OrderStatus.PartiallyCancelled;
                                orderToBeCancelled.ModifiedOn = currentTimestamp;
                                this.ResponseBuffer.UpdatedSellOrders.Add(orderToBeCancelled);
                            }
                        }
                    }
                }
                this.LastEventTime = currentTimestamp;
                await PublishResponseAsync();

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

        public async Task<(bool isProcessed, RequestStatus requestStatus, string message)> UpdateOrderAsync(UpdateOrder updateOrder)
        {
            if (!await match_semaphore.WaitAsync(timeout_In_Millisec)) //for overload protection
                return (false, RequestStatus.Timeout, $"rejected : timeout of {timeout_In_Millisec} millisec elapsed.");
            try
            {
                if (string.IsNullOrWhiteSpace(updateOrder.ID))
                    return (false, RequestStatus.Rejected, $"rejected : invalid order `id` supplied.");
                if (!OrderIndexer.TryGetValue(updateOrder.ID, out var pointer) || pointer == null)
                    return (false, RequestStatus.Rejected, $"rejected : no order with `id` {updateOrder.ID} found.");


                this.statistic.Updation++;
                var currentTimestamp = DateTime.UtcNow;

                this.ResponseBuffer.EventTS = currentTimestamp;
                this.ResponseBuffer.EventID = updateOrder.ID;
                this.ResponseBuffer.EventType = updation;

                Order orderToBeUpdated = default;

                if (pointer.IsStopOrder)
                {
                    if (pointer.Side == OrderSide.Buy)
                    {
                        LinkedList<Order> gropued_list;
                        if (Stop_BuyOrdersDict.TryGetValue(pointer.Price, out gropued_list))
                        {
                            orderToBeUpdated = gropued_list.FirstOrDefault(x => x.ID == updateOrder.ID);
                            if (orderToBeUpdated != null)
                            {
                                if (gropued_list.Count == 1)
                                    Stop_BuyOrdersDict.Remove(pointer.Price);
                                else
                                    gropued_list.Remove(orderToBeUpdated);
                            }
                        }
                    }
                    else
                    {
                        LinkedList<Order> gropued_list;
                        if (Stop_SellOrdersDict.TryGetValue(pointer.Price, out gropued_list))
                        {
                            orderToBeUpdated = gropued_list.FirstOrDefault(x => x.ID == updateOrder.ID);
                            if (orderToBeUpdated != null)
                            {
                                if (gropued_list.Count == 1)
                                    Stop_SellOrdersDict.Remove(pointer.Price);
                                else
                                    gropued_list.Remove(orderToBeUpdated);

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
                            orderToBeUpdated = level.Orders.FirstOrDefault(x => x.ID == updateOrder.ID);
                            if (orderToBeUpdated != null)
                            {
                                if (level.Orders.Count == 1 || level.TotalSize == orderToBeUpdated.PendingQuantity)
                                    BuyPrice_Level.Remove(pointer.Price);
                                else
                                {
                                    level.Orders.Remove(orderToBeUpdated);
                                }

                                level.TotalSize -= orderToBeUpdated.PendingQuantity;
                                this.ResponseBuffer.UpdatedBuyOrderBook[orderToBeUpdated.Price] = level.TotalSize;
                            }
                        }
                    }
                    else
                    {
                        if (SellPrice_Level.TryGetValue(pointer.Price, out var level))
                        {
                            orderToBeUpdated = level.Orders.FirstOrDefault(x => x.ID == updateOrder.ID);
                            if (orderToBeUpdated != null)
                            {
                                if (level.Orders.Count == 1 || level.TotalSize == orderToBeUpdated.PendingQuantity)
                                    SellPrice_Level.Remove(pointer.Price);
                                else
                                {
                                    level.Orders.Remove(orderToBeUpdated);
                                }

                                level.TotalSize -= orderToBeUpdated.PendingQuantity;
                                this.ResponseBuffer.UpdatedSellOrderBook[orderToBeUpdated.Price] = level.TotalSize;
                            }
                        }
                    }
                }



                if (orderToBeUpdated == null)
                    return (false, RequestStatus.Exception, $"Order with id {updateOrder.ID} not found.");

                var matchedQuantity = (orderToBeUpdated.Quantity - orderToBeUpdated.PendingQuantity);

                if (updateOrder.Quantity == Zero || matchedQuantity >= updateOrder.Quantity)
                {
                    orderToBeUpdated.Status = orderToBeUpdated.PendingQuantity == orderToBeUpdated.Quantity ? OrderStatus.FullyCancelled : OrderStatus.PartiallyCancelled;
                    orderToBeUpdated.ModifiedOn = currentTimestamp;
                    this.ResponseBuffer.UpdatedSellOrders.Add(orderToBeUpdated);
                }
                else
                {
                    if (updateOrder.Quantity > Zero)
                    {
                        orderToBeUpdated.Quantity = (decimal)updateOrder.Quantity;
                        orderToBeUpdated.PendingQuantity = orderToBeUpdated.Quantity - matchedQuantity;
                    }
                    if (updateOrder.Price > Zero)
                    {
                        orderToBeUpdated.Price = (decimal)updateOrder.Price;
                    }
                    if (updateOrder.TriggerPrice > Zero)
                    {
                        orderToBeUpdated.TriggerPrice = (decimal)updateOrder.TriggerPrice;
                    }


                    var matchResponse = await AcceptOrderAndProcessMatchAsync(orderToBeUpdated, accuireLock: false);

                    if (!matchResponse.isProcessed)
                    {
                        orderToBeUpdated.Status = orderToBeUpdated.PendingQuantity == orderToBeUpdated.Quantity ? OrderStatus.FullyCancelled : OrderStatus.PartiallyCancelled;
                        orderToBeUpdated.ModifiedOn = currentTimestamp;
                        this.ResponseBuffer.UpdatedSellOrders.Add(orderToBeUpdated);
                    }
                }


                this.LastEventTime = currentTimestamp;
                await PublishResponseAsync();
                return (true, RequestStatus.Processed, string.Empty);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"CancleAsync : {ex.Message}");

                return (false, RequestStatus.Exception, $"CancleAsync : {ex.Message}");

            }
            finally
            {
                match_semaphore.Release();
            }
        }

        async Task PublishResponseAsync()
        {
            await this.Connection.GetSubscriber().PublishAsync(this.response_channel, this.ResponseBuffer.SerializeObject(isFormattingIntended: false));
            this.ResetResponseBuffer();
        }

        void ResetResponseBuffer()
        {
            this.ResponseBuffer.EventID = null;
            this.ResponseBuffer.EventType = null;
            this.ResponseBuffer.EventTS = DateTime.MinValue;
            this.ResponseBuffer.To = all;
            this.ResponseBuffer.UpdatedBuyOrders.Clear();
            this.ResponseBuffer.UpdatedSellOrders.Clear();
            this.ResponseBuffer.NewTrades.Clear();
            this.ResponseBuffer.UpdatedBuyOrderBook.Clear();
            this.ResponseBuffer.UpdatedSellOrderBook.Clear();
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
