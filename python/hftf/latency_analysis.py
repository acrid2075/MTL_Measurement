#Authors: Bogdan Mukhametkaliev, Andrew Criddle
#Date of origination: September, 2020
#This work has been inspired partially by Jim Primbs' implementation

import numpy as np
import pandas as pd


def process_file(filename):
    ticker = filename.split('/')[-1][:-4]
    day = filename.split('/')[-2]
    print(f"  Processing {ticker}, {day}")
    #Load-in File:
    dff = pd.read_csv(filename)#,
                #   header = 0, dtype = {'typ': np.int64, 'timestamp': np.int64, 'orrf': np.int64, 'buy_sell':np.int64, 'shares': np.int64,
                #                        'price': np.int64, 'executed_shares': np.int64, 'executed_price': np.int64, 'new_orff': np.int64,
                #                        'cancelled_shares': np.int64, 'bid': np.int64, 'ask': np.int64,
                #                        'spread': np.int64, 'ask_depth': np.int64, 'bid_depth': np.int64,
                #                        'depth': np.int64}, na_values = np.nan)
    dff[['price', 'bid', 'ask', 'spread']] = dff[['price', 'bid', 'ask', 'spread']]/10000
    dff[['typ', 'buy_sell']] = dff[['typ', 'buy_sell']].astype(pd.Int64Dtype())

    f = lambda x: chr(x) if isinstance(x, int) else x
    dff[['typ', 'buy_sell']] = dff[['typ', 'buy_sell']].map(f)

    dff.loc[0, 'spread'] = pd.NA
    dff.loc[dff['bid'] == 0, 'bid'] = pd.NA
    dff.loc[dff['ask'] == 0, 'ask'] = pd.NA

    dff = dff.copy()
    #Market Hours:
    day_start_ns = 9.5 * 60 * 60 * 1e+9
    day_end_ns = 16 * 60 * 60 * 1e+9
    delays = [5000, 10000, 15000, 20000, 25000, 30000, 35000, 40000]#[2**k for k in range(8, 17)]

    dff = dff[(dff.timestamp >= day_start_ns) & (dff.timestamp <= day_end_ns)]

    #Define Quote Book:
    quotes = dff.loc[~dff.timestamp.duplicated(keep = 'last')] #last logistical quote
    quotes.reset_index(drop = True, inplace = True)

    #Define Executes:
    executes = dff.loc[dff['typ'].isin(['E', 'C', 'P']),:] #executions
    executes = executes.loc[~executes.timestamp.duplicated(keep='last')] #last transactional price
    executes.reset_index(drop = True, inplace = True)

    #Merge Filtered:
    filtered_df = pd.merge(executes[['typ', 'timestamp', 'price']], quotes[['typ', 'timestamp', 'bid', 'ask']], 
                        on = 'timestamp', how = 'outer')
    filtered_df.sort_values(by = ['timestamp'], inplace = True)
    filtered_df.reset_index(drop = True, inplace = True)

    #Get Mid-Price for non execution prices:
    na_price_ind = filtered_df.price.isna()
    filtered_df.loc[na_price_ind, ['price']] = (filtered_df[na_price_ind].ask + filtered_df[na_price_ind].bid) / 2

    #Define Signals:
    signals = filtered_df.loc[filtered_df['typ_x'].isin(['E', 'C', 'P']), :] #executions, 'type_x' MODIFY

    eps_delta = 1e-4
    # eps_time = 1e-7
    eps_eff = 1e-5

    #Delay:

    results = []
    trend_length = []

    uptrend = 0
    downtrend = 0

    delay = 0

    delay_one_way = delay

    #Initialize and record efficiency:
    eff_list = []  # efficiency list
    e = 0 # 0 for no efficiency, 1 for efficiency
    eff_list.append(e)

    #Initialize and record direction:
    direction = 1  # 1 is up, -1 is down
    # direction_list = []

    #Initialize and record wealth:
    W = np.zeros(len(signals))
    W[0] = 0 
    prof = np.zeros(len(signals))
    execution_time = day_start_ns - 2 * delay_one_way
    # cont_price = filtered_df[filtered_df.timestamp >= execution_time].reset_index(drop=True).price[0]


    #Intialize and record positions:
    h = np.zeros(len(signals))  # number of shares held
    h[0] = 1.
    desired = np.zeros(len(signals))
    desired[0] = 1.

    #trade count:
    trade_count = 0

    #initial signal price:
    sp = signals.price.iloc[0]
    shares_to_purchase = 0
    in_penalty_vec = []
    in_penalty = False
    in_penalty_vec.append(in_penalty * 1)
    # to_trade_vec = []
    # to_trade_vec.append(0.0)
    direction_vec = []
    direction_vec.append(direction)

    for i in range(1, len(signals)):

        sa = signals.ask.iloc[i]
        sb = signals.bid.iloc[i]

        #Time considerations:
        server_time = signals.timestamp.iloc[i] + delay_one_way
        if signals.timestamp.iloc[i] > execution_time:
            if shares_to_purchase == 2:
                tempdf = filtered_df.loc[signals.index[i - 1]: signals.index[i]]
                ask_exec = tempdf[tempdf['timestamp'] <= execution_time].reset_index(drop=True).ask.values[-1]
                slippage = ask_exec - sp
                trade_count += 1
            elif shares_to_purchase == -2:
                tempdf = filtered_df.loc[signals.index[i - 1]: signals.index[i]]
                bid_exec = tempdf[tempdf['timestamp'] <= execution_time].reset_index(drop=True).bid.values[-1]
                slippage = sp - bid_exec
                trade_count += 1
            else: #shares_to_purchase == 0
                slippage = 0
            h[i] = h[i-1] + shares_to_purchase
            shares_to_purchase = 0
            in_penalty = False
        else:
            h[i] = h[i-1]
            in_penalty = True

        in_penalty_vec.append(int(in_penalty))

        #Price, ask, and bid:
        pp = sp #previous price
        sp = signals.price.iloc[i]
        sa = signals.ask.iloc[i]
        sb = signals.bid.iloc[i]

        #Update Wealth:
        W[i] = W[i-1] + h[i]*(sp - pp) - 2 * slippage
        prof[i] = h[i]*(sp - pp) - 2 * slippage
        slippage = 0

        if direction == 1:  #trend is up

            if (sp - pp) > eps_delta:  #new up move
                if downtrend > 0:
                    trend_length.append(downtrend)
                    downtrend = 0
                uptrend += 1
                # check for efficiency:
                if sp > sa - eps_eff: # (abs(sp - sa) < eps_eff) or (sp > sa):
                    e = 1
                    desired[i] = 1 #desired to go long
                else:  # no efficiency
                    e = 0
                    desired[i] = h[i] #hold old position

            elif (sp - pp) < -eps_delta:  # down, up interval ends
                direction = -1 #update direction
                if uptrend > 0:
                    trend_length.append(uptrend)
                    uptrend = 0
                downtrend += 1
                # check efficiency:
                if sp < sb + eps_eff: # (abs(sp - sb) < eps_eff) or (sp < sb):
                    e = 1
                    desired[i] = -1. #desired to go short
                else:
                    e = 0
                    desired[i] = h[i] #hold old position

            else:  # sideways move
                #check efficiency:
                if sp > sa - eps_eff: # (abs(sp - sa)  < eps_eff) or (sp > sa):
                    e = 1
                    desired[i] = 1. #desired to go long
                else:
                    e = 0
                    desired[i] = h[i] #hold old position

        elif direction == -1:
            if (sp - pp) < -eps_delta:  # new down move
                if uptrend > 0:
                    trend_length.append(uptrend)
                    uptrend = 0
                downtrend += 1
                #check efficiency:
                if sp < sb + eps_eff: # (abs(sp - sb) < eps_eff) or (sp < sb):
                    e = 1
                    desired[i] = -1. #desired to go short
                else:
                    e = 0
                    desired[i] = h[i] #hold old position

            elif (sp - pp) > eps_delta:  # up, interval ends
                direction = 1 #update direction
                if downtrend > 0:
                    trend_length.append(downtrend)
                    downtrend = 0
                uptrend += 1
                #check efficiency
                if sp > sa - eps_eff: # (abs(sp - sa) < eps_eff) or (sp > sa):
                    e = 1
                    desired[i] = 1.
                else:
                    e = 0
                    desired[i] = h[i]

            else:  # sideways move
                #check efficiency:
                if sp < sb + eps_eff: # (abs(sp - sb) < eps_eff) or (sp < sb):
                    e = 1
                    desired[i] = -1. #desired to go short
                else:
                    e = 0
                    desired[i] = h[i] #hold old position
        eff_list.append(e)
        direction_vec.append(direction)
        if (desired[i] == 1) and (h[i] == -1) and (not in_penalty):
            shares_to_purchase = 2.0
            execution_time = server_time + delay_one_way
        elif (desired[i] == -1) and (h[i] == 1) and (not in_penalty):
            shares_to_purchase = -2.0
            execution_time = server_time + delay_one_way
            
    #     else:
    #         shares_to_purchase = 0.0
        
        # to_trade_vec.append(shares_to_purchase)
        # if (i%1000 == 0):
        #     print("   Done for i:", i)
    # print(f"Delay {v} ns")
    # print(f"   Profit=${str(W[-1])[:5]}, ")
    # print(f"   Return is between {str(W[-1] / signals['price'].max() * 100)[:5]}% and {str(W[-1] / signals['price'].min() * 100)[:5]}% per day")
    # print(f"   {trade_count} trades conducted")

    # ticker TEXT, date TEXT, latency value INTEGER, EOD profit FLOAT, STD of profits per trade FLOAT, no. trades INT, maximum executed price FLOAT, minimum executed price FLOAT, average trend length FLOAT, number of trends INT
    results.append([str(ticker), str(day), int(delay), float(W[-1]),  
                    float(np.std(prof)), int(trade_count), float(signals['price'].max()), 
                    float(signals['price'].min()), float(np.mean(trend_length)), len(trend_length), sum(eff_list), len(eff_list)])

    for delay in delays:
        print(f"      Addressing delay {delay} microseconds, {ticker}")
        delay_one_way = delay

        #Initialize and record efficiency:
        eff_list = []  # efficiency list
        e = 0 # 0 for no efficiency, 1 for efficiency
        eff_list.append(e)

        #Initialize and record direction:
        direction = 1  # 1 is up, -1 is down
        # direction_list = []

        #Initialize and record wealth:
        W = np.zeros(len(signals))
        W[0] = 0 
        prof = np.zeros(len(signals))
        execution_time = day_start_ns - 2 * delay_one_way
        # cont_price = filtered_df[filtered_df.timestamp >= execution_time].reset_index(drop=True).price[0]

        #Intialize and record positions:
        h = np.zeros(len(signals))  # number of shares held
        h[0] = 1
        desired = np.zeros(len(signals))
        desired[0] = 1

        #trade count:
        trade_count = 0

        #initial signal price:
        sp = signals.price.iloc[0]
        shares_to_purchase = 0
        in_penalty_vec = []
        in_penalty = False
        in_penalty_vec.append(in_penalty * 1)
        # to_trade_vec = []
        # to_trade_vec.append(0.0)
        direction_vec = []
        direction_vec.append(direction)

        for i in range(1, len(signals)):

            sa = signals.ask.iloc[i]
            sb = signals.bid.iloc[i]
            
            #Time considerations:
            server_time = signals.timestamp.iloc[i] + delay_one_way
            
            #Reconcile Shares held:
            if signals.timestamp.iloc[i] > execution_time:
                if shares_to_purchase == 2:
                    tempdf = filtered_df.loc[signals.index[i - 1]: signals.index[i]]
                    ask_exec = tempdf[tempdf['timestamp'] <= execution_time].reset_index(drop=True).ask.values[-1]
                    slippage = ask_exec - sp
                    trade_count += 1
                elif shares_to_purchase == -2:
                    tempdf = filtered_df.loc[signals.index[i - 1]: signals.index[i]]
                    bid_exec = tempdf[tempdf['timestamp'] <= execution_time].reset_index(drop=True).bid.values[-1]
                    slippage = sp - bid_exec
                    trade_count += 1
                else: #shares_to_purchase == 0
                    slippage = 0
                h[i] = h[i-1] + shares_to_purchase
                shares_to_purchase = 0
                in_penalty = False
            else:
                h[i] = h[i-1]
                in_penalty = True
            
            in_penalty_vec.append(int(in_penalty))
                
            #Price, ask, and bid:
            pp = sp #previous price
            sp = signals.price.iloc[i]
            sa = signals.ask.iloc[i]
            sb = signals.bid.iloc[i]
            
            #Update Wealth:    
            W[i] = W[i-1] + h[i]*(sp - pp) - 2 * slippage
            prof[i] = h[i]*(sp - pp) - 2 * slippage
            slippage = 0

            if direction == 1:  #trend is up

                if (sp - pp) > eps_delta:  #new up move
                    # check for efficiency:
                    if sp > sa - eps_eff: # (abs(sp - sa) < eps_eff) or (sp > sa):
                        e = 1
                        desired[i] = 1 #desired to go long
                    else:  # no efficiency
                        e = 0
                        desired[i] = h[i] #hold old position

                elif (sp - pp) < -eps_delta:  # down, up interval ends
                    direction = -1 #update direction
                    # check efficiency:
                    if sp < sb + eps_eff: # (abs(sp - sb) < eps_eff) or (sp < sb):
                        e = 1
                        desired[i] = -1. #desired to go short
                    else:
                        e = 0
                        desired[i] = h[i] #hold old position

                else:  # sideways move
                    #check efficiency:
                    if sp > sa - eps_eff: # (abs(sp - sa)  < eps_eff) or (sp > sa):
                        e = 1
                        desired[i] = 1. #desired to go long
                    else:
                        e = 0
                        desired[i] = h[i] #hold old position

            elif direction == -1:
                if (sp - pp) < -eps_delta:  # new down move
                    #check efficiency:
                    if sp < sb + eps_eff: # (abs(sp - sb) < eps_eff) or (sp < sb):
                        e = 1
                        desired[i] = -1. #desired to go short
                    else:
                        e = 0
                        desired[i] = h[i] #hold old position

                elif (sp - pp) > eps_delta:  # up, interval ends
                    direction = 1 #update direction
                    #check efficiency
                    if sp > sa - eps_eff: # (abs(sp - sa) < eps_eff) or (sp > sa):
                        e = 1
                        desired[i] = 1.
                    else:
                        e = 0
                        desired[i] = h[i]

                else:  # sideways move
                    #check efficiency:
                    if sp < sb + eps_eff: # (abs(sp - sb) < eps_eff) or (sp < sb):
                        e = 1
                        desired[i] = -1. #desired to go short
                    else:
                        e = 0
                        desired[i] = h[i] #hold old position
                        
            eff_list.append(e)
            direction_vec.append(direction)
            if (desired[i] == 1) and (h[i] == -1) and (not in_penalty):
                shares_to_purchase = 2.0
                execution_time = server_time + delay_one_way
                
            elif (desired[i] == -1) and (h[i] == 1) and (not in_penalty):
                shares_to_purchase = -2.0
                execution_time = server_time + delay_one_way
                
        #     else:
        #         shares_to_purchase = 0.0
            
            # to_trade_vec.append(shares_to_purchase)
            # if (i%1000 == 0):
            #     print("   Done for i:", i)
        # print(f"Delay {v} ns")
        # print(f"   Profit=${str(W[-1])[:5]}, ")
        # print(f"   Return is between {str(W[-1] / signals['price'].max() * 100)[:5]}% and {str(W[-1] / signals['price'].min() * 100)[:5]}% per day")
        # print(f"   {trade_count} trades conducted")

        # ticker TEXT, date TEXT, latency value INTEGER, EOD profit FLOAT, STD of profits per trade FLOAT, no. trades INT, maximum executed price FLOAT, minimum executed price FLOAT, average trend length FLOAT, number of trends INT
        results.append([str(ticker), str(day), int(delay), float(W[-1]),  
                        float(np.std(prof)), int(trade_count), float(signals['price'].max()), 
                        float(signals['price'].min()), float(np.mean(trend_length)), len(trend_length), sum(eff_list), len(eff_list)])
        
    print(f"   Completed {day}, {ticker}")
    return results
                
