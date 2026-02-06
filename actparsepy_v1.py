import pandas as pd
dfBogdan = pd.read_csv("/home/users/swarnick/010218_BA.csv")
dfRust = pd.read_csv("/home/users/swarnick/itch_files/itch_parser/data/010218/BA.csv")
dfRust[['price', 'executed_price', 'bid', 'ask', 'spread']] = dfRust[['price', 'executed_price', 'bid', 'ask', 'spread']]/10000
dfRust[['typ', 'buy_sell']] = dfRust[['typ', 'buy_sell']].astype(pd.Int64Dtype())

f = lambda x: chr(x) if isinstance(x, int) else x
dfRust[['typ', 'buy_sell']] = dfRust[['typ', 'buy_sell']].map(f)
dfBogdan.rename(columns={'type':'typ', 'seconds':'timestamp', 'orn':'orrf', 'side':'buy_sell', 'current bid':'bid', 'current ask':'ask', 'ask depth':'ask_depth', 'bid depth':'bid_depth'}, inplace=True)
# dfRust.drop(columns=['executed_shares', 'executed_price', 'new_orff', 'cancelled_shares'], inplace=True)
# dfBogdan.drop(columns=['shares_remaining'], inplace=True)
dfRust.loc[0, 'spread'] = pd.NA
dfRust.loc[dfRust['bid'] == 0, 'bid'] = pd.NA
dfRust.loc[dfRust['ask'] == 0, 'ask'] = pd.NA

dfRust[["buy_sell", "price"]] = dfRust.groupby("orrf")[["buy_sell", "price"]].ffill()
dfRust['current_orrf'] = dfRust['new_orff'].where(dfRust['new_orff'].notnull(), dfRust["orrf"])
dfRust[["buy_sell", "price"]] = dfRust.groupby("current_orrf")[["buy_sell", "price"]].ffill()
dfRust[["buy_sell", "price"]] = dfRust.groupby("orrf")[["buy_sell", "price"]].ffill()
dfRust.drop(columns=["current_orrf"], inplace=True)

dfRust["price"] = dfRust["executed_price"].where(dfRust["executed_price"].notnull(), dfRust["price"])
dfRust["shares"] = dfRust["executed_shares"].where(dfRust["executed_shares"].notnull(), dfRust["shares"])
dfRust["shares"] = dfRust["cancelled_shares"].where(dfRust["cancelled_shares"].notnull(), dfRust["shares"])

dfBogdan["nexttyp"] = dfBogdan["typ"].shift(-1)
dfBogdan["nexttime"] = dfBogdan["timestamp"].shift(-1)
dfBogdan.loc[dfBogdan["typ"]=="U", "orrf"] = dfBogdan.loc[dfBogdan["typ"]=="U", "orrf"].shift()
dfBogdan = dfBogdan.loc[(dfBogdan["nexttyp"] != "U") | (dfBogdan["typ"] != "U") | (dfBogdan["nexttime"] != dfBogdan["timestamp"])]
dfBogdan.drop(columns=["nexttyp", "nexttime"], inplace=True)

dfRust['from'] = 'Rust'
dfBogdan['from'] = 'Bogdan'
dfRust = dfRust.sort_values(by=["timestamp", "orrf", "typ"]).reset_index(drop=True)
dfBogdan = dfBogdan.sort_values(by=["timestamp", "orrf", "typ"]).reset_index(drop=True)
df = pd.concat([dfRust, dfBogdan])
df = df.reset_index().sort_values(['index', 'timestamp', 'orrf', 'typ'])
#diff = df.drop_duplicates(['typ', 'timestamp', 'orrf', 'shares', 'price', 'bid', 'ask', 'spread', 'ask_depth', 'bid_depth', 'depth'], keep=False)
diff = df.drop_duplicates(['index', 'typ', 'timestamp', 'orrf', 'price', 'bid', 'ask'], keep=False)
diff.head()
print("If the previous dataframe.head() returned an empty dataframe, \n then the type, timestamp, order reference number, price, \n bid, and ask are the same between the two datasets.")