import websocket
import json
import pandas as pd
from sqlalchemy import create_engine
import time as time

engine = create_engine('sqlite:///COINS2.db')

endpoint = 'wss://stream.binance.com:9443/ws/!miniTicker@arr'

def on_message(ws,message):
    print('Message: ',message)
    out = json.loads(message)
    print('Out    : ',out)
    #df_import(out)



def df_import(data):
    t1 = time.time()
    ## Transforms out into a database
    df_ = pd.DataFrame(data)
    ## Create a filtered Data frame with the above condition
    df_ = df_[df_['s'].str.endswith('USDT')]
    df_.c = df_.c.astype(float)
    final = df_[['s','E','c']]
    for i in range(len(final)):
        ## iterate 
        row_ = final[i:i+1]
        ## Write to sql just time and close - the name of the sql table
        ## is row_.s.values[0]
        row_[['E','c']].to_sql(row_.s.values[0],engine,index=False,if_exists='append')
        t2 = time.time()
        print(t2-t1)
    


ws = websocket.WebSocketApp(endpoint, on_message=on_message)
ws.run_forever()


