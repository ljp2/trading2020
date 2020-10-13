import pandas as pd
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import threading
import time
import datetime


class TradingApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.bars = None
        self.end_date = None

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        event_connect.set()

    def historicalData(self, reqId, bar):
        bartime = bar.date.split()[1]
        self.bars.append((bartime, bar.open, bar.high, bar.low, bar.close, bar.volume))

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        df = pd.DataFrame(self.bars, columns='time open high low close volume'.split()).set_index('time')
        bars_date = '{}'.format(self.end_date.split()[0])

        df.to_pickle('/Users/ljp2/junk/bars/{}.pkl'.format(bars_date))
        # df.to_pickle('C:/junk/bars/{}.pkl'.format(bars_date))

        # df.to_csv(open('C:/junk/bars/{}.csv'.format(bars_date), 'w'))
        # df.to_csv(open('/Users/ljp2/junk/{}.csv'.format(bars_date), 'w'))
        event_datadone.set()

    def get_data(self, contract, queryTime):
        self.bars = []
        self.end_date = queryTime.strftime("%Y%m%d %H:%M:%S")
        app.reqHistoricalData(reqId=1,
                              contract=contract,
                              endDateTime=self.end_date,
                              durationStr='1 D',
                              barSizeSetting='5 mins',
                              whatToShow='TRADES',
                              useRTH=1,
                              formatDate=1,
                              keepUpToDate=False,
                              chartOptions=[])

def websocket_con():
    app.run()


event_connect = threading.Event()
event_datadone = threading.Event()
app = TradingApp()

app.connect("127.0.0.1", 4002, clientId=1)
threading.Thread(target=websocket_con, daemon=True).start()
event_connect.wait()
time.sleep(1)

contract = Contract()
contract.symbol = "SPY"
contract.exchange = "SMART"
contract.secType = "STK"
contract.currency = "USD"

dd = datetime.timedelta(days=1)
query_time_start = datetime.datetime(2020, 10, 9, 16, 30)
for i in range(2):
    queryTime = query_time_start- i * dd
    if queryTime.weekday() <= 4:
        print(queryTime,
              ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][queryTime.weekday()])
        event_datadone.clear()
        app.get_data(contract, queryTime)
        event_datadone.wait()
    time.sleep(10)

time.sleep(1)
app.disconnect()
print('DONE DONE DONE')
