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
        self.bars = []

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        event_connect.set()

    def historicalData(self, reqId, bar):
        barday, bartime = bar.date.split()
        self.bars.append( (barday, bartime, bar.open, bar.high, bar.low, bar.close, bar.average) )


    def historicalDataEnd(self, reqId: int, start: str, end: str):
        event_datadone.set()
        df = pd.DataFrame(self.bars, columns='day time open high low close avg'.split())
        bars_date = '{}'.format(self.end_date.split()[0])
        print(bars_date)
        df.to_csv(open('/Users/ljp2/junk/{}.csv'.format(bars_date), 'w'))

    def get_data(self, contract, queryTime):
        print(queryTime)
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
                              chartOptions=[])  # EClient function to request contract details


def websocket_con():
    app.run()


event_connect = threading.Event()
event_datadone = threading.Event()
app = TradingApp()

app.connect("127.0.0.1", 4002, clientId=1)
threading.Thread(target=websocket_con, daemon=True).start()
event_connect.wait()

contract = Contract()
contract.symbol = "SPY"
contract.exchange = "SMART"
contract.secType = "STK"
contract.currency = "USD"

dd = datetime.timedelta(days=1)
queryTime = datetime.datetime(2020,10,9, 16, 30)
for i in range(5):
    event_datadone.clear()
    app.get_data(contract, queryTime - i*dd)
    event_datadone.wait()


time.sleep(1)
app.disconnect()
print('DONE DONE DONE')
