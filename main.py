from trade_client import ClientInterface
import pprint
import datetime as dt
import pickle
import cx_Oracle
import random
import json
import redis
import pandas as pd
import os
import time
import sys
from threading import Thread
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.widgets import Button


class OracleSql(object):
    '''
    Query data from database
    '''

    def __init__(self, pt=False):
        '''
        Initialize database
        '''
        self.host, self.oracle_port = '18.210.64.72', '1521'
        self.db, self.current_schema = 'tdb', 'wind'
        self.user, self.pwd = 'reader', 'reader'
        self.pt = pt

    def __enter__(self):
        '''
        Connect to database
        :return: self
        '''
        self.conn = self.__connect_to_oracle()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def __connect_to_oracle(self):
        '''
        Connect to database
        :return: connection
        '''
        dsn = self.host + ':' + self.oracle_port + '/' + self.db
        try:
            connection = cx_Oracle.connect(self.user, self.pwd, dsn, encoding="UTF-8", nencoding="UTF-8")
            connection.current_schema = self.current_schema
            if self.pt is True:
                print('Connected to Oracle database successful!')
        except Exception:
            print('Failed on connecting to Oracle database!')
            connection = None
        return connection

    def query(self, sql: str):
        '''
        Query data
        '''
        import pandas as pd
        return pd.read_sql(sql, self.conn)

    def execute(self, sql: str):
        '''
        Execute SQL scripts, including inserting and updating

        '''
        self.conn.cursor().execute(sql)
        self.conn.commit()


def getTradingDays(startDate: str, endDate: str) -> list:
    sql = \
        '''
        SELECT
        ''' + '''
	TRADE_DAYS 
    FROM
        asharecalendar 
    WHERE
        S_INFO_EXCHMARKET = 'SSE' 
        AND trade_days BETWEEN {} AND {}
    '''.format(startDate, endDate)
    with OracleSql() as oracle:
        tradingDays = oracle.query(sql)
    return list(tradingDays.TRADE_DAYS)


class SubInterface(ClientInterface):
    def __init__(self, name):
        # self.positions = query_postion()
        # self.trade_pnl(1)
        super(SubInterface, self).__init__(name)
        self.position = None
        self.finish_init = False
        # self.check_balance()

    def init_pnl(self, date):
        # Get last close price using Oracle
        self.ticker_list = [key for key, value in self.position.items()]
        print("初始持仓：", self.ticker_list)
        for contract, content in self.position.items():
            S_INFO_WINDCODE = contract[:6] + ".CFE"
            sql = \
                '''
                SELECT
                    S_DQ_CLOSE 
                FROM
                    "CINDEXFUTURESEODPRICES" 
                WHERE
                    TRADE_DT = {}
                    AND S_INFO_WINDCODE = '{}'
                '''.format(date, S_INFO_WINDCODE)
            with OracleSql() as oracle:
                content["last_close_price"] = float(oracle.query(sql).squeeze())
        self.update_price()

    def update_price(self):
        # Get current price using Redis
        config_file_path = 'config.json'
        with open(config_file_path) as f:
            config_data = json.load(f)
        redis_ip = config_data['redis_ip']
        local_redis = redis.Redis(host=redis_ip, port=6379, db=3,
                                  decode_responses=True)
        stmt = 'SELECT s_info_windcode, fs_mapping_windcode FROM CfuturesContractMapping ' \
               ' WHERE S_INFO_WINDCODE like \'IC0%.CFE\' AND STARTDATE <= {0} AND ENDDATE >= {0}'.format(
            dt.datetime.now().strftime('%Y%m%d'))
        wind_conn = cx_Oracle.connect('reader/reader@18.210.64.72:1521/tdb')
        wind_conn.current_schema = 'WIND'
        df = pd.read_sql(stmt, wind_conn).set_index('S_INFO_WINDCODE')
        fut_map_dict = df['FS_MAPPING_WINDCODE'].to_dict()
        ic00 = float(local_redis.get(fut_map_dict['IC00.CFE']))
        ic01 = float(local_redis.get(fut_map_dict['IC01.CFE']))
        ic02 = float(local_redis.get(fut_map_dict['IC02.CFE']))
        self.position["IC1908_1"]["price"] = ic00
        self.position["IC1908_2"]["price"] = ic00
        self.position["IC1909_1"]["price"] = ic01
        self.position["IC1909_2"]["price"] = ic01
        self.position["IC1912_1"]["price"] = ic02
        self.position["IC1912_2"]["price"] = ic02
        # print(pp.pprint(self.position))
        self.pos_pnl = 0
        for contract, content in self.position.items():
            if contract[-1] == '1':
                self.pos_pnl += (content["price"] - content["last_close_price"]) * float(content["current_vol"]) * 200
            elif contract[-1] == '2':
                self.pos_pnl -= (content["price"] - content["last_close_price"]) * float(content["current_vol"]) * 200
        print("持仓盈亏：", self.pos_pnl)
        self.finish_init = True

    def trade_pnl(self, record):
        if record["futures_direction"] == 1:  # Open new position
            key = record["stock_code"] + '_' + str(record["entrust_direction"])
            self.position[key]["current_vol"] += record["entrust_quantity"]
        elif record["futures_direction"] == 2:  # Close new position
            if record["entrust_direction"] == 1:
                key = record["stock_code"] + "_2"
            elif record["entrust_direction"] == 2:
                key = record["stock_code"] + "_1"
            self.position[key]["current_vol"] -= record["entrust_quantity"]
        else:
            print("-----------------------", record["futures_direction"])

        if not os.path.exists("pnl_adjusted.pkl"):
            with open("pnl_adjusted.pkl", 'wb') as f:
                pickle.dump(0, f)
        with open("pnl_adjusted.pkl", "rb") as f:
            pnl_adjusted = pickle.load(f)
            # Think it in a straight and simple way: when the price rises, we lose money if we long and we earn money if we short.
            if record["entrust_direction"] == 1:
                pnl_adjusted -= record["total_deal_amount"] - self.position[key]["last_close_price"] * 200
            elif record["entrust_direction"] == 2:
                pnl_adjusted += record["total_deal_amount"] - self.position[key]["last_close_price"] * 200
            print("交易盈亏调整:", pnl_adjusted)
        with open("pnl_adjusted.pkl", "wb") as f:
            pickle.dump(pnl_adjusted, f)
        self.pnl_adjusted = pnl_adjusted

    def onOnlySubscribeKnock(self, info):
        # print('on only subscribe knock', info)
        print(pp.pprint(info))
        self.trade_pnl(info)
        self.pnl = self.pos_pnl + self.pnl_adjusted
        print("实时盈亏：", self.pnl)  # Actually, we should update self.pos_pnl here!!!

        # with open(str(random.randint(0, 100000)) + ".pkl", 'wb') as f:
        #     pickle.dump(info, f)

    def onQueryPosition(self, info):
        # print('query position: ', info)
        self.position = info["Position"]
        self.preprocess_contract()

        date = dt.datetime.strftime(dt.datetime.now(), "%Y%m%d")
        tradingDay_list = getTradingDays("20120101", "20191231")
        date_lag1 = tradingDay_list[tradingDay_list.index(date) - 1]

        self.init_pnl(date_lag1)

    def preprocess_contract(self):
        for contract, _ in self.position.items():
            for item, _ in self.position[contract].items():
                if item != "combi_no":
                    self.position[contract][item] = float(self.position[contract][item])
        new_dict = dict()
        for contract, content in self.position.items():
            if contract.startswith("IC19"):
                contract_name = contract[0: 8]
                if contract_name not in new_dict:
                    new_dict[contract_name] = content
                else:
                    temp_dict = dict()
                    for key, value in content.items():
                        temp_dict[key] = new_dict[contract_name][key] + content[key]
                    new_dict[contract_name] = temp_dict
        new_dict.pop("IC1907_2")
        self.position = new_dict


class Position(object):
    def __init__(self):
        self.interface = SubInterface()
        self.position = self.interface.query_position()

    def update_xx_info(self, info):
        pass

    def update_xx2_info(self, info):
        pass
        # self.output(= None

    def output(self):
        print('Helo')


class ButtonHandler():
    def __init__(self, interface: SubInterface):
        self.flag = True
        self.range_s, self.range_e, self.range_step = 0, 1, 0.005
        self.interface = interface

    # 线程函数，用来更新数据并重新绘制图形
    def threadStart(self):
        y = list()
        while self.flag:
            self.pnl = self.interface.pnl_adjusted + self.interface.pos_pnl
            time.sleep(0.01)
            if self.interface.finish_init is False:
                continue
            y.append(self.pnl)
            xdata = list(range(6000))
            ydata = y[-6000:]
            if len(ydata) < 6000:
                if len(ydata) == 0:
                    continue
                empty_digit = 6000 - len(ydata)
                xdata = [i + empty_digit for i in xdata]
                xdata = xdata[: len(ydata)]
            l.set_xdata(xdata)
            l.set_ydata(ydata)
            plt.title("PNL:  " + str(round(self.pnl, 2)), x=-3, y=11.5, fontsize=20)
            plt.draw()

    def mockTradingStart(self):
        with open("Q.pkl", 'rb') as f:
            Q = pickle.load(f)
        for label in "abccdabccdaaaddccbb" * 10:
            time.sleep(10 * random.random())
            print("~" * 80)
            # print(pp.pprint(Q[label]))
            self.interface.trade_pnl(Q[label])
            self.interface.update_price()
            print("实时盈亏：", self.interface.pos_pnl + self.interface.pnl_adjusted)

    def Start(self, event):
        self.flag = True
        # 创建并启动新线程
        t = Thread(target=self.threadStart)
        t.start()
        s = Thread(target=self.mockTradingStart)
        s.start()

    def Stop(self, event):
        self.flag = False

    def Print(self, event):
        self.flag = False
        print("\n" * 3)
        print("-" * 100)
        # print(pp.pprint(self.interface.position))
        print(dt.datetime.now())
        for contract, content in self.interface.position.items():
            print("contract: ", contract, "\t\tvol: ", content["current_vol"], "\t\tprice: ", content["price"],
                  "\t\t last close price: ", content["last_close_price"])
        print("-" * 100)
        print("\n" * 3)


if __name__ == '__main__':
    pp = pprint.PrettyPrinter(indent=4)
    # 10034的O32账号无权限查询8301账户，因此用7043账号查询8301账户
    # account_no = '8302'
    # combi_no = '83023005'
    account_no = '8301'
    combi_no = '8301361'

    reset = input("是否重置浮动盈亏？ 按任意键选择“否”， 输入yes选择“是”\n>>>")
    if reset == "yes":
        os.remove("pnl_adjusted.pkl")

    interface = SubInterface('ufx_trading_hhk')
    interface.init()
    positions = interface.query_position(account_no, combi_no)
    interface.subscribe_knock(combi_no)

    fig, ax = plt.subplots()

    plt.subplots_adjust(bottom=0.2)
    ax = plt.gca()
    ax.spines['left'].set_color('none')
    ax.yaxis.set_ticks_position('right')
    l, = plt.plot([6000,], [0, ] , lw=2)
    plt.grid(color='r', linestyle='--', linewidth=1, alpha=0.3)
    plt.xlim(xmin=0, xmax=6000)
    plt.ylim(ymin=-150000, ymax=150000)
    plt.xticks([600 * _ for _ in range(11)], ['-10', '-9', '-8', '-7', '-6', '-5', '-4', '-3', '-2', '-1', 'now'])
    plt.yticks([-1500000, -1200000, -900000, -600000, -300000, 0, 300000, 600000, 900000, 1200000, 1500000])
    plt.xlabel("Time (min)")
    plt.ylabel("Profit and Loss")
    interface.pos_pnl = 0
    interface.pnl_adjusted = 0
    callback = ButtonHandler(interface)
    axprev = plt.axes([0.71, 0.05, 0.1, 0.075])
    bprev = Button(axprev, 'Stop')
    bprev.on_clicked(callback.Stop)
    axnext = plt.axes([0.6, 0.05, 0.1, 0.075])
    bnext = Button(axnext, 'Start')
    bnext.on_clicked(callback.Start)
    axprint = plt.axes([0.82, 0.05, 0.1, 0.075])
    bprint = Button(axprint, 'Print')
    bprint.on_clicked(callback.Print)
    plt.show()

    ### Mock trading ###
    # with open("Q.pkl", 'rb') as f:
    #     Q = pickle.load(f)
    # for label in "abccdabccdaaaddccbb":
    #     time.sleep(5)
    #     print('\n\n' + "~" * 80)
    #     # print(pp.pprint(Q[label]))
    #     interface.trade_pnl(Q[label])
    #     interface.update_price()
    #     print("实时盈亏：",  interface.pos_pnl + interface.pnl_adjusted)

    # print(pp.pprint(interface.position))
    ####################=
