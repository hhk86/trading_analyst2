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
        # self.check_balance()


    def init_pnl(self, date):
        #Get last close price using Oracle
        print(self.ticker_list)
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
        print(pp.pprint(self.position))


        self.pos_pnl = 0
        for contract, content in self.position.items():
            if contract[-1] == '1':
                self.pos_pnl += (content["price"] - content["last_close_price"]) * float(content["current_vol"]) * 200
            elif contract[-1] == '2':
                self.pos_pnl -= (content["price"] - content["last_close_price"]) * float(content["current_vol"]) * 200
        print(self.pos_pnl)


    def trade_pnl(self, record):
        key = record["stock_code"] + '_' + str(record["entrust_direction"]) #
        if record["futures_direction"] == 1: # Open new position
            self.position[key]["current_vol"] += record["entrust_quantity"]
            if not os.path.exists("pnl_adjusted.pkl"):
                with open("pnl_adjusted.pkl", 'wb') as f:
                    pickle.dump(0, f)
            with open("pnl_adjusted.pkl", "rb") as f:
                pnl_adjusted = pickle.load(f)
                print("Old pnl:", pnl_adjusted)
                pnl_adjusted = pnl_adjusted + self.position[key]["last_close_price"] * 200 - record["total_deal_amount"]
                print("New pnl:", pnl_adjusted)
            with open("pnl_adjusted.pkl", "wb") as f:
                pickle.dump(pnl_adjusted, f)
            self.pnl_adjusted = pnl_adjusted


    def onOnlySubscribeKnock(self, info):
        # print('on only subscribe knock', info)
        pp = pprint.PrettyPrinter(indent=4)
        print(pp.pprint(info))
        self.trade_pnl(info)
        print("实时盈亏：",  self.pos_pnl + self.pnl_adjusted)

        # with open(str(random.randint(0, 100000)) + ".pkl", 'wb') as f:
        #     pickle.dump(info, f)


    def onQueryPosition(self, info):
        # print('query position: ', info)
        save = input("Save position as yesterday close position? \nInput today's date to confirm\n>>>")
        date = dt.datetime.strftime(dt.datetime.now(), "%Y%m%d")
        tradingDay_list = getTradingDays("20120101", "20191231")
        date_lag1 = tradingDay_list[tradingDay_list.index(date) - 1]
        self.position = info["Position"]
        self.position.pop("IC1907_2")  # This is a bug
        self.position.pop("IF1908_2")  # Don't consider IF contract now
        self.position.pop("IF1909_1")
        self.position.pop("IF1909_2")
        self.position.pop("IF1912_1")
        self.position.pop("IF1912_2")

        # Current_Vol 还需从字符串转换成浮点型

        if save == date:
            with open( "dailyBalance\\" + date_lag1 + ".pkl", 'wb') as f:
                pickle.dump(self.position, f)
            print("已将当前持仓量储存昨日持仓量")
        else:
            with open("dailyBalance\\" + date_lag1 + ".pkl", 'rb') as f:
                self.position = pickle.load(f)
                print("dailyBalance\\" + date_lag1 + ".pkl")
            print("读取昨日持仓量")
        # print(pp.pprint(self.position))
        self.ticker_list = [key for key, value in self.position.items()]
        # print(pp.pprint(self.position))
        self.init_pnl(date_lag1)
        # self.trade_pnl(0)


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


if __name__ == '__main__':
    pp = pprint.PrettyPrinter(indent=4)
    # 10034的O32账号无权限查询8301账户，因此用7043账号查询8301账户
    # account_no = '8302'
    # combi_no = '83023005'
    account_no = '8301'
    combi_no = '8301361'

    interface = SubInterface('ufx_trading_hhk')

    interface.init()

    # 查询持仓
    positions = interface.query_position(account_no, combi_no)

    # 订阅成交
    interface.subscribe_knock(combi_no)



