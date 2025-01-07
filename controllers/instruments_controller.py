import pymysql
import pyotp
from pymysql.cursors import DictCursor

from config import db_config


def get_refresh_totp(totp_token):
    totp = pyotp.TOTP(totp_token)
    return totp.now()


class InstrumentsController:
    def __init__(self):
        self.conn = pymysql.connect(**db_config, cursorclass=DictCursor)
        self.create_kite_instruments_table()

    def create_kite_instruments_table(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS zerodha_instruments (
                                zerodha_instrument_token INT,
                                zerodha_exchange_token INT,
                                zerodha_trading_symbol VARCHAR(255),
                                zerodha_name VARCHAR(255),
                                zerodha_last_price FLOAT,
                                zerodha_expiry DATE,
                                zerodha_strike FLOAT,
                                zerodha_tick_size FLOAT,
                                zerodha_lot_size INT,
                                zerodha_instrument_type VARCHAR(255),
                                zerodha_segment VARCHAR(255),
                                zerodha_exchange VARCHAR(255)
                            )
                        ''')
            self.conn.commit()

    def clear_kite_instruments(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''TRUNCATE TABLE zerodha_instruments''')
            self.conn.commit()

    def load_kite_instruments(self, kite):
        try:
            all_instruments = kite.instruments()
            insert_query = """INSERT INTO zerodha_instruments (zerodha_instrument_token, zerodha_exchange_token, zerodha_trading_symbol, zerodha_name, zerodha_last_price, zerodha_expiry, zerodha_strike, zerodha_tick_size, zerodha_lot_size, zerodha_instrument_type, zerodha_segment, zerodha_exchange ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            with self.conn.cursor() as cursor:
                for instrument in all_instruments:
                    data = (
                        instrument['instrument_token'],
                        instrument['exchange_token'],
                        instrument['tradingsymbol'],
                        instrument['name'],
                        instrument['last_price'],
                        instrument['expiry'],
                        instrument['strike'],
                        instrument['tick_size'],
                        instrument['lot_size'],
                        instrument['instrument_type'],
                        instrument['segment'],
                        instrument['exchange']
                    )
                    cursor.execute(insert_query, data)
                    self.conn.commit()
            return True, "Zerodha instruments load successful"
        except Exception as e:
            return False, str(e)
