import logging
from datetime import datetime, time

from config import observable_indices
from controllers.PositionsController import PositionsController
from controllers.broker_controller import BrokerController
from controllers.option_chain_controller import OptionChainController
from controllers.technical_analysis import TechnicalAnalysis

broker_controller = BrokerController()
kite = broker_controller.kite_login()
technical_analysis = TechnicalAnalysis()
option_chain_controller = OptionChainController()
positions_controller = PositionsController()
TRADING_END_TIME = time(15, 15)

logging.basicConfig(
    filename='trading_engine.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def refresh_options_to_observe(index):
    ltp = broker_controller.get_ltp_kite(kite, index['token'])
    index['ce_option'] = positions_controller.get_option_for_buying(index['name'], 1, ltp)
    index['pe_option'] = positions_controller.get_option_for_buying(index['name'], 2, ltp)


if __name__ == "__main__":
    while True:
        try:
            current_time = datetime.now().time()
            if current_time > TRADING_END_TIME:
                for index in observable_indices:
                    active_position = positions_controller.check_for_existing_index_position(index['name'])
                    if active_position:
                        for position in active_position:
                            exit_price = broker_controller.get_ltp_kite(kite, position['zerodha_instrument_token'])
                            positions_controller.exit_position(position, exit_price, exit_reason="End of Day")
                break
            for index in observable_indices:
                positions = positions_controller.check_for_existing_index_position(index['name'])
                if not positions:
                    refresh_options_to_observe(index)
                    ce_one_min_df = broker_controller.kite_historic_data(kite, index['ce_option']['zerodha_option'][
                        'zerodha_instrument_token'], 'minute')
                    ce_five_min_df = broker_controller.kite_historic_data(kite, index['ce_option']['zerodha_option'][
                        'zerodha_instrument_token'], '5minute')
                    ce_fifteen_min_df = broker_controller.kite_historic_data(kite, index['ce_option']['zerodha_option'][
                        'zerodha_instrument_token'], '60minute')
                    if ce_one_min_df.iloc[-2].buy_signal and ce_five_min_df.iloc[-1].buy_signal and \
                            ce_fifteen_min_df.iloc[-1].buy_signal:
                        positions_controller.enter_new_position(index['name'], index['ce_option'],
                                                                ce_one_min_df.iloc[-1].close, 1)
                        continue
                    pe_one_min_df = broker_controller.kite_historic_data(kite, index['pe_option']['zerodha_option'][
                        'zerodha_instrument_token'], 'minute')
                    pe_five_min_df = broker_controller.kite_historic_data(kite, index['pe_option']['zerodha_option'][
                        'zerodha_instrument_token'], '5minute')
                    pe_fifteen_min_df = broker_controller.kite_historic_data(kite, index['pe_option']['zerodha_option'][
                        'zerodha_instrument_token'], '60minute')
                    if pe_one_min_df.iloc[-2].buy_signal and pe_five_min_df.iloc[-1].buy_signal and \
                            pe_fifteen_min_df.iloc[-1].buy_signal:
                        positions_controller.enter_new_position(index['name'], index['pe_option'],
                                                                pe_one_min_df.iloc[-1].close, 2)
                        continue

                else:
                    for position in positions:
                        position_one_min_df = broker_controller.kite_historic_data(kite,
                                                                                   position['zerodha_instrument_token'],
                                                                                   'minute')
                        position_five_min_df = broker_controller.kite_historic_data(kite, position[
                            'zerodha_instrument_token'], '5minute')
                        if position_one_min_df.iloc[-2].sell_signal and position_five_min_df.iloc[-1].sell_signal:
                            positions_controller.exit_position(position, position_one_min_df.iloc[-1].close,
                                                               "Strategy Exit")
        except Exception as e:
            logging.error(f"MAIN_FUNCTION ERROR: {e}", exc_info=True)
