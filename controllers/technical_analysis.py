import pandas_ta as ta
import pandas as pd
import numpy as np


class TechnicalAnalysis:
    @staticmethod
    def entry_exit_arrows(df, length=10):
        # Initialize empty columns for the arrows
        df['up_arrow'] = None
        df['down_arrow'] = None

        # Calculate highest and lowest values over the specified length
        df['highest'] = df['high'].rolling(window=length).max()
        df['lowest'] = df['low'].rolling(window=length).min()

        # Initialize previous trend value
        prev_trend = None

        # Loop through the DataFrame and determine entry and exit points
        for i in range(length, len(df)):
            # Determine if the current high is equal or greater than the highest over the length
            if df['high'].iloc[i] >= df['highest'].iloc[i]:
                trend = True  # Uptrend
            elif df['low'].iloc[i] <= df['lowest'].iloc[i]:
                trend = False  # Downtrend
            else:
                trend = prev_trend  # Maintain the previous trend if no condition is met

            # Check for trend change and mark arrows
            if trend:
                df.loc[i, 'up_arrow'] = True  # Up arrow position for uptrend
                df.loc[i, 'down_arrow'] = None  # No down arrow in an uptrend
            else:
                df.loc[i, 'down_arrow'] = True  # Down arrow position for downtrend
                df.loc[i, 'up_arrow'] = None  # No up arrow in a downtrend

            # Update previous trend for the next iteration
            prev_trend = trend

        return df

    @staticmethod
    def calculate_atr(df, period=14):
        high_low = df['high'] - df['low']
        high_close = (df['high'] - df['close'].shift()).abs()
        low_close = (df['low'] - df['close'].shift()).abs()
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = true_range.rolling(window=period).mean()
        return atr

    def calculate_signals(self, df, a=2, c=1):
        df = self.entry_exit_arrows(df)
        df['EMA_1'] = ta.ema(close=df['close'], length=1)
        df['atr'] = self.calculate_atr(df, period=c)
        df['nLoss'] = a * df['atr']
        df['src'] = df['close']
        df['xATRTrailingStop'] = np.nan

        # Initialize xATRTrailingStop
        if len(df) > 0:
            df.loc[df.index[0], 'xATRTrailingStop'] = df.loc[df.index[0], 'src']
        for i in range(1, len(df)):
            if df.iloc[i]['src'] > df.iloc[i - 1]['xATRTrailingStop'] and df.iloc[i - 1]['src'] > \
                    df.iloc[i - 1]['xATRTrailingStop']:
                df.loc[df.index[i], 'xATRTrailingStop'] = max(df.iloc[i - 1]['xATRTrailingStop'],
                                                              df.iloc[i]['src'] - df.iloc[i]['nLoss'])
            elif df.iloc[i]['src'] < df.iloc[i - 1]['xATRTrailingStop'] and df.iloc[i - 1]['src'] < \
                    df.iloc[i - 1]['xATRTrailingStop']:
                df.loc[df.index[i], 'xATRTrailingStop'] = min(df.iloc[i - 1]['xATRTrailingStop'],
                                                              df.iloc[i]['src'] + df.iloc[i]['nLoss'])
            elif df.iloc[i]['src'] > df.iloc[i - 1]['xATRTrailingStop']:
                df.loc[df.index[i], 'xATRTrailingStop'] = df.iloc[i]['src'] - df.iloc[i]['nLoss']
            else:
                df.loc[df.index[i], 'xATRTrailingStop'] = df.iloc[i]['src'] + df.iloc[i]['nLoss']

        df['pos'] = np.where(
            (df['src'].shift(1) < df['xATRTrailingStop'].shift(1)) & (df['src'] > df['xATRTrailingStop']),
            1, np.where(
                (df['src'].shift(1) > df['xATRTrailingStop'].shift(1)) & (df['src'] < df['xATRTrailingStop']), -1,
                np.nan))
        df['pos'] = df['pos'].ffill().fillna(0)
        df['buy_signal'] = (df['src'] > df['xATRTrailingStop']) & (df['EMA_1'] > df['xATRTrailingStop'])
        df['sell_signal'] = (df['src'] < df['xATRTrailingStop']) & (df['EMA_1'] < df['xATRTrailingStop'])

        return df