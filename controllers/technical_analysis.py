import numpy as np
import pandas as pd
import pandas_ta as ta


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
        df = self.compute_strategy(df)
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

    def compute_strategy(self, df, kama_length=1, fastend=2.5, slowend=20, sloma=20, fma_shift=1):
        """
        Replicates the Pine Script logic of Kaufman Adaptive Moving Average (KAMA)
        with a Fast MA (FMA) and SMA applied to a DataFrame containing OHLC data.

        Parameters:
        - df: DataFrame with 'high', 'low', and 'close' columns
        - kama_length: Length for KAMA calculation
        - fastend: Fast EMA smoothing constant
        - slowend: Slow EMA smoothing constant
        - sloma: Period for SMA calculation
        - fma_shift: Shift applied to FMA input

        Returns:
        - DataFrame with 'FMA' and 'SMA' columns
        """

        # Mid-price: HLC3
        xPrice = (df['high'] + df['low'] + df['close']) / 3

        # Efficiency Ratio calculation
        xvnoise = abs(xPrice - xPrice.shift(1))
        nsignal = abs(xPrice - xPrice.shift(kama_length))
        nnoise = xvnoise.rolling(window=kama_length).sum()
        nefratio = np.where(nnoise != 0, nsignal / nnoise, 0)

        # Smoothing Constant
        nfastend = 2 / (fastend + 1)
        nslowend = 2 / (slowend + 1)
        nsmooth = (nefratio * (nfastend - nslowend) + nslowend) ** 2

        # KAMA calculation (like EMA but dynamic smoothing)
        nAMA = [xPrice.iloc[0]]
        for i in range(1, len(xPrice)):
            prev = nAMA[-1]
            smooth = nsmooth[i]
            price = xPrice.iloc[i]
            nAMA.append(prev + smooth * (price - prev))
        nAMA = pd.Series(nAMA, index=df.index)

        # FMA = EMA of shifted KAMA with period 1
        shifted_kama = nAMA.shift(fma_shift)
        bfma = shifted_kama.ewm(span=1, adjust=False).mean()

        # SMA = EMA of unshifted KAMA with `sloma` period
        bsma = nAMA.ewm(span=sloma, adjust=False).mean()
        df['bfma'] = bfma
        df['bsma'] = bsma
        return df
