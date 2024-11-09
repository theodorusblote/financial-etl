import yfinance as yf
import ta
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

def extract_data(ticker, period, interval):
    """
    Extracts historical data for a given ticker.

    Args:
        ticker (str): Ticker symbol.
        period (str): Time period.
        interval (str): Data interval.

    Returns:
        pd.DataFrame: Dataframe containing historical data.
    """
    ticker = yf.Ticker(ticker)
    return ticker.history(period=period, interval=interval)

def transform_data(df):
    """
    Transforms data by adding technical indicators.

    Args:
        df (pd.DataFrame): Dataframe containing historical data.

    Returns:
        pd.DataFrame: Dataframe containing transformed data.
    """
    # Add features
    df['Close Lag'] = df['Close'].shift(1)
    df['Volume Lag'] = df['Volume'].shift(1)
    df['50-Day SMA'] = ta.trend.SMAIndicator(close=df['Close'], window=50).sma_indicator()
    df['200-Day SMA'] = ta.trend.SMAIndicator(close=df['Close'], window=200).sma_indicator()
    df['RSI'] = ta.momentum.RSIIndicator(close=df['Close'], window=14).rsi()
    df['MACD'] = ta.trend.MACD(close=df['Close']).macd()
    df['ATR'] = ta.volatility.AverageTrueRange(high=df['High'], low=df['Low'], close=df['Close'], window=14).average_true_range()
    df['OBV'] = ta.volume.OnBalanceVolumeIndicator(close=df['Close'], volume=df['Volume']).on_balance_volume()

    # Drop NaN values resulting from calculations
    df.dropna(inplace=True)

    return df

def load_data(df):
    """
    Loads DataFrame into a PostgreSQL database.

    Args:
        df (pd.DataFrame): Dataframe containing transformed data.
    """
    # Load environment variables
    load_dotenv()

    DB_USERNAME = os.getenv('DB_USERNAME')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')

    # Create SQL engine
    engine = create_engine(f'postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Load DataFrame into SQL database
    df.to_sql('stock_data', con=engine, if_exists='replace', index=False)
