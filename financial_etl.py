import yfinance as yf

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
