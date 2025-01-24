from dotenv import load_dotenv
from parser_reddit.db_reddit import DbReddit
from parser_reddit.search import RedditPostsSearch
from utils.func import *
from threading import Thread

load_dotenv(override=True)

def create_db_tables():
    DbReddit().create()

def run_tread(symbols):
    for symbol in symbols:
        type_posts = ['relevance','hot','top']
        for type_post in type_posts:
            try:
                print('----',symbol,', type=',type_post)
                RedditPostsSearch().parse(symbol, type_post)
            except Exception as ex:
                print(ex)

def main():

    all_symbols = ['AAPL', 'NVDA', 'MSFT', 'AMZN', 'META', 'TSLA', 'GOOGL', 'AVGO', 'GOOG', 'BRK.B', 'JPM', 'LLY', 'V', 'XOM', 'UNH', 'MA', 'COST', 'HD', 'WMT', 'PG', 'NFLX', 'JNJ', 'BAC', 'CRM', 'ABBV', 'CVX', 'ORCL', 'WFC', 'MRK', 'KO', 'CSCO', 'NOW', 'ACN', 'TMO', 'ISRG', 'IBM', 'LIN', 'PEP', 'MCD', 'GE', 'AMD', 'ABT', 'GS', 'DIS', 'ADBE', 'PM', 'CAT', 'QCOM', 'TXN', 'AXP', 'MS', 'INTU', 'VZ', 'BKNG', 'RTX', 'T', 'SPGI', 'AMAT', 'DHR', 'C', 'PFE', 'LOW', 'PLTR', 'BLK', 'AMGN', 'NEE', 'BSX', 'HON', 'PGR', 'UNP', 'UBER', 'CMCSA', 'TJX', 'COP', 'ETN', 'SYK', 'BX', 'BA', 'ANET', 'ADP', 'FI', 'MU', 'PANW', 'DE', 'GILD', 'SCHW', 'BMY', 'MDT', 'GEV', 'ADI', 'VRTX', 'SBUX', 'TMUS', 'KKR', 'MMC', 'LMT', 'LRCX', 'PLD', 'KLAC', 'CB', 'CEG', 'UPS', 'INTC', 'PYPL', 'SO', 'ELV', 'AMT', 'EQIX', 'MO', 'TT', 'PH', 'ICE', 'DUK', 'NKE', 'CDNS', 'CME', 'APH', 'CRWD', 'SHW', 'SNPS', 'AON', 'CI', 'PNC', 'CMG', 'MSI', 'WM', 'MDLZ', 'EOG', 'MMM', 'WELL', 'USB', 'TDG', 'MCK', 'APO', 'ZTS', 'MCO', 'AJG', 'COF', 'WMB', 'CL', 'REGN', 'EMR', 'APD', 'ORLY', 'ITW', 'GD', 'BDX', 'CTAS', 'CVS', 'NOC', 'MAR', 'OKE', 'TFC', 'CSX', 'ADSK', 'FDX', 'TGT', 'SLB', 'BK', 'HLT', 'FTNT', 'ECL', 'RCL', 'KMI', 'ABNB', 'VST', 'CARR', 'PCAR', 'FCX', 'HCA', 'GM', 'ROP', 'DLR', 'NSC', 'NXPI', 'AZO', 'TRV', 'JCI', 'SRE', 'AMP', 'SPG', 'AFL', 'WDAY', 'AEP', 'HWM', 'URI', 'MET', 'CMI', 'ALL', 'CPRT', 'PWR', 'PSX', 'ROST', 'GWW', 'MPC', 'MSCI', 'O', 'NEM', 'TRGP', 'DFS', 'PAYX', 'AIG', 'PSA', 'D', 'FICO', 'BKR', 'VLO', 'PEG', 'TEL', 'FAST', 'RSG', 'DHI', 'AXON', 'CTVA', 'KMB', 'FIS', 'PRU', 'A', 'COR', 'LULU', 'DAL', 'HES', 'AME', 'LHX', 'CBRE', 'EW', 'KVUE', 'IT', 'F', 'CCI', 'EXC', 'VRSK', 'KR', 'GEHC', 'CTSH', 'GLW', 'XEL', 'OTIS', 'PCG', 'IR', 'SYY', 'ACGL', 'VMC', 'ODFL', 'KDP', 'UAL', 'RMD', 'ETR', 'OXY', 'YUM', 'WAB', 'MNST', 'IDXX', 'HUM', 'IQV', 'CHTR', 'EA', 'LEN', 'DELL', 'FANG', 'MLM', 'GRMN', 'GIS', 'DXCM', 'MTB', 'NDAQ', 'EFX', 'ED', 'HIG', 'DD', 'ROK', 'WTW', 'IRM', 'EXR', 'DECK', 'EBAY', 'EQT', 'CNC', 'HPQ', 'WEC', 'MCHP', 'AVB', 'VICI', 'ANSS', 'RJF', 'CAH', 'MPWR', 'TTWO', 'CSGP', 'HPE', 'FITB', 'NUE', 'XYL', 'STZ', 'KEYS', 'DOW', 'TSCO', 'STT', 'GDDY', 'PPG', 'GPN', 'FTV', 'MTD', 'BR', 'CCL', 'SYF', 'DOV', 'TPL', 'HAL', 'SW', 'CHD', 'KHC', 'CPAY', 'NVR', 'DTE', 'VLTO', 'TROW', 'CDW', 'AEE', 'BRO', 'NTAP', 'DVN', 'HBAN', 'VTR', 'AWK', 'PPL', 'ADM', 'WST', 'TYL', 'EIX', 'PHM', 'WAT', 'EQR', 'HUBB', 'ON', 'EXPE', 'HSY', 'ATO', 'PTC', 'TER', 'WDC', 'TDY', 'CINF', 'RF', 'K', 'WY', 'SBAC', 'ZBH', 'CTRA', 'IFF', 'DRI', 'LYV', 'WBD', 'CNP', 'PKG', 'ES', 'NTRS', 'NRG', 'ZBRA', 'CFG', 'LII', 'STE', 'LDOS', 'FSLR', 'STX', 'FE', 'BIIB', 'CBOE', 'CMS', 'LYB', 'LH', 'CLX', 'IP', 'PODD', 'LUV', 'ULTA', 'BLDR', 'COO', 'MKC', 'JBL', 'TRMB', 'SNA', 'ESS', 'EL', 'KEY', 'WRB', 'INVH', 'STLD', 'NI', 'MAA', 'FDS', 'VRSN', 'J', 'PFG', 'PNR', 'CF', 'DGX', 'MAS', 'OMC', 'TPR', 'GPC', 'IEX', 'MOH', 'HOLX', 'BALL', 'BBY', 'TSN', 'BAX', 'ARE', 'ALGN', 'L', 'LNT', 'EG', 'SMCI', 'LVS', 'EXPD', 'AVY', 'FFIV', 'DG', 'KIM', 'SWKS', 'GEN', 'DPZ', 'TXT', 'DLTR', 'EVRG', 'DOC', 'RVTY', 'APTV', 'AMCR', 'JBHT', 'AKAM', 'ROL', 'VTRS', 'POOL', 'SWK', 'EPAM', 'JNPR', 'JKHY', 'UDR', 'CAG', 'KMX', 'CHRW', 'CPT', 'TECH', 'NCLH', 'HST', 'NDSN', 'REG', 'ALLE', 'MRNA', 'INCY', 'ALB', 'BG', 'DAY', 'SJM', 'UHS', 'AIZ', 'EMN', 'BXP', 'FOXA', 'NWSA', 'IPG', 'PAYC', 'SOLV', 'ERIE', 'LKQ', 'GL', 'PNW', 'RL', 'TAP', 'GNRC', 'HSIC', 'APA', 'WBA', 'HRL', 'MOS', 'ENPH', 'LW', 'AOS', 'AES', 'TFX', 'CRL', 'MKTX', 'MTCH', 'HII', 'FRT', 'CE', 'WYNN', 'IVZ', 'CPB', 'HAS', 'DVA', 'MGM', 'CZR', 'BWA', 'MHK', 'FMC', 'BF.B', 'PARA', 'BEN', 'FOX', 'NWS']
    symbols = all_symbols[50:]

    threads_count = 30
    countThread = round(len(symbols) / int(threads_count)) + 1
    for i in func_chunk_array(symbols, countThread):
        Thread(target=run_tread, args=(i,)).start()

    # for symbol in symbols:
    #     type_posts = ['relevance','hot','top']
    #     for type_post in type_posts:
    #         print('----',symbol,', type=',type_post)
    #         RedditPostsSearch().parse(symbol, type_post)
            

if __name__ == "__main__":
    create_db_tables()
    main()