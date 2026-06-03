import json
import os
import time
import re
import urllib.parse
import urllib.request
import http.cookiejar
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import etf_core
except Exception as _etf_import_error:
    etf_core = None

try:
    import marketpulse_core
except Exception as _marketpulse_import_error:
    marketpulse_core = None

try:
    import aiinfra_core
except Exception as _aiinfra_import_error:
    aiinfra_core = None

HOST = os.environ.get("HOST", "0.0.0.0")
LOCAL_URL_HOST = os.environ.get("LOCAL_URL_HOST", "localhost")
PORT = int(os.environ.get("PORT", "7722"))
ROOT = Path(__file__).resolve().parent
CACHE_DIR = ROOT / ".cache"
CACHE_DIR.mkdir(exist_ok=True)
SMALLCAP_DB_PATH = ROOT / "smallcaps-database.json"
SCAN_STATE_PATH = ROOT / "scan-state.json"

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36"
QQQ_FALLBACK = ["NVDA", "MSFT", "AAPL", "AMZN", "AVGO", "META", "GOOGL", "GOOG", "TSLA", "COST", "NFLX", "AMD", "PLTR", "ASML", "TMUS", "CSCO", "AZN", "LIN", "PEP", "QCOM", "SHOP", "INTU", "AMAT", "AMGN", "ISRG", "TXN", "BKNG", "PDD", "ARM", "ADBE", "GILD", "HON", "CMCSA", "MU", "PANW", "ADP", "LRCX", "ADI", "VRTX", "MELI", "SBUX", "KLAC", "CEG", "CRWD", "MDLZ", "DASH", "INTC", "CTAS", "ORLY", "REGN", "MAR", "PYPL", "ABNB", "FTNT", "SNPS", "CDNS", "MRVL", "MNST", "CSX", "ADSK", "WDAY", "ROP", "NXPI", "PCAR", "AEP", "CHTR", "PAYX", "ROST", "KDP", "FAST", "EXC", "ODFL", "TEAM", "DDOG", "EA", "BKR", "XEL", "IDXX", "TTWO", "ZS", "FANG", "GEHC", "KHC", "MCHP", "DXCM", "CTSH", "VRSK", "CSGP", "ON", "CDW", "BIIB", "GFS", "MDB", "ANSS", "ILMN", "WBD", "SIRI"]
SP500_SEED = ["NVDA", "MSFT", "AAPL", "AMZN", "AVGO", "META", "GOOGL", "GOOG", "TSLA", "BRK-B", "JPM", "LLY", "V", "XOM", "MA", "UNH", "COST", "WMT", "NFLX", "PG", "JNJ", "HD", "BAC", "ABBV", "KO", "PLTR", "PM", "ORCL", "CRM", "CVX", "CSCO", "IBM", "ABT", "MCD", "GE", "MRK", "WFC", "LIN", "AMD", "DIS", "INTU", "AXP", "T", "GS", "NOW", "QCOM", "CAT", "VZ", "RTX", "PEP", "ISRG", "UBER", "BKNG", "MS", "AMGN", "PGR", "TXN", "SPGI", "SCHW", "C", "AMAT", "NEE", "LOW", "BLK", "HON", "UNP", "CMCSA", "BA", "SYK", "ETN", "ADP", "PFE", "TJX", "BSX", "GILD", "PANW", "DE", "LRCX", "COP", "VRTX", "ADBE", "ADI", "MDT", "CB", "MMC", "PLD", "KLAC", "MU", "NKE", "LMT", "SBUX", "ANET", "SO", "MO", "ICE", "ELV", "DUK", "CME", "BMY", "INTC", "SHW", "AMT"]




def get_small_caps_from_yahoo(limit=500):
    limit = max(10, min(SMALLCAP_MAX_LIMIT, int(limit or 500)))

    fallback = [
        "SOFI","RKLB","IONQ","OPEN","JOBY","ACHR","ASTS","HIMS","HOOD","RGTI","SOUN","RXRX","DNA","BBAI","AI",
        "UPST","AFRM","LCID","QS","ENVX","IREN","CORZ","BITF","CLSK","RIOT","MARA","HUT","WULF","APLD","NBIS",
        "SMR","OKLO","CRDO","AEHR","INDI","LUNR","DUOL","APP","FROG","PATH","CFLT","ESTC","BILL","TOST","MQ",
        "NU","TMDX","INSP","PRCT","AXON","CELH","CROX","ELF","IOT","TENB","ZI","BOX","DBX","GTLB","ALGM","CAMT",
        "COHR","FORM","IPGP","LSCC","MTSI","POWI","RMBS","SMTC","SYNA","TSEM","UCTT","VECO","ACLS","ENPH",
        "SEDG","RUN","NOVA","BE","PLUG","FCEL","CHPT","EVGO","BLNK","ARRY","SHLS","STEM","FLNC","EOSE",
        "VKTX","HALO","EXEL","IONS","ARWR","BEAM","CRSP","EDIT","NTLA","TWST","TXG","PACB","NVAX","AXSM",
        "CYTK","KURA","RARE","VCEL","XENE","ZLAB","CDE","HL","AG","EXK","FSM","SILV","MAG","BTG","EGO",
        "KGC","PAAS","SSRM","AGI","IAG","NGD","SAND","AA","ATI","CLF","CENX","KALU","MT","NUE","STLD",
        "UEC","UUUU","NXE","DNN","AR","RRC","CNX","SM","MTDR","PR","CHRD","CIVI","CRK","KOS","MUR","NOG",
        "BANC","CADE","COLB","FHB","FHN","FITB","HBAN","HWC","KEY","OZK","SNV","UBSI","UMBF","VLY","WAL",
        "ALLY","COOP","ENVA","LC","NAVI","OMF","SLM","ABG","AN","CVNA","GPI","LAD","PAG","SAH","KMX","AAP",
        "AZO","ORLY","RH","BBY","CHWY","ETSY","FIVE","GME","URBN","BOOT","BURL","DKS","FL","LEVI","OLLI",
        "PLAY","SHAK","TXRH","WING","YETI","AAL","ALK","DAL","JBLU","LUV","SAVE","SKYW","ULCC","CPA","ATSG",
        "EXPD","HUBG","KNX","LSTR","MRTN","ODFL","SAIA","SNDR","WERN","ARCB","CHRW","XPO","RXO","MATX","ZIM"
    ]

    out = []
    for s in fallback:
        s = yahoo_symbol(s)
        if s and s not in out:
            out.append(s)

    return out[:limit]


SP500_QQQ_CUSTOM = ['A', 'AAPL', 'ABBV', 'ABNB', 'ABT', 'ACGL', 'ACN', 'ADBE', 'ADI', 'ADM', 'ADP', 'ADSK', 'AEE', 'AEP', 'AES', 'AFL', 'AIG', 'AIZ', 'AJG', 'AKAM', 'ALB', 'ALGN', 'ALL', 'ALLE', 'AMAT', 'AMCR', 'AMD', 'AME', 'AMGN', 'AMP', 'AMT', 'AMZN', 'ANET', 'AON', 'AOS', 'APA', 'APD', 'APH', 'APO', 'APP', 'APTV', 'ARE', 'ARES', 'ATO', 'AVB', 'AVGO', 'AVY', 'AWK', 'AXON', 'AXP', 'AZO', 'BA', 'BAC', 'BALL', 'BAX', 'BBY', 'BDX', 'BEN', 'BF-B', 'BG', 'BIIB', 'BK', 'BKNG', 'BKR', 'BLDR', 'BLK', 'BMY', 'BR', 'BRK-B', 'BRO', 'BSX', 'BX', 'BXP', 'C', 'CAG', 'CAH', 'CARR', 'CASY', 'CAT', 'CB', 'CBOE', 'CBRE', 'CCI', 'CCL', 'CDNS', 'CDW', 'CEG', 'CF', 'CFG', 'CHD', 'CHRW', 'CHTR', 'CI', 'CIEN', 'CINF', 'CL', 'CLX', 'CMCSA', 'CME', 'CMG', 'CMI', 'CMS', 'CNC', 'CNP', 'COF', 'COHR', 'COIN', 'COO', 'COP', 'COR', 'COST', 'CPAY', 'CPB', 'CPRT', 'CPT', 'CRH', 'CRL', 'CRM', 'CRWD', 'CSCO', 'CSGP', 'CSX', 'CTAS', 'CTSH', 'CTVA', 'CVNA', 'CVS', 'CVX', 'D', 'DAL', 'DASH', 'DD', 'DDOG', 'DE', 'DECK', 'DELL', 'DG', 'DGX', 'DHI', 'DHR', 'DIS', 'DLR', 'DLTR', 'DOC', 'DOV', 'DOW', 'DPZ', 'DRI', 'DTE', 'DUK', 'DVA', 'DVN', 'DXCM', 'EA', 'EBAY', 'ECL', 'ED', 'EFX', 'EG', 'EIX', 'EL', 'ELV', 'EME', 'EMR', 'EOG', 'EPAM', 'EQIX', 'EQR', 'EQT', 'ERIE', 'ES', 'ESS', 'ETN', 'ETR', 'EVRG', 'EW', 'EXC', 'EXE', 'EXPD', 'EXPE', 'EXR', 'F', 'FANG', 'FAST', 'FCX', 'FDS', 'FDX', 'FE', 'FFIV', 'FICO', 'FIS', 'FISV', 'FITB', 'FIX', 'FOX', 'FOXA', 'FRT', 'FSLR', 'FTNT', 'FTV', 'GD', 'GDDY', 'GE', 'GEHC', 'GEN', 'GEV', 'GILD', 'GIS', 'GL', 'GLW', 'GM', 'GNRC', 'GOOG', 'GOOGL', 'GPC', 'GPN', 'GRMN', 'GS', 'GWW', 'HAL', 'HAS', 'HBAN', 'HCA', 'HD', 'HIG', 'HII', 'HLT', 'HON', 'HOOD', 'HPE', 'HPQ', 'HRL', 'HSIC', 'HST', 'HSY', 'HUBB', 'HUM', 'HWM', 'IBKR', 'IBM', 'ICE', 'IDXX', 'IEX', 'IFF', 'INCY', 'INTC', 'INTU', 'INVH', 'IP', 'IQV', 'IR', 'IRM', 'ISRG', 'IT', 'ITW', 'IVZ', 'J', 'JBHT', 'JBL', 'JCI', 'JKHY', 'JNJ', 'JPM', 'KDP', 'KEY', 'KEYS', 'KHC', 'KIM', 'KKR', 'KLAC', 'KMB', 'KMI', 'KO', 'KR', 'KVUE', 'L', 'LDOS', 'LEN', 'LH', 'LHX', 'LII', 'LIN', 'LITE', 'LLY', 'LMT', 'LNT', 'LOW', 'LRCX', 'LULU', 'LUV', 'LVS', 'LYB', 'LYV', 'MA', 'MAA', 'MAR', 'MAS', 'MCD', 'MCHP', 'MCK', 'MCO', 'MDLZ', 'MDT', 'MET', 'META', 'MGM', 'MKC', 'MLM', 'MMM', 'MNST', 'MO', 'MOS', 'MPC', 'MPWR', 'MRK', 'MRNA', 'MRSH', 'MS', 'MSCI', 'MSFT', 'MSI', 'MTB', 'MTD', 'MU', 'NCLH', 'NDAQ', 'NDSN', 'NEE', 'NEM', 'NFLX', 'NI', 'NKE', 'NOC', 'NOW', 'NRG', 'NSC', 'NTAP', 'NTRS', 'NUE', 'NVDA', 'NVR', 'NWS', 'NWSA', 'NXPI', 'O', 'ODFL', 'OKE', 'OMC', 'ON', 'ORCL', 'ORLY', 'OTIS', 'OXY', 'PANW', 'PAYX', 'PCAR', 'PCG', 'PEG', 'PEP', 'PFE', 'PFG', 'PG', 'PGR', 'PH', 'PHM', 'PKG', 'PLD', 'PLTR', 'PM', 'PNC', 'PNR', 'PNW', 'PODD', 'POOL', 'PPG', 'PPL', 'PRU', 'PSA', 'PSKY', 'PSX', 'PTC', 'PWR', 'PYPL', 'Q', 'QCOM', 'RCL', 'REG', 'REGN', 'RF', 'RJF', 'RL', 'RMD', 'ROK', 'ROL', 'ROP', 'ROST', 'RSG', 'RTX', 'RVTY', 'SATS', 'SBAC', 'SBUX', 'SCHW', 'SHW', 'SJM', 'SLB', 'SMCI', 'SNA', 'SNDK', 'SNPS', 'SO', 'SOLV', 'SPG', 'SPGI', 'SRE', 'STE', 'STLD', 'STT', 'STX', 'STZ', 'SW', 'SWK', 'SWKS', 'SYF', 'SYK', 'SYY', 'T', 'TAP', 'TDG', 'TDY', 'TECH', 'TEL', 'TER', 'TFC', 'TGT', 'TJX', 'TKO', 'TMO', 'TMUS', 'TPL', 'TPR', 'TRGP', 'TRMB', 'TROW', 'TRV', 'TSCO', 'TSLA', 'TSN', 'TT', 'TTD', 'TTWO', 'TXN', 'TXT', 'TYL', 'UAL', 'UBER', 'UDR', 'UHS', 'ULTA', 'UNH', 'UNP', 'UPS', 'URI', 'USB', 'V', 'VEEV', 'VICI', 'VLO', 'VLTO', 'VMC', 'VRSK', 'VRSN', 'VRT', 'VRTX', 'VST', 'VTR', 'VTRS', 'VZ', 'WAB', 'WAT', 'WBD', 'WDAY', 'WDC', 'WEC', 'WELL', 'WFC', 'WM', 'WMB', 'WMT', 'WRB', 'WSM', 'WST', 'WTW', 'WY', 'WYNN', 'XEL', 'XOM', 'XYL', 'XYZ', 'YUM', 'ZBH', 'ZBRA', 'ZTS', 'ALNY', 'ARM', 'ASML', 'CCEP', 'FER', 'INSM', 'MELI', 'MRVL', 'MSTR', 'PDD', 'SHOP', 'TRI', 'ZS']

THEME_UNIVERSES = {'semiconductor': ['AMAT', 'LRCX', 'KLAC', 'ASML', 'TER', 'ONTO', 'ACLS', 'MKSI', 'ENTG', 'UCTT', 'VECO', 'AEHR', 'FORM', 'COHU', 'ICHR', 'NVDA', 'AMD', 'AVGO', 'MRVL', 'MU', 'TSM', 'ARM', 'ALAB', 'MCHP', 'NXPI', 'ON', 'QRVO', 'SWKS', 'LSCC', 'MPWR', 'DIOD', 'RMBS', 'SMTC', 'POWI', 'SLAB', 'CRUS', 'AMBA', 'CAMT', 'TSEM', 'GFS', 'INTC', 'TXN', 'ADI', 'WDC', 'STX'], 'russell2000': ['AAMI', 'AAOI', 'AAP', 'AARD', 'AAT', 'ABAT', 'ABCB', 'ABEO', 'ABG', 'ABM', 'ABR', 'ABSI', 'ABUS', 'ABX', 'ACA', 'ACAD', 'ACCO', 'ACDC', 'ACEL', 'ACH', 'ACHR', 'ACIC', 'ACIW', 'ACLS', 'ACMR', 'ACNB', 'ACNT', 'ACR', 'ACRE', 'ACRS', 'ACT', 'ACTG', 'ACTU', 'ACU', 'ACVA', 'ADAM', 'ADCT', 'ADEA', 'ADMA', 'ADNT', 'ADPT', 'ADTN', 'ADUS', 'ADV', 'AEHR', 'AEIS', 'AEO', 'AESI', 'AEVA', 'AEYE', 'AFCG', 'AFRI', 'AGIO', 'AGL', 'AGM', 'AGNT', 'AGX', 'AGYS', 'AHCO', 'AHR', 'AHRT', 'AI', 'AII', 'AIN', 'AIOT', 'AIP', 'AIR', 'AIRJ', 'AIRO', 'AIRS', 'AISP', 'AIV', 'AKBA', 'AKR', 'AKTS', 'ALCO', 'ALDX', 'ALEC', 'ALG', 'ALGT', 'ALH', 'ALHC', 'ALIT', 'ALKS', 'ALKT', 'ALLO', 'ALMS', 'ALMU', 'ALNT', 'ALRM', 'ALRS', 'ALT', 'ALTG', 'ALTI', 'ALX', 'AMAL', 'AMBA', 'AMBP', 'AMBQ', 'AMC', 'AMCX', 'AMLX', 'AMN', 'AMPH', 'AMPL', 'AMPX', 'AMR', 'AMRC', 'AMRX', 'AMSC', 'AMSF', 'AMTB', 'AMWD', 'ANAB', 'ANDE', 'ANF', 'ANGI', 'ANGO', 'ANIK', 'ANIP', 'ANNX', 'AOMR', 'AORT', 'AOSL', 'AOUT', 'APAM', 'APEI', 'APGE', 'APLD', 'APLE', 'APOG', 'APPN', 'APPS', 'AQST', 'ARAI', 'ARAY', 'ARCB', 'ARCT', 'ARDT', 'ARDX', 'AREN', 'ARHS', 'ARI', 'ARKO', 'ARL', 'ARLO', 'AROC', 'AROW', 'ARQ', 'ARQT', 'ARR', 'ARRY', 'ARVN', 'ARWR', 'ASAN', 'ASB', 'ASC', 'ASIC', 'ASIX', 'ASLE', 'ASO', 'ASPI', 'ASPN', 'ASTE', 'ASTH', 'ASUR', 'ATEC', 'ATEN', 'ATEX', 'ATKR', 'ATLC', 'ATLN', 'ATLO', 'ATMU', 'ATNI', 'ATOM', 'ATRC', 'ATRO', 'ATYR', 'AUB', 'AUPH', 'AURA', 'AVA', 'AVAH', 'AVAV', 'AVBH', 'AVBP', 'AVD', 'AVIR', 'AVNS', 'AVNT', 'AVNW', 'AVO', 'AVPT', 'AVR', 'AVXL', 'AWR', 'AX', 'AXGN', 'AXSM', 'AZTA', 'AZZ', 'BALY', 'BANC', 'BAND', 'BANF', 'BANR', 'BARK', 'BATRA', 'BATRK', 'BBAI', 'BBBY', 'BBCP', 'BBIO', 'BBNX', 'BBSI', 'BBT', 'BBUC', 'BBW', 'BCAL', 'BCAX', 'BCBP', 'BCC', 'BCML', 'BCO', 'BCPC', 'BCRX', 'BDC', 'BDN', 'BE', 'BEAM', 'BEEP', 'BELFA', 'BELFB', 'BETA', 'BETR', 'BFC', 'BFH', 'BFLY', 'BFS', 'BFST', 'BGC', 'BGS', 'BH', 'BHB', 'BHE', 'BHR', 'BHRB', 'BHVN', 'BIOA', 'BIPC', 'BJRI', 'BKD', 'BKE', 'BKH', 'BKKT', 'BKSY', 'BKTI', 'BKU', 'BKV', 'BL', 'BLBD', 'BLFS', 'BLKB', 'BLMN', 'BLND', 'BLX', 'BLZE', 'BMBL', 'BMI', 'BMRC', 'BNED', 'BNL', 'BNTC', 'BOC', 'BOH', 'BOOM', 'BOOT', 'BORR', 'BOW', 'BOX', 'BPRN', 'BRBS', 'BRCB', 'BRCC', 'BRSL', 'BRSP', 'BRT', 'BRZE', 'BSET', 'BSRR', 'BSVN', 'BTBT', 'BTDR', 'BTMD', 'BTSG', 'BTU', 'BULL', 'BUR', 'BUSE', 'BV', 'BVFL', 'BVS', 'BWB', 'BWFG', 'BWIN', 'BWMN', 'BXC', 'BXMT', 'BY', 'BYND', 'BYRN', 'BZAI', 'BZH', 'CABO', 'CAC', 'CADL', 'CAKE', 'CAL', 'CALM', 'CALX', 'CALY', 'CAPR', 'CARE', 'CARG', 'CARL', 'CARS', 'CASH', 'CASS', 'CATX', 'CATY', 'CBAN', 'CBFV', 'CBK', 'CBL', 'CBLL', 'CBNA', 'CBNK', 'CBRL', 'CBT', 'CBU', 'CBZ', 'CC', 'CCB', 'CCBG', 'CCNE', 'CCOI', 'CCRN', 'CCS', 'CCSI', 'CD', 'CDE', 'CDNA', 'CDNL', 'CDP', 'CDRE', 'CDXS', 'CDZI', 'CECO', 'CELC', 'CENT', 'CENTA', 'CENX', 'CERS', 'CEVA', 'CFBK', 'CFFI', 'CFFN', 'CGEM', 'CGON', 'CHCO', 'CHCT', 'CHEF', 'CHMG', 'CHRS', 'CIA', 'CIFR', 'CIM', 'CIVB', 'CIX', 'CLAR', 'CLB', 'CLBK', 'CLDT', 'CLDX', 'CLFD', 'CLMB', 'CLMT', 'CLNE', 'CLOV', 'CLPR', 'CLPT', 'CLSK', 'CLW', 'CMC', 'CMCL', 'CMCO', 'CMDB', 'CMP', 'CMPR', 'CMPX', 'CMRC', 'CMRE', 'CMT', 'CMTG', 'CNDT', 'CNK', 'CNMD', 'CNNE', 'CNO', 'CNOB', 'CNR', 'CNS', 'CNX', 'CNXN', 'COCO', 'CODI', 'COFS', 'COGT', 'COHU', 'COLL', 'COMP', 'CON', 'COOK', 'CORZ', 'COSO', 'COUR', 'CPF', 'CPK', 'CPRI', 'CPRX', 'CPS', 'CPSS', 'CRAI', 'CRC', 'CRCT', 'CRD-A', 'CRDF', 'CRDO', 'CRGY', 'CRI', 'CRK', 'CRMD', 'CRML', 'CRMT', 'CRNC', 'CRNX', 'CRSP', 'CRSR', 'CRVL', 'CRVS', 'CSPI', 'CSR', 'CSTL', 'CSTM', 'CSV', 'CSW', 'CTBI', 'CTEV', 'CTGO', 'CTKB', 'CTO', 'CTOS', 'CTRE', 'CTRI', 'CTRN', 'CTS', 'CUBI', 'CURB', 'CURI', 'CURV', 'CV', 'CVBF', 'CVCO', 'CVGW', 'CVI', 'CVLG', 'CVLT', 'CVRX', 'CVSA', 'CWAN', 'CWBC', 'CWCO', 'CWH', 'CWK', 'CWST', 'CWT', 'CXDO', 'CXM', 'CXW', 'CYH', 'CYRX', 'CYTK', 'CZFS', 'CZNC', 'CZWI', 'DAKT', 'DAN', 'DAVE', 'DBD', 'DBI', 'DBRG', 'DC', 'DCGO', 'DCH', 'DCO', 'DCOM', 'DCTH', 'DDD', 'DEA', 'DEC', 'DEI', 'DERM', 'DFH', 'DFIN', 'DFTX', 'DGICA', 'DGII', 'DH', 'DHC', 'DHT', 'DIN', 'DIOD', 'DJCO', 'DK', 'DLX', 'DMAC', 'DMRC', 'DNA', 'DNLI', 'DNOW', 'DNTH', 'DNUT', 'DOCN', 'DOLE', 'DOMO', 'DORM', 'DOUG', 'DRH', 'DRUG', 'DRVN', 'DSGN', 'DSGR', 'DSP', 'DX', 'DXPE', 'DY', 'DYN', 'EAT', 'EBC', 'EBF', 'EBMT', 'EBS', 'ECBK', 'ECPG', 'ECVT', 'EDIT', 'EE', 'EEX', 'EFC', 'EFOR', 'EFSC', 'EFSI', 'EGAN', 'EGBN', 'EGHT', 'EGY', 'EHTH', 'EIG', 'ELA', 'ELDN', 'ELMD', 'ELME', 'ELVN', 'EMBC', 'EML', 'ENOV', 'ENR', 'ENS', 'ENSG', 'ENTA', 'ENVA', 'ENVX', 'EOLS', 'EOSE', 'EP', 'EPAC', 'EPC', 'EPM', 'EPRT', 'EPSN', 'EQBK', 'ERAS', 'ERII', 'ESCA', 'ESE', 'ESNT', 'ESOA', 'ESPR', 'ESQ', 'ESRT', 'ETD', 'ETON', 'EU', 'EVC', 'EVCM', 'EVER', 'EVEX', 'EVGO', 'EVH', 'EVI', 'EVLV', 'EVMN', 'EVTC', 'EWTX', 'EXFY', 'EXPO', 'EXTR', 'EYE', 'EYPT', 'FA', 'FATE', 'FBIZ', 'FBK', 'FBLA', 'FBNC', 'FBP', 'FBRT', 'FBYD', 'FC', 'FCAP', 'FCBC', 'FCCO', 'FCF', 'FCFS', 'FCPT', 'FDBC', 'FDMT', 'FDP', 'FEIM', 'FELE', 'FENC', 'FET', 'FF', 'FFAI', 'FFBC', 'FFIC', 'FFIN', 'FG', 'FHTX', 'FIBK', 'FIGS', 'FINW', 'FIP', 'FISI', 'FIVN', 'FIZZ', 'FLD', 'FLG', 'FLGT', 'FLNC', 'FLNG', 'FLOC', 'FLR', 'FLWS', 'FLXS', 'FLY', 'FLYW', 'FLYX', 'FMAO', 'FMBH', 'FMNB', 'FN', 'FNKO', 'FNLC', 'FNWD', 'FOA', 'FOR', 'FORM', 'FORR', 'FOXF', 'FPI', 'FRAF', 'FRBA', 'FRD', 'FRME', 'FRPH', 'FRSH', 'FRST', 'FSBC', 'FSBW', 'FSLY', 'FSP', 'FSS', 'FSTR', 'FSUN', 'FTDR', 'FTK', 'FTLF', 'FTRE', 'FUBO', 'FUL', 'FULC', 'FULT', 'FUN', 'FUNC', 'FVCB', 'FVR', 'FWRD', 'FWRG', 'FXNC', 'GABC', 'GAIA', 'GAMB', 'GATX', 'GBCI', 'GBFH', 'GBTG', 'GBX', 'GCBC', 'GCMG', 'GCO', 'GCT', 'GDOT', 'GDYN', 'GEF', 'GENC', 'GENI', 'GEO', 'GERN', 'GETY', 'GEVO', 'GFF', 'GH', 'GHC', 'GHM', 'GIC', 'GIII', 'GKOS', 'GLNG', 'GLRE', 'GLSI', 'GLUE', 'GNE', 'GNK', 'GNL', 'GNW', 'GO', 'GOCO', 'GOGO', 'GOLD', 'GOLF', 'GOOD', 'GOSS', 'GPGI', 'GPI', 'GPOR', 'GPRE', 'GRAL', 'GRBK', 'GRC', 'GRDN', 'GRND', 'GRNT', 'GRPN', 'GSAT', 'GSBC', 'GSHD', 'GSM', 'GT', 'GTLS', 'GTN', 'GTX', 'GTY', 'GVA', 'GWRS', 'GYRE', 'HAE', 'HAFC', 'HAIN', 'HASI', 'HBB', 'HBCP', 'HBNC', 'HBT', 'HCAT', 'HCC', 'HCI', 'HCKT', 'HCSG', 'HDSN', 'HE', 'HELE', 'HFFG', 'HFWA', 'HG', 'HGV', 'HIFS', 'HIMS', 'HIPO', 'HL', 'HLF', 'HLIO', 'HLIT', 'HLLY', 'HLMN', 'HLX', 'HMN', 'HNI', 'HNRG', 'HNST', 'HNVR', 'HOMB', 'HOPE', 'HOV', 'HP', 'HPK', 'HPP', 'HQI', 'HQY', 'HRI', 'HRMY', 'HROW', 'HRTG', 'HRTX', 'HSHP', 'HSTM', 'HTB', 'HTFL', 'HTH', 'HTLD', 'HTO', 'HTZ', 'HUBG', 'HUMA', 'HURA', 'HURN', 'HUT', 'HVT', 'HWBK', 'HWC', 'HWKN', 'HY', 'HYLN', 'HZO', 'IART', 'IBCP', 'IBEX', 'IBOC', 'IBP', 'IBRX', 'IBTA', 'ICFI', 'ICHR', 'ICUI', 'IDCC', 'IDR', 'IDT', 'IDYA', 'IE', 'IESC', 'IHRT', 'III', 'IIIN', 'IIIV', 'IIPR', 'IKT', 'ILPT', 'IMAX', 'IMKTA', 'IMMR', 'IMNM', 'IMVT', 'IMXI', 'INBK', 'INBX', 'INDB', 'INDI', 'INDV', 'INGN', 'INMB', 'INN', 'INNV', 'INOD', 'INR', 'INSE', 'INSG', 'INSW', 'INTA', 'INV', 'INVA', 'INVX', 'IONQ', 'IOSP', 'IOVA', 'IPAR', 'IPI', 'IRMD', 'IRON', 'IRT', 'IRTC', 'IRWD', 'ISPR', 'ISTR', 'ITGR', 'ITIC', 'ITRI', 'IVR', 'IVT', 'JACK', 'JAKK', 'JANX', 'JBGS', 'JBI', 'JBIO', 'JBLU', 'JBSS', 'JBTM', 'JCAP', 'JELD', 'JILL', 'JJSF', 'JMSB', 'JOBY', 'JOE', 'JOUT', 'JRVR', 'JXN', 'JYNT', 'KAI', 'KALU', 'KALV', 'KBH', 'KE', 'KELYA', 'KFRC', 'KFY', 'KG', 'KGEI', 'KGS', 'KIDS', 'KINS', 'KLC', 'KLIC', 'KLTR', 'KMT', 'KMTS', 'KN', 'KNF', 'KNTK', 'KOD', 'KODK', 'KOP', 'KOPN', 'KOS', 'KREF', 'KRG', 'KRMD', 'KRNY', 'KRO', 'KROS', 'KRRO', 'KRT', 'KRUS', 'KRYS', 'KSS', 'KTB', 'KTOS', 'KULR', 'KURA', 'KW', 'KWR', 'KYMR', 'LAB', 'LADR', 'LAKE', 'LAND', 'LARK', 'LASR', 'LAUR', 'LAW', 'LBRT', 'LBRX', 'LC', 'LCII', 'LCNB', 'LDI', 'LE', 'LEG', 'LEGH', 'LENZ', 'LEU', 'LFCR', 'LFMD', 'LFST', 'LFT', 'LFVN', 'LGIH', 'LGN', 'LGND', 'LIF', 'LILA', 'LILAK', 'LINC', 'LIND', 'LION', 'LIVN', 'LKFN', 'LMAT', 'LMB', 'LMND', 'LMNR', 'LMRI', 'LNN', 'LNSR', 'LNTH', 'LOB', 'LOCO', 'LOVE', 'LPA', 'LPG', 'LPRO', 'LQDA', 'LQDT', 'LRMR', 'LRN', 'LTBR', 'LTC', 'LTH', 'LUCD', 'LUMN', 'LUNG', 'LUNR', 'LVWR', 'LWAY', 'LXEO', 'LXFR', 'LXP', 'LXU', 'LYTS', 'LZ', 'LZB', 'LZM', 'MAC', 'MAGN', 'MAMA', 'MARA', 'MASS', 'MATV', 'MATW', 'MATX', 'MAX', 'MAZE', 'MBC', 'MBI', 'MBIN', 'MBUU', 'MBWM', 'MBX', 'MC', 'MCB', 'MCBS', 'MCFT', 'MCHB', 'MCRI', 'MCS', 'MCW', 'MCY', 'MD', 'MDGL', 'MDV', 'MDWD', 'MDXG', 'MEC', 'MED', 'MEI', 'METC', 'MFA', 'MFIN', 'MG', 'MGEE', 'MGNI', 'MGPI', 'MGRC', 'MGTX', 'MGY', 'MH', 'MHO', 'MIAX', 'MIR', 'MIRM', 'MITK', 'MITT', 'MKTW', 'MLAB', 'MLKN', 'MLP', 'MLR', 'MLYS', 'MMI', 'MMS', 'MMSI', 'MNKD', 'MNPR', 'MNRO', 'MNSB', 'MNTK', 'MOD', 'MOG-A', 'MOV', 'MPAA', 'MPB', 'MPLT', 'MPTI', 'MQ', 'MRBK', 'MRCY', 'MRDN', 'MRTN', 'MRVI', 'MRX', 'MSBI', 'MSEX', 'MSGE', 'MTH', 'MTRN', 'MTRX', 'MTUS', 'MTW', 'MTX', 'MUR', 'MVBF', 'MVIS', 'MVST', 'MWA', 'MXCT', 'MXL', 'MYE', 'MYFW', 'MYGN', 'MYO', 'MYPS', 'MYRG', 'MZTI', 'NABL', 'NAGE', 'NAT', 'NATH', 'NATL', 'NATR', 'NAVI', 'NAVN', 'NB', 'NBBK', 'NBHC', 'NBN', 'NBR', 'NBTB', 'NC', 'NCMI', 'NE', 'NECB', 'NEO', 'NEOG', 'NEON', 'NESR', 'NEWT', 'NEXN', 'NEXT', 'NFBK', 'NFE', 'NG', 'NGNE', 'NGS', 'NGVC', 'NGVT', 'NHC', 'NHI', 'NIC', 'NJR', 'NKSH', 'NKTX', 'NL', 'NLOP', 'NMAX', 'NMIH', 'NMRK', 'NN', 'NNE', 'NNI', 'NNOX', 'NODK', 'NOG', 'NOVT', 'NPB', 'NPCE', 'NPK', 'NPKI', 'NPO', 'NPWR', 'NRC', 'NRDS', 'NRDY', 'NREF', 'NRIM', 'NRIX', 'NSIT', 'NSP', 'NSSC', 'NTB', 'NTCT', 'NTGR', 'NTLA', 'NTST', 'NUS', 'NUTX', 'NUVB', 'NUVL', 'NVAX', 'NVCR', 'NVCT', 'NVEC', 'NVGS', 'NVRI', 'NVTS', 'NWBI', 'NWE', 'NWFL', 'NWN', 'NWPX', 'NX', 'NXDR', 'NXDT', 'NXRT', 'NXT', 'NXXT', 'OABI', 'OBK', 'OBT', 'OCFC', 'OCUL', 'ODC', 'OEC', 'OFG', 'OFIX', 'OFLX', 'OGS', 'OI', 'OII', 'OIS', 'OKLO', 'OLMA', 'OLP', 'OLPX', 'OM', 'OMCL', 'OMDA', 'OMER', 'ONB', 'ONEW', 'ONIT', 'ONT', 'OOMA', 'OPAL', 'OPBK', 'OPCH', 'OPFI', 'OPK', 'OPLN', 'OPRT', 'OPRX', 'OPTU', 'ORA', 'ORC', 'ORGO', 'ORIC', 'ORKA', 'ORN', 'ORRF', 'OSBC', 'OSCR', 'OSG', 'OSIS', 'OSPN', 'OSUR', 'OSW', 'OTTR', 'OUST', 'OUT', 'OVBC', 'OVLY', 'OXM', 'PACB', 'PACK', 'PACS', 'PAGS', 'PAHC', 'PAL', 'PAMT', 'PANL', 'PAR', 'PARR', 'PATK', 'PAX', 'PAYO', 'PAYS', 'PBF', 'PBFS', 'PBH', 'PBI', 'PBYI', 'PCB', 'PCRX', 'PCT', 'PCVX', 'PCYO', 'PD', 'PDEX', 'PDFS', 'PDLB', 'PDM', 'PDYN', 'PEB', 'PEBK', 'PEBO', 'PECO', 'PENG', 'PESI', 'PFBC', 'PFIS', 'PFS', 'PFSI', 'PGC', 'PGEN', 'PGNY', 'PGY', 'PHAT', 'PHIN', 'PHR', 'PI', 'PII', 'PINE', 'PIPR', 'PJT', 'PKBK', 'PKE', 'PKOH', 'PL', 'PLAB', 'PLAY', 'PLBC', 'PLGO', 'PLMR', 'PLOW', 'PLPC', 'PLSE', 'PLTK', 'PLUG', 'PLUS', 'PLX', 'PLXS', 'PMI', 'PMT', 'PMTS', 'PNBK', 'PNRG', 'PNTG', 'POR', 'POWI', 'POWL', 'POWW', 'PPHC', 'PPTA', 'PRA', 'PRAA', 'PRAX', 'PRCH', 'PRCT', 'PRDO', 'PRG', 'PRGS', 'PRIM', 'PRK', 'PRKS', 'PRLB', 'PRM', 'PRME', 'PROP', 'PRSU', 'PRTA', 'PRTH', 'PRVA', 'PSFE', 'PSIX', 'PSMT', 'PSNL', 'PSTL', 'PTCT', 'PTEN', 'PTGX', 'PTLO', 'PTON', 'PUBM', 'PUMP', 'PVLA', 'PWP', 'PXED', 'PZZA', 'QBTS', 'QCRH', 'QDEL', 'QLYS', 'QNST', 'QSI', 'QTRX', 'QTWO', 'QUAD', 'QUBT', 'RAMP', 'RAPP', 'RBB', 'RBBN', 'RBCAA', 'RBKB', 'RC', 'RCAT', 'RCEL', 'RCKT', 'RCKY', 'RCMT', 'RCUS', 'RDN', 'RDNT', 'RDVT', 'RDW', 'REAL', 'REAX', 'REFI', 'RELL', 'RELY', 'REPL', 'REPX', 'RES', 'REX', 'REZI', 'RGCO', 'RGNX', 'RGP', 'RGR', 'RGTI', 'RHLD', 'RHP', 'RICK', 'RIG', 'RIGL', 'RIOT', 'RLAY', 'RLGT', 'RLJ', 'RM', 'RMAX', 'RMBI', 'RMBS', 'RMNI', 'RMR', 'RNAC', 'RNGR', 'RNST', 'ROAD', 'ROCK', 'ROG', 'ROOT', 'RPAY', 'RPC', 'RPD', 'RPT', 'RR', 'RRBI', 'RRR', 'RSI', 'RSVR', 'RUM', 'RUN', 'RUSHA', 'RVLV', 'RVSB', 'RWT', 'RXO', 'RXRX', 'RXST', 'RXT', 'RYAM', 'RYTM', 'RYZ', 'RZLT', 'RZLV', 'SABR', 'SAFE', 'SAFT', 'SAFX', 'SAH', 'SAMG', 'SANA', 'SANM', 'SATL', 'SATS', 'SB', 'SBC', 'SBCF', 'SBFG', 'SBGI', 'SBH', 'SBRA', 'SBSI', 'SCHL', 'SCL', 'SCSC', 'SCVL', 'SD', 'SDGR', 'SDRL', 'SEAT', 'SEG', 'SEI', 'SEM', 'SENEA', 'SEPN', 'SERV', 'SEVN', 'SEZL', 'SFBC', 'SFBS', 'SFIX', 'SFL', 'SFNC', 'SFST', 'SG', 'SGC', 'SGHC', 'SGHT', 'SGRY', 'SHAK', 'SHBI', 'SHEN', 'SHLS', 'SHO', 'SHOO', 'SI', 'SIBN', 'SIEB', 'SIG', 'SIGA', 'SIGI', 'SILA', 'SION', 'SITC', 'SITM', 'SKIL', 'SKIN', 'SKT', 'SKWD', 'SKY', 'SKYH', 'SKYT', 'SKYW', 'SKYX', 'SLAB', 'SLDB', 'SLDE', 'SLDP', 'SLG', 'SLND', 'SLP', 'SLQT', 'SLS', 'SLSN', 'SLVM', 'SM', 'SMA', 'SMBC', 'SMBK', 'SMC', 'SMHI', 'SMID', 'SMP', 'SMPL', 'SMR', 'SMTC', 'SMTI', 'SNBR', 'SNDA', 'SNDX', 'SNEX', 'SNFCA', 'SNWV', 'SOC', 'SONO', 'SOUN', 'SPB', 'SPFI', 'SPHR', 'SPIR', 'SPNT', 'SPOK', 'SPRY', 'SPSC', 'SPT', 'SPWR', 'SPXC', 'SR', 'SRBK', 'SRCE', 'SRRK', 'SRTA', 'SSP', 'SSRM', 'SSTI', 'SSTK', 'STAA', 'STBA', 'STC', 'STEL', 'STEP', 'STGW', 'STIM', 'STNE', 'STNG', 'STOK', 'STRA', 'STRL', 'STRS', 'STRT', 'STRW', 'STRZ', 'STXS', 'SUNS', 'SUPN', 'SVC', 'SVCO', 'SVRA', 'SVV', 'SWBI', 'SWIM', 'SWX', 'SXC', 'SXI', 'SXT', 'SYBT', 'SYNA', 'SYRE', 'TALK', 'TALO', 'TARA', 'TARS', 'TBBK', 'TBCH', 'TBI', 'TBPH', 'TBRG', 'TCBI', 'TCBK', 'TCI', 'TCMD', 'TCX', 'TDAY', 'TDOC', 'TDS', 'TDUP', 'TDW', 'TE', 'TEAD', 'TECX', 'TENB', 'TEX', 'TFIN', 'TG', 'TGLS', 'TGTX', 'TH', 'THFF', 'THR', 'THRM', 'THRY', 'TIC', 'TILE', 'TIPT', 'TITN', 'TK', 'TKNO', 'TLS', 'TLSI', 'TMCI', 'TMDX', 'TMHC', 'TMP', 'TNC', 'TNDM', 'TNET', 'TNGX', 'TNK', 'TNXP', 'TOI', 'TOWN', 'TPB', 'TPC', 'TR', 'TRAK', 'TRC', 'TRDA', 'TREE', 'TRIP', 'TRMK', 'TRN', 'TRNO', 'TRNS', 'TROX', 'TRS', 'TRST', 'TRTX', 'TRUP', 'TRVI', 'TSBK', 'TSHA', 'TSSI', 'TTAM', 'TTEC', 'TTGT', 'TTI', 'TTMI', 'TUSK', 'TVGN', 'TVRD', 'TVTX', 'TWI', 'TWO', 'TWST', 'TXG', 'TXNM', 'TYRA', 'TZOO', 'UAMY', 'UBSI', 'UCB', 'UCTT', 'UE', 'UEC', 'UFCS', 'UFPI', 'UFPT', 'UHT', 'UIS', 'ULCC', 'ULH', 'UMBF', 'UMH', 'UNB', 'UNF', 'UNFI', 'UNIT', 'UNTY', 'UPB', 'UPBD', 'UPST', 'UPWK', 'URBN', 'URGN', 'USAR', 'USAU', 'USCB', 'USGO', 'USLM', 'USNA', 'USPH', 'UTI', 'UTL', 'UTMD', 'UTZ', 'UUUU', 'UVE', 'UVSP', 'UVV', 'VABK', 'VAC', 'VAL', 'VALU', 'VC', 'VCEL', 'VCTR', 'VCYT', 'VECO', 'VEL', 'VERA', 'VERX', 'VGAS', 'VHI', 'VIA', 'VIAV', 'VICR', 'VIR', 'VIRC', 'VISN', 'VITL', 'VLGEA', 'VLY', 'VMD', 'VNDA', 'VOXR', 'VOYG', 'VPG', 'VRDN', 'VRE', 'VREX', 'VRM', 'VRNS', 'VRRM', 'VRTS', 'VSAT', 'VSCO', 'VSEC', 'VSTM', 'VSTS', 'VTEX', 'VTOL', 'VTS', 'VUZI', 'VVX', 'VYGR', 'VYX', 'WABC', 'WAFD', 'WALD', 'WASH', 'WAY', 'WBTN', 'WD', 'WDFC', 'WEAV', 'WERN', 'WEST', 'WEYS', 'WGO', 'WGS', 'WHD', 'WHG', 'WINA', 'WK', 'WKC', 'WLDN', 'WLFC', 'WLTH', 'WLY', 'WMK', 'WNC', 'WNEB', 'WOOF', 'WOR', 'WRBY', 'WRLD', 'WS', 'WSBC', 'WSBF', 'WSFS', 'WSR', 'WT', 'WTBA', 'WTI', 'WTS', 'WTTR', 'WULF', 'WVE', 'WWW', 'WYFI', 'XENE', 'XERS', 'XHR', 'XMTR', 'XNCR', 'XOMA', 'XPEL', 'XPER', 'XPOF', 'XPRO', 'XRN', 'XRX', 'XZO', 'YELP', 'YEXT', 'YORW', 'YOU', 'YSS', 'ZBIO', 'ZD', 'ZETA', 'ZGN', 'ZIP', 'ZUMZ', 'ZVIA', 'ZVRA', 'ZWS', 'ZYME'], 'nuclear': ['CCJ', 'CEG', 'BWXT', 'LEU', 'UEC', 'UUUU', 'NXE', 'DNN', 'SMR', 'OKLO', 'NNE', 'LTBR', 'URG', 'EU', 'GVXXF', 'PALAF', 'URNM', 'URA', 'SRUUF', 'BHP', 'RIO', 'FCUUF', 'ISOIF', 'PTUUF', 'DNN', 'UEC', 'UUUU', 'CCJ', 'NXE', 'SMR', 'OKLO', 'BWXT', 'FLR', 'J', 'ACM', 'GEV', 'ETN', 'EMR', 'HON', 'ROK', 'CEG', 'VST', 'TLN', 'PEG', 'EXC', 'NEE', 'DUK', 'SO'], 'space': ['RKLB', 'LUNR', 'ASTS', 'ACHR', 'JOBY', 'SPIR', 'PL', 'IRDM', 'VSAT', 'GSAT', 'MAXR', 'BA', 'LMT', 'NOC', 'RTX', 'GD', 'TXT', 'HWM', 'TDG', 'HEI', 'CW', 'SPR', 'KTOS', 'AVAV', 'MRCY', 'BWXT', 'LDOS', 'SAIC', 'CACI', 'OSIS', 'AXON', 'AUR', 'SYM', 'SERV', 'TER', 'ROK', 'IRBT', 'ISRG', 'ZBRA', 'CGNX', 'HLX', 'OII', 'TXT', 'AIR', 'ATRO', 'ERJ', 'RYCEY', 'EADSY', 'BAESY', 'SAABY', 'FINMY', 'DRS', 'HII', 'TDY', 'LHX', 'MOG-A'], 'aiinfra': ['NVDA', 'AVGO', 'AMD', 'MRVL', 'MU', 'TSM', 'ARM', 'ALAB', 'ANET', 'CSCO', 'JNPR', 'NTAP', 'PSTG', 'WDC', 'STX', 'SMCI', 'DELL', 'HPE', 'VRT', 'ETN', 'PWR', 'POWL', 'STRL', 'EME', 'FIX', 'IESC', 'WCC', 'GEV', 'CEG', 'VST', 'TLN', 'NEE', 'DUK', 'SO', 'PEG', 'EXC', 'OKLO', 'SMR', 'BWXT', 'CCJ', 'LEU', 'APLD', 'IREN', 'CORZ', 'CLSK', 'RIOT', 'MARA', 'WULF', 'NBIS', 'CRWV', 'ORCL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'PLTR', 'SNOW', 'DDOG', 'NET', 'CRWD', 'ZS', 'MDB', 'ESTC', 'CFLT', 'NOW', 'CRM', 'ADBE', 'SNPS', 'CDNS', 'KLAC', 'AMAT', 'LRCX', 'ASML', 'TER', 'ONTO', 'ACLS']}

MAX_BATCH_SYMBOLS = int(os.environ.get("MAX_BATCH_SYMBOLS", "250"))
BATCH_WORKERS = int(os.environ.get("BATCH_WORKERS", "20"))

_cookiejar = http.cookiejar.CookieJar()
_opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_cookiejar))
_crumb = None
_crumb_time = 0

def cache_path(name):
    safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in name)
    return CACHE_DIR / (safe + ".json")

def read_cache(name, max_age_sec):
    p = cache_path(name)
    if not p.exists():
        return None
    if time.time() - p.stat().st_mtime > max_age_sec:
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

def read_cache_any(name):
    try:
        p = cache_path(name)
        if not p.exists():
            return None
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

def write_cache(name, data):
    try:
        cache_path(name).write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass

def mark_fallback_row(row, source, reason):
    out = dict(row or {})
    out["fallbackData"] = True
    out["source"] = source
    out.setdefault("errors", {})
    if isinstance(out["errors"], dict):
        out["errors"]["fallbackReason"] = reason
    return out

def read_stale_stock(symbol):
    symbol = yahoo_symbol(symbol)
    return read_cache_any("stock_fallback_" + symbol) or read_cache_any("stock_" + symbol)

def load_scan_state():
    try:
        data = json.loads(SCAN_STATE_PATH.read_text(encoding="utf-8-sig"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def save_scan_state(scan_type, rows, meta=None):
    scan_type = (scan_type or "").strip().lower()
    if scan_type not in ("screener", "weekbreakout"):
        raise ValueError("Unknown scan state type")
    if not isinstance(rows, list):
        raise ValueError("rows must be a list")
    data = load_scan_state()
    data[scan_type] = {
        "savedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "count": len(rows),
        "meta": meta if isinstance(meta, dict) else {},
        "rows": rows[:10000],
    }
    tmp = SCAN_STATE_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    tmp.replace(SCAN_STATE_PATH)
    return data[scan_type]

def http_get(url, timeout=25, accept="application/json,text/plain,*/*"):
    req = urllib.request.Request(url, headers={
        "User-Agent": UA,
        "Accept": accept,
        "Accept-Language": "en-US,en;q=0.9,da;q=0.8",
        "Connection": "close",
    })
    with _opener.open(req, timeout=timeout) as resp:
        return resp.status, resp.read().decode("utf-8", errors="replace")

def get_json(url, timeout=25):
    status, txt = http_get(url, timeout=timeout)
    return json.loads(txt)

def ensure_yahoo_session():
    global _crumb, _crumb_time
    if _crumb and (time.time() - _crumb_time) < 21600:
        return _crumb
    for u in ["https://fc.yahoo.com", "https://finance.yahoo.com/quote/NVDA"]:
        try:
            http_get(u, timeout=12, accept="text/html,*/*")
        except Exception:
            pass
    status, text = http_get("https://query1.finance.yahoo.com/v1/test/getcrumb", timeout=15, accept="text/plain,*/*")
    crumb = text.strip()
    if status == 200 and crumb and "<html" not in crumb.lower() and len(crumb) < 200:
        _crumb = crumb
        _crumb_time = time.time()
        return _crumb
    raise RuntimeError("Kunne ikke hente Yahoo crumb/session")

def raw(v):
    if isinstance(v, dict) and "raw" in v:
        return v.get("raw")
    return v

def yahoo_symbol(symbol):
    return symbol.strip().upper().replace(".", "-")

def yahoo_quote_batch(symbols):
    symbols = [yahoo_symbol(s) for s in symbols if s.strip()]
    if not symbols:
        return {}
    cached_key = "quote_batch_" + "_".join(symbols)
    cached = read_cache(cached_key, 120)
    if cached:
        return cached
    crumb = ensure_yahoo_session()
    fields = ",".join([
        "symbol","shortName","longName","regularMarketPrice","regularMarketPreviousClose",
        "regularMarketChangePercent","trailingPE","forwardPE","marketCap","currency",
        "fiftyTwoWeekHigh","fiftyTwoWeekLow","regularMarketTime","marketCap"
    ])
    url = "https://query1.finance.yahoo.com/v7/finance/quote?" + urllib.parse.urlencode({
        "symbols": ",".join(symbols),
        "fields": fields,
        "crumb": crumb
    })
    try:
        data = get_json(url, timeout=25)
    except Exception:
        fallback = {}
        for s in symbols:
            q = read_cache("quote_" + s, 86400 * 30) or read_cache_any("quote_" + s)
            if q:
                fallback[s] = q
        if fallback:
            return fallback
        raise
    results = data.get("quoteResponse", {}).get("result", [])
    out = {(r.get("symbol") or "").upper(): r for r in results}
    for sym, q in list(out.items()):
        if sym:
            write_cache("quote_" + sym, q)
    for s in symbols:
        if s not in out:
            q = read_cache("quote_" + s, 86400 * 30) or read_cache_any("quote_" + s)
            if q:
                out[s] = q
    write_cache(cached_key, out)
    return out

def yahoo_summary(symbol):
    symbol = yahoo_symbol(symbol)
    cached = read_cache("summary_" + symbol, 900)
    if cached:
        return cached
    crumb = ensure_yahoo_session()
    modules = "price,summaryDetail,defaultKeyStatistics,financialData,incomeStatementHistory,balanceSheetHistory,cashflowStatementHistory,earningsTrend,recommendationTrend,upgradeDowngradeHistory,assetProfile,insiderHolders,insiderTransactions,netSharePurchaseActivity"
    url = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/" + urllib.parse.quote(symbol) + "?" + urllib.parse.urlencode({
        "modules": modules,
        "crumb": crumb
    })
    data = get_json(url, timeout=30)
    write_cache("summary_" + symbol, data)
    return data


def yahoo_news(symbol):
    """Fetch news from Yahoo Finance RSS and Finviz."""
    symbol = yahoo_symbol(symbol)
    cache_key = "news_" + symbol
    cached = read_cache(cache_key, 900)
    if cached:
        return cached

    news = []

    # 1) Yahoo Finance RSS
    try:
        url = "https://feeds.finance.yahoo.com/rss/2.0/headline?" + urllib.parse.urlencode({
            "s": symbol, "region": "US", "lang": "en-US"
        })
        _, txt = http_get(url, timeout=15, accept="application/rss+xml,text/xml,*/*")
        items = re.findall(r"<item>(.*?)</item>", txt, flags=re.S)
        for item in items[:8]:
            title = re.search(r"<title><!\[CDATA\[(.*?)\]\]></title>|<title>(.*?)</title>", item, re.S)
            link  = re.search(r"<link>(.*?)</link>", item, re.S)
            pubdate = re.search(r"<pubDate>(.*?)</pubDate>", item, re.S)
            src   = re.search(r"<source[^>]*>(.*?)</source>", item, re.S)
            if title:
                t = (title.group(1) or title.group(2) or "").strip()
                l = (link.group(1) or "").strip() if link else ""
                d = (pubdate.group(1) or "").strip() if pubdate else ""
                s = (src.group(1) or "Yahoo Finance").strip() if src else "Yahoo Finance"
                if t and l and t.lower() != symbol.lower():
                    news.append({"title": t, "link": l, "date": d, "source": s})
    except Exception:
        pass

    # 2) Finviz news scrape
    if len(news) < 5:
        try:
            url = "https://finviz.com/quote.ashx?t=" + urllib.parse.quote(symbol)
            _, html = http_get(url, timeout=15, accept="text/html,*/*")
            rows = re.findall(r'<tr[^>]*class="[^"]*news[^"]*"[^>]*>(.*?)</tr>|news-link-cell[^>]*>(.*?)</a>', html, flags=re.S)
            # More targeted: find news table rows
            news_rows = re.findall(r'<a[^>]+href="(https?://[^"]+)"[^>]*class="[^"]*tab-link[^"]*"[^>]*>(.*?)</a>', html, flags=re.S)
            # Try news-specific pattern
            fv_items = re.findall(
                r'<tr[^>]*>\s*<td[^>]*>([^<]{3,20})</td>\s*<td[^>]*><a[^>]+href="(https?://[^"]+)"[^>]*>(.*?)</a>.*?<span[^>]*>\[([^\]]+)\]</span>',
                html, flags=re.S
            )
            for item in fv_items[:6]:
                date_str, link, title, source = item
                title = re.sub(r"<[^>]+>", "", title).strip()
                if title and link:
                    news.append({"title": title, "link": link, "date": date_str.strip(), "source": source.strip()})
        except Exception:
            pass

    # 3) Yahoo Finance search API as fallback
    if len(news) < 3:
        try:
            crumb = ensure_yahoo_session()
            url = "https://query1.finance.yahoo.com/v1/finance/search?" + urllib.parse.urlencode({
                "q": symbol, "quotesCount": 0, "newsCount": 8, "crumb": crumb
            })
            data = get_json(url, timeout=15)
            for item in (data.get("news") or [])[:8]:
                t = item.get("title", "")
                l = item.get("link", "") or ("https://finance.yahoo.com/news/" + item.get("uuid",""))
                d = ""
                ptime = item.get("providerPublishTime")
                if ptime:
                    import datetime
                    d = datetime.datetime.utcfromtimestamp(ptime).strftime("%d %b %Y")
                s = item.get("publisher", "Yahoo Finance")
                if t and l:
                    news.append({"title": t, "link": l, "date": d, "source": s})
        except Exception:
            pass

    # Deduplicate and limit
    seen = set()
    unique = []
    for n in news:
        key = n["title"][:60]
        if key not in seen:
            seen.add(key)
            unique.append(n)

    result = unique[:7]
    if result:
        write_cache(cache_key, result)
    return result

def yahoo_earnings(symbol):
    """
    Fetch the latest earnings report highlights for a ticker.
    Sources (in priority order):
      1) Yahoo Finance quoteSummary quarterly income statements  -> structured numbers
      2) Yahoo Finance earnings history + trend                  -> EPS beats/misses
      3) Stockanalysis.com earnings page                        -> reported figures
      4) Macrotrends revenue/earnings pages                     -> headline numbers
    Returns a dict with structured highlights ready for the frontend.
    """
    symbol = yahoo_symbol(symbol)
    cache_key = "earnings_" + symbol
    cached = read_cache(cache_key, 1800)
    if cached:
        return cached

    result = {
        "ticker": symbol,
        "reportDate": "",
        "reportPeriod": "",
        "source": "",
        "highlights": [],   # list of {label, value, vs, beat, desc}
        "epsActual": None,
        "epsEstimate": None,
        "epsBeat": None,
        "revenueActual": None,
        "revenueEstimate": None,
        "revBeat": None,
        "guidance": "",
        "error": "",
    }

    # ── 1) Yahoo Finance quoteSummary quarterly modules ──────────────
    try:
        crumb = ensure_yahoo_session()
        modules = "earningsHistory,incomeStatementHistoryQuarterly,balanceSheetHistoryQuarterly,cashflowStatementHistoryQuarterly,earningsTrend"
        url = ("https://query2.finance.yahoo.com/v10/finance/quoteSummary/"
               + urllib.parse.quote(symbol) + "?"
               + urllib.parse.urlencode({"modules": modules, "crumb": crumb}))
        data = get_json(url, timeout=25)
        qsr = (data.get("quoteSummary", {}).get("result") or [None])[0]
        if qsr:
            # ── EPS history ──────────────────────────────────────────
            eps_hist = (qsr.get("earningsHistory", {}).get("history") or [])
            if eps_hist:
                latest = eps_hist[-1]  # most recent quarter
                eps_act = raw(latest.get("epsActual"))
                eps_est = raw(latest.get("epsEstimate"))
                eps_diff = raw(latest.get("epsDifference"))
                eps_surp = raw(latest.get("surprisePercent"))
                q_str = latest.get("quarter", {})
                if isinstance(q_str, dict):
                    q_str = q_str.get("fmt", "")
                result["epsActual"]   = eps_act
                result["epsEstimate"] = eps_est
                result["reportPeriod"] = str(q_str)
                if eps_act is not None and eps_est is not None:
                    beat = eps_act >= eps_est
                    result["epsBeat"] = beat
                    surp_txt = ""
                    if eps_surp is not None:
                        surp_txt = f" ({'+' if eps_surp>0 else ''}{eps_surp:.1f}% vs. estimat)"
                    result["highlights"].append({
                        "label": "EPS",
                        "value": f"${eps_act:.2f}",
                        "vs": f"est. ${eps_est:.2f}",
                        "beat": beat,
                        "desc": f"{'Slog' if beat else 'Missede'} konsensus-estimat med{surp_txt}"
                    })

            # ── Quarterly income statement ───────────────────────────
            inc_q = (qsr.get("incomeStatementHistoryQuarterly", {})
                        .get("incomeStatementHistory") or [])
            if inc_q:
                q0 = inc_q[0]
                q1 = inc_q[1] if len(inc_q) > 1 else {}
                q4 = inc_q[4] if len(inc_q) > 4 else {}  # same quarter last year

                rev   = raw(q0.get("totalRevenue"))
                rev_p = raw(q1.get("totalRevenue"))   # prev quarter
                rev_y = raw(q4.get("totalRevenue"))   # YoY same quarter
                gp    = raw(q0.get("grossProfit"))
                ni    = raw(q0.get("netIncome"))
                ni_p  = raw(q1.get("netIncome"))
                ebit  = raw(q0.get("ebit"))

                end_date = q0.get("endDate", {})
                if isinstance(end_date, dict):
                    result["reportDate"] = end_date.get("fmt", "")

                def fmtB(v):
                    if v is None: return "—"
                    v = float(v)
                    if abs(v) >= 1e9:  return f"${v/1e9:.2f}B"
                    if abs(v) >= 1e6:  return f"${v/1e6:.1f}M"
                    return f"${v:,.0f}"

                def pct_chg(new, old):
                    if new is None or old is None or old == 0: return None
                    return (float(new) - float(old)) / abs(float(old)) * 100

                result["revenueActual"] = rev

                # Revenue
                if rev:
                    rev_yoy = pct_chg(rev, rev_y)
                    rev_qoq = pct_chg(rev, rev_p)
                    desc_parts = []
                    if rev_yoy is not None:
                        desc_parts.append(f"{'▲' if rev_yoy>0 else '▼'} {abs(rev_yoy):.1f}% YoY")
                    if rev_qoq is not None:
                        desc_parts.append(f"{'▲' if rev_qoq>0 else '▼'} {abs(rev_qoq):.1f}% QoQ")
                    result["highlights"].append({
                        "label": "Omsætning",
                        "value": fmtB(rev),
                        "vs": " · ".join(desc_parts) if desc_parts else "",
                        "beat": rev_yoy is not None and rev_yoy > 0,
                        "desc": "Kvartalsvis omsætning"
                    })

                # Gross profit & margin
                if gp and rev and float(rev) != 0:
                    gm = float(gp) / float(rev) * 100
                    result["highlights"].append({
                        "label": "Bruttomargin",
                        "value": f"{gm:.1f}%",
                        "vs": fmtB(gp),
                        "beat": gm > 30,
                        "desc": "Bruttofortjeneste som andel af omsætning"
                    })

                # Net income
                if ni:
                    ni_yoy = pct_chg(ni, raw(q4.get("netIncome")))
                    ni_qoq = pct_chg(ni, ni_p)
                    desc_parts = []
                    if ni_yoy is not None:
                        desc_parts.append(f"{'▲' if ni_yoy>0 else '▼'} {abs(ni_yoy):.1f}% YoY")
                    if ni_qoq is not None:
                        desc_parts.append(f"{'▲' if ni_qoq>0 else '▼'} {abs(ni_qoq):.1f}% QoQ")
                    result["highlights"].append({
                        "label": "Nettoresultat",
                        "value": fmtB(ni),
                        "vs": " · ".join(desc_parts) if desc_parts else "",
                        "beat": float(ni) > 0,
                        "desc": "Kvartalsvis bundlinje"
                    })

            # ── Cash flow quarterly ──────────────────────────────────
            cf_q = (qsr.get("cashflowStatementHistoryQuarterly", {})
                       .get("cashflowStatements") or [])
            if cf_q:
                c0 = cf_q[0]
                ocf   = raw(c0.get("totalCashFromOperatingActivities"))
                capex = raw(c0.get("capitalExpenditures"))
                fcf   = (float(ocf) + float(capex)) if ocf and capex else None

                def fmtB(v):
                    if v is None: return "—"
                    v = float(v)
                    if abs(v) >= 1e9:  return f"${v/1e9:.2f}B"
                    if abs(v) >= 1e6:  return f"${v/1e6:.1f}M"
                    return f"${v:,.0f}"

                if ocf:
                    result["highlights"].append({
                        "label": "Op. Cash Flow",
                        "value": fmtB(ocf),
                        "vs": f"CapEx {fmtB(capex)}" if capex else "",
                        "beat": float(ocf) > 0,
                        "desc": "Operationel pengestrøm — viser reel cash-generering"
                    })
                if fcf is not None:
                    result["highlights"].append({
                        "label": "Fri Cash Flow",
                        "value": fmtB(fcf),
                        "vs": "",
                        "beat": fcf > 0,
                        "desc": f"FCF = Op. CF + CapEx. {'Positiv — virksomheden genererer frie midler' if fcf>0 else 'Negativ — investerer mere end cash-generering'}"
                    })

            # ── Balance sheet quarterly ──────────────────────────────
            bal_q = (qsr.get("balanceSheetHistoryQuarterly", {})
                        .get("balanceSheetStatements") or [])
            if bal_q:
                b0 = bal_q[0]
                b1 = bal_q[1] if len(bal_q) > 1 else {}
                cash   = raw(b0.get("cash"))
                stinv  = raw(b0.get("shortTermInvestments"))
                cash_p = raw(b1.get("cash"))
                debt   = raw(b0.get("longTermDebt"))
                equity = raw(b0.get("totalStockholderEquity"))
                cash_total = (float(cash or 0) + float(stinv or 0)) if cash is not None else None

                def fmtB(v):
                    if v is None: return "—"
                    v = float(v)
                    if abs(v) >= 1e9:  return f"${v/1e9:.2f}B"
                    if abs(v) >= 1e6:  return f"${v/1e6:.1f}M"
                    return f"${v:,.0f}"

                if cash_total is not None:
                    cash_chg = None
                    if cash_p:
                        cash_chg = (float(cash_total) - float(cash_p)) / abs(float(cash_p)) * 100
                    result["highlights"].append({
                        "label": "Cash & Likvider",
                        "value": fmtB(cash_total),
                        "vs": (f"{'▲' if cash_chg>0 else '▼'} {abs(cash_chg):.1f}% QoQ") if cash_chg else "",
                        "beat": cash_total > 0,
                        "desc": "Total cash og kortsigtede investeringer"
                    })

                if debt and equity and float(equity) != 0:
                    de = float(debt) / float(equity) * 100

                    def fmtB2(v):
                        if v is None: return "—"
                        v = float(v)
                        if abs(v) >= 1e9:  return f"${v/1e9:.2f}B"
                        if abs(v) >= 1e6:  return f"${v/1e6:.1f}M"
                        return f"${v:,.0f}"

                    result["highlights"].append({
                        "label": "Gæld/Egenkapital",
                        "value": f"{de:.0f}%",
                        "vs": f"Gæld {fmtB2(debt)}",
                        "beat": de < 100,
                        "desc": f"{'Lav' if de<50 else 'Moderat' if de<150 else 'Høj'} gældsætning — {'solid' if de<100 else 'forhøjet'} balancestruktur"
                    })

            # ── Earnings trend / guidance ────────────────────────────
            trend = qsr.get("earningsTrend", {}).get("trend") or []
            for t in trend:
                if t.get("period") == "0q":
                    eps_est_nq = raw((t.get("earningsEstimate") or {}).get("avg"))
                    rev_est_nq = raw((t.get("revenueEstimate") or {}).get("avg"))
                    rev_low    = raw((t.get("revenueEstimate") or {}).get("low"))
                    rev_high   = raw((t.get("revenueEstimate") or {}).get("high"))
                    rev_growth_est = raw((t.get("revenueEstimate") or {}).get("growth"))

                    def fmtB_inner(v):
                        if v is None: return "—"
                        v = float(v)
                        if abs(v) >= 1e9:  return f"${v/1e9:.2f}B"
                        if abs(v) >= 1e6:  return f"${v/1e6:.1f}M"
                        return f"${v:,.0f}"

                    if eps_est_nq is not None:
                        result["highlights"].append({
                            "label": "EPS Estimat (næste kvartal)",
                            "value": f"${eps_est_nq:.2f}",
                            "vs": "",
                            "beat": None,
                            "desc": "Analytiker-konsensus for kommende kvartals EPS"
                        })
                    if rev_est_nq is not None:
                        rg_txt = f" · vækst {rev_growth_est*100:.1f}%" if rev_growth_est else ""
                        result["highlights"].append({
                            "label": "Omsætningsestimat (næste kvartal)",
                            "value": fmtB_inner(rev_est_nq),
                            "vs": f"Range {fmtB_inner(rev_low)}–{fmtB_inner(rev_high)}" if rev_low and rev_high else "",
                            "beat": None,
                            "desc": f"Analytiker-konsensus for kommende kvartals omsætning{rg_txt}"
                        })
                    break

            result["source"] = "Yahoo Finance Quarterly Statements"

    except Exception as e:
        result["error"] = str(e)

    # ── 2) Stockanalysis.com as supplementary source ─────────────────
    if len(result["highlights"]) < 4:
        try:
            url = f"https://stockanalysis.com/stocks/{symbol.lower()}/financials/?p=quarterly"
            _, html = http_get(url, timeout=20, accept="text/html,*/*")
            # Find latest quarter header
            qheaders = re.findall(r'<th[^>]*>([A-Z][a-z]+ \d{4}|Q\d \'\d{2}|Q\d \d{4})</th>', html)
            if qheaders and not result["reportPeriod"]:
                result["reportPeriod"] = qheaders[0]
            # Revenue row
            rev_match = re.search(r'Revenue.*?<td[^>]*>([\$\d\.B M]+)</td>', html, re.S)
            if rev_match and not result["revenueActual"]:
                result["highlights"].append({
                    "label": "Omsætning (StockAnalysis)",
                    "value": rev_match.group(1).strip(),
                    "vs": "",
                    "beat": None,
                    "desc": "Seneste kvartals omsætning fra StockAnalysis"
                })
            if not result["source"]:
                result["source"] = "StockAnalysis.com"
        except Exception:
            pass

    # Limit highlights to 10
    result["highlights"] = result["highlights"][:10]

    if result["highlights"]:
        write_cache(cache_key, result)
    return result


def yahoo_chart(symbol):
    symbol = yahoo_symbol(symbol)
    cached = read_cache("chart_" + symbol, 900)
    if cached:
        return cached
    url = "https://query1.finance.yahoo.com/v8/finance/chart/" + urllib.parse.quote(symbol) + "?" + urllib.parse.urlencode({
        "range": "1y",
        "interval": "1d"
    })
    data = get_json(url, timeout=25)
    write_cache("chart_" + symbol, data)
    return data

def calc_returns_from_chart(chart_data):
    result = (chart_data.get("chart", {}).get("result") or [None])[0]
    if not result:
        return None, None, None, None
    quote = (result.get("indicators", {}).get("quote") or [{}])[0]
    closes = [c for c in (quote.get("close") or []) if c is not None]
    meta = result.get("meta", {})
    price = meta.get("regularMarketPrice") or (closes[-1] if closes else None)
    if not closes:
        return price, None, None, None
    c0 = price or closes[-1]
    def ret(days):
        if len(closes) > days and closes[-days]:
            try:
                return float(c0) / float(closes[-days]) - 1
            except Exception:
                return None
        return None
    return price, ret(63), ret(126), ret(252)


def calc_price_structure(chart_data):
    """Price/risk metrics from 1y Yahoo chart. Returns volatility, max drawdown, 200MA distance and trend stability."""
    result = (chart_data.get("chart", {}).get("result") or [None])[0]
    if not result:
        return {}
    quote = (result.get("indicators", {}).get("quote") or [{}])[0]
    closes = [float(c) for c in (quote.get("close") or []) if c is not None and c > 0]
    if len(closes) < 30:
        return {}
    price = closes[-1]
    rets = [(closes[i] / closes[i-1] - 1.0) for i in range(1, len(closes)) if closes[i-1] > 0]
    def stdev(vals):
        if not vals: return None
        m = sum(vals)/len(vals)
        return (sum((x-m)**2 for x in vals)/len(vals))**0.5
    vol = stdev(rets)
    annual_vol = vol * (252**0.5) if vol is not None else None
    peak = closes[0]; max_dd = 0.0
    above_count = 0
    for i,c in enumerate(closes):
        peak = max(peak, c)
        max_dd = min(max_dd, c/peak - 1.0)
        if i >= 199:
            ma = sum(closes[i-199:i+1]) / 200
            if c >= ma: above_count += 1
    ma200 = sum(closes[-200:]) / min(200, len(closes))
    ath = max(closes)
    return {
        "volatilityAnnual": annual_vol,
        "maxDrawdown1Y": max_dd,
        "ma200": ma200,
        "distanceFrom200MA": (price / ma200 - 1.0) if ma200 else None,
        "athProximity": (price / ath) if ath else None,
        "trendStability": above_count / max(1, len(closes)-199) if len(closes) >= 200 else None,
    }

def score_components(pe, peg, roe, rev_growth, m3, m6, m12):
    """Backwards-compatible VQM score used by older UI fields."""
    def to_float(x, default=None):
        try:
            if x is None: return default
            return float(x)
        except Exception:
            return default
    pe = to_float(pe); peg = to_float(peg); roe = to_float(roe, 0); rev_growth = to_float(rev_growth, 0)
    m3 = to_float(m3, 0); m6 = to_float(m6, 0); m12 = to_float(m12, 0)
    def clamp(x): return max(0, min(100, x))
    value = 0
    if pe and pe > 0: value += (1 / max(pe, 1)) * 1200
    if peg and peg > 0: value += (1 / max(peg, 0.1)) * 25
    value = clamp(value)
    quality = clamp((roe * 100) * 0.75 + (rev_growth * 100) * 0.25)
    momentum = clamp(35 + m3*20 + m6*25 + m12*35)
    total = clamp(value*0.20 + quality*0.25 + momentum*0.25)
    return value, quality, momentum, total


def sector_benchmark_symbol(sector, industry=""):
    """Lightweight Yahoo ETF proxy for sector-relative strength without changing API shape."""
    text = ((sector or "") + " " + (industry or "")).lower()
    if "semiconductor" in text: return "SMH"
    if "technology" in text or "software" in text or "internet" in text: return "XLK"
    if "communication" in text or "media" in text: return "XLC"
    if "consumer cyclical" in text or "auto" in text or "retail" in text: return "XLY"
    if "consumer defensive" in text or "staples" in text: return "XLP"
    if "health" in text or "biotech" in text or "pharma" in text: return "XLV"
    if "financial" in text or "bank" in text: return "XLF"
    if "industrial" in text or "aerospace" in text or "defense" in text: return "XLI"
    if "energy" in text or "oil" in text or "gas" in text: return "XLE"
    if "basic materials" in text or "materials" in text or "mining" in text: return "XLB"
    if "real estate" in text or "reit" in text: return "XLRE"
    if "utilities" in text: return "XLU"
    return "SPY"


def _benchmark_returns(symbol):
    """Cached 3/6/12m returns + price structure for benchmark ETFs."""
    key = "bench_ext_" + yahoo_symbol(symbol)
    cached = read_cache(key, 1800)
    if cached:
        return cached
    try:
        ch = yahoo_chart(symbol)
        _, b3, b6, b12 = calc_returns_from_chart(ch)
        pm = calc_price_structure(ch)
        out = {"m3": b3, "m6": b6, "m12": b12, **(pm or {})}
        write_cache(key, out)
        return out
    except Exception:
        return {}


def calc_market_regime():
    """Small regime engine using free ETF proxies only. No new top-level data structure."""
    cached = read_cache("market_regime_v2", 1800)
    if cached:
        return cached
    spy = _benchmark_returns("SPY")
    qqq = _benchmark_returns("QQQ")
    xlu = _benchmark_returns("XLU")
    xlp = _benchmark_returns("XLP")
    xlk = _benchmark_returns("XLK")
    def sf(v,d=0):
        try: return float(v) if v is not None else d
        except Exception: return d
    spy6, spy12 = sf(spy.get("m6")), sf(spy.get("m12"))
    qqq6, xlk6 = sf(qqq.get("m6")), sf(xlk.get("m6"))
    defensive6 = (sf(xlu.get("m6")) + sf(xlp.get("m6"))) / 2
    vol = sf(spy.get("volatilityAnnual"), 0.22)
    dd = abs(sf(spy.get("maxDrawdown1Y"), 0))
    if dd > 0.25 or vol > 0.34:
        regime = "Panic / High Volatility"
        weights = {"value":0.15,"quality":0.30,"momentum":0.15,"revisions":0.10,"risk":0.25,"sentimentCatalyst":0.05}
    elif spy6 < -0.08 and defensive6 > spy6:
        regime = "Recession / Defensive"
        weights = {"value":0.15,"quality":0.35,"momentum":0.15,"revisions":0.10,"risk":0.20,"sentimentCatalyst":0.05}
    elif (qqq6 > spy6 + 0.05) and (xlk6 > spy6 + 0.04):
        regime = "AI / Growth Mania"
        weights = {"value":0.10,"quality":0.25,"momentum":0.35,"revisions":0.15,"risk":0.10,"sentimentCatalyst":0.05}
    elif spy12 > 0.10 and spy6 > 0.02:
        regime = "Bull Market"
        weights = {"value":0.15,"quality":0.25,"momentum":0.30,"revisions":0.15,"risk":0.10,"sentimentCatalyst":0.05}
    else:
        regime = "Neutral / Mixed"
        weights = {"value":0.20,"quality":0.25,"momentum":0.25,"revisions":0.15,"risk":0.10,"sentimentCatalyst":0.05}
    out = {"regime": regime, "weights": weights, "spy6m": spy6, "spy12m": spy12, "qqq6m": qqq6, "defensive6m": defensive6, "marketVol": vol}
    write_cache("market_regime_v2", out)
    return out



def calc_multi_year_engine(fin_history):
    """5-10 year style trend engine from available Yahoo annual statements.
    Returns compact metrics that are stored inside financials only, so the API shape remains compatible.
    """
    def safe(v, default=None):
        try: return float(v) if v is not None else default
        except Exception: return default
    def clamp(x): return max(0.0, min(100.0, float(x)))
    rows = fin_history or []
    if not rows:
        return {"multiYearScore": 50, "historyYears": 0, "trendQuality": "MISSING"}
    # newest first in Yahoo; reverse for old->new slope logic
    ordered = list(reversed(rows))
    def series(k): return [safe(r.get(k)) for r in ordered if safe(r.get(k)) is not None]
    def growth(vals):
        if len(vals) < 2 or not vals[0]: return None
        return (vals[-1] / abs(vals[0]) - 1.0)
    def slope_score(vals, scale=120):
        if len(vals) < 2: return None
        return clamp(50 + (vals[-1] - vals[0]) * scale)
    rev = series('revenue')
    gm = series('grossMargin')
    om = series('operatingMargin')
    fcfm = series('fcfMargin')
    debt = series('netDebt')
    shares = series('shares')
    roic = series('roic')
    rev_growth_total = growth(rev)
    revenue_trend_score = clamp(50 + (rev_growth_total or 0) * 35) if rev_growth_total is not None else None
    margin_trend_score = None
    parts=[]
    for vals,w,sc in [(gm,.25,120),(om,.30,180),(fcfm,.25,160),(roic,.20,160)]:
        ss=slope_score(vals, sc)
        if ss is not None: parts.append((ss,w))
    if parts:
        margin_trend_score = sum(v*w for v,w in parts)/sum(w for v,w in parts)
    debt_trend_score = None
    if len(debt) >= 2:
        debt_trend_score = clamp(55 - (debt[-1]-debt[0]) / max(1, abs(debt[0]) or 1) * 35)
    dilution_trend_score = None
    if len(shares) >= 2 and shares[0]:
        dilution = shares[-1]/shares[0]-1.0
        dilution_trend_score = clamp(70 - dilution*220)
    available=[x for x in [revenue_trend_score,margin_trend_score,debt_trend_score,dilution_trend_score] if x is not None]
    multi = sum(available)/len(available) if available else 50
    return {
        "multiYearScore": round(clamp(multi)),
        "historyYears": len(rows),
        "revenueTrendScore": round(revenue_trend_score) if revenue_trend_score is not None else None,
        "marginTrendScore": round(margin_trend_score) if margin_trend_score is not None else None,
        "debtTrendScore": round(debt_trend_score) if debt_trend_score is not None else None,
        "dilutionTrendScore": round(dilution_trend_score) if dilution_trend_score is not None else None,
        "roicTrend": roic,
        "fcfMarginTrend": fcfm,
        "grossMarginTrend": gm,
        "operatingMarginTrend": om,
        "trendQuality": "LIVE DATA" if len(rows) >= 3 else "PROXY" if len(rows) else "MISSING"
    }


def calc_backtest_proxy(score, revisions, risk, momentum, m12):
    """Lightweight local what-if/backtest proxy. Real historical point-in-time backtesting needs a stored database."""
    def safe(v, default=0):
        try: return float(v) if v is not None else default
        except Exception: return default
    def clamp(x): return max(0, min(100, float(x)))
    score=safe(score); revisions=safe(revisions); risk=safe(risk,50); momentum=safe(momentum,50); m12=safe(m12,0)
    setup = clamp(score*0.45 + revisions*0.20 + risk*0.20 + momentum*0.15)
    expected_6m = (setup-50)/100 * 0.18 + min(max(m12, -0.5), 1.5)*0.08
    win_prob = clamp(48 + (setup-50)*0.55 + min(max(m12, -0.4), 0.8)*12)
    drawdown_risk = clamp(60 - risk*0.45 + max(0, momentum-75)*0.15)
    return {"setupScore": round(setup), "expected6mProxy": expected_6m, "winProbabilityProxy": round(win_prob), "drawdownRiskProxy": round(drawdown_risk), "status": "PROXY"}

def calc_professional_scores(f, pe, peg, roe, rev_growth, m3, m6, m12, price_metrics=None):
    """Institutional-style score model without changing the API shape.

    Added engines layered into proScores/financials only:
      • Earnings Quality + accruals
      • Piotroski F + Altman Z integration
      • Relative sector strength
      • Regime adaptive weights
      • Smart money / ownership proxies
      • Dilution / SBC proxy
      • Moat, catalyst and narrative proxies
      • Better downside/tail/gap/liquidity/correlation risk
      • Composite factor percentile-like ranking
    """
    def safe(v, default=None):
        try: return float(v) if v is not None else default
        except Exception: return default
    def clamp(x): return max(0.0, min(100.0, float(x)))
    def norm_pct(v, center=0.0, scale=1.0, base=50.0):
        if v is None: return None
        return clamp(base + ((float(v) - center) * scale))
    def weighted(parts, fallback=50):
        w=sum(w for v,w in parts if v is not None)
        return clamp(sum(v*w for v,w in parts if v is not None)/w) if w else fallback

    price_metrics = price_metrics or {}

    # Multi-year financial trend engine fallback.
    # Fix: previous build referenced multi_year_score before it was defined
    # when blending it into the final institutional score.
    multi_year = f.get('multiYearEngine') or {}
    multi_year_score = safe(multi_year.get('multiYearScore'), 50)

    pe=safe(pe); peg=safe(peg); roe=safe(roe,0); rev_growth=safe(rev_growth,0)
    m3=safe(m3,0); m6=safe(m6,0); m12=safe(m12,0)

    revenue=safe(f.get('totalRevenue'))
    gross_profit=safe(f.get('grossProfit'))
    net_income=safe(f.get('netIncome'))
    op_cf=safe(f.get('operatingCashflow'))
    fcf=safe(f.get('freeCashflow'))
    capex=safe(f.get('capex'))
    ebit=safe(f.get('ebit'))
    ebitda=safe(f.get('ebitda'))
    debt=safe(f.get('totalDebt'),0)
    cash=safe(f.get('cashAndEquivalents'),0)
    equity=safe(f.get('stockholderEquity'))
    total_assets=safe(f.get('totalAssets'))
    total_liab=safe(f.get('totalLiabilities'))
    shares=safe(f.get('sharesOutstanding'))
    beta=safe(f.get('beta'))
    vol=safe(price_metrics.get('volatilityAnnual'))
    maxdd=safe(price_metrics.get('maxDrawdown1Y'))
    dist200=safe(price_metrics.get('distanceFrom200MA'))
    athp=safe(price_metrics.get('athProximity'))
    stability=safe(price_metrics.get('trendStability'))

    gross_margin=safe(f.get('grossMargin'))
    operating_margin=safe(f.get('operatingMargin')) or safe(f.get('ebitMargin'))
    net_margin=safe(f.get('netMargin'))
    fcf_margin=safe(f.get('fcfMargin'))
    if fcf_margin is None and fcf is not None and revenue and revenue>0:
        fcf_margin = fcf / revenue
    invested_capital=(debt or 0)+(equity or 0)-(cash or 0) if equity is not None else None
    roic=(ebit/invested_capital) if ebit is not None and invested_capital and invested_capital>0 else None

    # 1) VALUE
    value=0
    if pe and pe>0: value += (1/max(pe,1))*1200
    if peg and peg>0: value += (1/max(peg,0.1))*25
    ps=safe(f.get('priceToSales'))
    pb=safe(f.get('priceToBook'))
    ev_ebitda=safe(f.get('evEbitda'))
    if ps is not None: value += max(0, 20-ps*2)
    if pb is not None: value += max(0, 12-pb*1.5)
    if ev_ebitda is not None and ev_ebitda>0: value += max(0, 16-ev_ebitda*0.75)
    value=clamp(value)

    # 2) EARNINGS QUALITY
    accrual_ratio = ((net_income or 0) - (op_cf or 0)) / total_assets if total_assets and net_income is not None and op_cf is not None else None
    cash_net_income = (op_cf / net_income) if op_cf is not None and net_income and net_income != 0 else None
    fcf_conversion = (fcf / net_income) if fcf is not None and net_income and net_income != 0 else None
    margin_sustainability = weighted([
        (norm_pct(gross_margin, center=0.25, scale=120, base=45) if gross_margin is not None else None, .35),
        (norm_pct(operating_margin, center=0.08, scale=190, base=45) if operating_margin is not None else None, .35),
        (norm_pct(net_margin, center=0.05, scale=180, base=45) if net_margin is not None else None, .30),
    ])
    accrual_score = clamp(70 - (accrual_ratio or 0)*280) if accrual_ratio is not None else None
    cash_income_score = clamp(45 + (cash_net_income or 0)*25) if cash_net_income is not None else None
    fcf_conv_score = clamp(45 + (fcf_conversion or 0)*25) if fcf_conversion is not None else None
    one_time_proxy = clamp(100 - abs((net_margin or 0) - (fcf_margin or 0))*180) if net_margin is not None and fcf_margin is not None else 55
    earnings_quality = weighted([
        (cash_income_score,.25), (accrual_score,.30), (fcf_conv_score,.25),
        (one_time_proxy,.10), (margin_sustainability,.10)
    ])

    # 3) QUALITY with ROIC + FCF + earnings quality
    roic_score = norm_pct(roic, center=0.08, scale=260, base=45) if roic is not None else None
    roe_score = norm_pct(roe, center=0.10, scale=210, base=45) if roe is not None else 50
    fcf_score = norm_pct(fcf_margin, center=0.05, scale=240, base=45) if fcf_margin is not None else None
    gross_score = norm_pct(gross_margin, center=0.25, scale=120, base=45) if gross_margin is not None else None
    op_score = norm_pct(operating_margin, center=0.08, scale=190, base=45) if operating_margin is not None else None
    piotroski=safe(f.get('piotroskiF'))
    altman=safe(f.get('altmanZ'))
    piotroski_score = clamp((piotroski or 0)/9*100) if piotroski is not None else None
    altman_score = clamp(35 + (altman or 0)*15) if altman is not None else None
    quality=weighted([
        (roic_score,0.28),(roe_score,0.14),(fcf_score,0.16),(gross_score,0.09),(op_score,0.09),
        (earnings_quality,0.14),(piotroski_score,0.06),(altman_score,0.04)
    ])
    if rev_growth is not None:
        quality=clamp(quality*0.92 + clamp(45 + rev_growth*180)*0.08)

    # 4) RELATIVE SECTOR STRENGTH + RISK ADJUSTED MOMENTUM
    sector_sym = sector_benchmark_symbol(f.get('sector'), f.get('industry'))
    sector_b = _benchmark_returns(sector_sym)
    index_b = _benchmark_returns('SPY')
    b3=safe(sector_b.get('m3'),0); b6=safe(sector_b.get('m6'),0); b12=safe(sector_b.get('m12'),0)
    i3=safe(index_b.get('m3'),0); i6=safe(index_b.get('m6'),0); i12=safe(index_b.get('m12'),0)
    stock_blend=(m3*0.25 + m6*0.30 + m12*0.45)
    sector_blend=(b3*0.25 + b6*0.30 + b12*0.45)
    index_blend=(i3*0.25 + i6*0.30 + i12*0.45)
    relative_sector_score=clamp(50 + (stock_blend-sector_blend)*180)
    relative_index_score=clamp(50 + (stock_blend-index_blend)*180)
    sector_momentum_score=clamp(50 + sector_blend*180)
    sector_etf_rs=clamp(50 + (sector_blend-index_blend)*220)
    industry_breadth_proxy=clamp((sector_b.get('trendStability') or 0.5)*100)
    sector_strength=weighted([(sector_momentum_score,.35),(industry_breadth_proxy,.25),(sector_etf_rs,.30),(50,.10)])

    return_score = clamp(50 + stock_blend*180)
    vol_score = clamp(100 - max(0, (vol or 0.25)-0.18)*160) if vol is not None else 55
    sharpe_like = clamp(return_score*0.70 + vol_score*0.30)
    ma200_score = clamp(50 + (dist200 or 0)*150) if dist200 is not None else 50
    ath_score = clamp((athp or 0.75)*100) if athp is not None else 50
    stability_score = clamp((stability or 0.50)*100) if stability is not None else 50
    momentum = weighted([
        (relative_sector_score,0.22),(relative_index_score,0.22),(sharpe_like,0.20),
        (stability_score,0.13),(ma200_score,0.10),(ath_score,0.05),(sector_strength,0.08)
    ])

    # 5) EARNINGS REVISION SCORE
    up30=safe(f.get('epsRevUpLast30days'),0); down30=safe(f.get('epsRevDownLast30days'),0)
    up7=safe(f.get('epsRevUpLast7days'),0); down7=safe(f.get('epsRevDownLast7days'),0)
    eps_growth=safe(f.get('epsEstimateGrowth'))
    rev_est_growth=safe(f.get('revenueEstimateGrowth'))
    revisions_30 = clamp(50 + (up30-down30)*12)
    revisions_7 = clamp(50 + (up7-down7)*15)
    eps_growth_score = norm_pct(eps_growth, center=0.00, scale=160, base=50) if eps_growth is not None else 50
    rev_growth_score = norm_pct(rev_est_growth, center=0.00, scale=130, base=50) if rev_est_growth is not None else 50
    updown=f.get('lastUpDowngrade') or {}
    action=(updown.get('action') or '').lower(); to_grade=(updown.get('toGrade') or '').lower()
    rec=(f.get('recommendationKey') or '').lower()
    analyst_score=50
    if 'up' in action or ('main' in action and 'buy' in to_grade): analyst_score += 25
    if 'down' in action: analyst_score -= 25
    if rec in ('strong_buy','strongbuy'): analyst_score += 25
    elif rec=='buy': analyst_score += 15
    elif rec in ('sell','underperform'): analyst_score -= 20
    revisions=clamp(revisions_30*0.35 + revisions_7*0.15 + eps_growth_score*0.20 + rev_growth_score*0.15 + clamp(analyst_score)*0.15)

    # 6) SMART MONEY + DILUTION
    inst=safe(f.get('heldByInstitutions')); insider=safe(f.get('heldByInsiders'))
    insider_net=safe(f.get('insiderNetPercent'))
    short_pct=safe(f.get('shortPctFloat'))
    buyback_proxy=safe(f.get('insiderNetShares'))
    institutional_score = clamp(45 + (inst or 0)*45) if inst is not None else 50
    insider_score = 50
    if insider is not None: insider_score += min(15, insider*60)
    if insider_net is not None: insider_score += max(-25, min(25, insider_net*220))
    if buyback_proxy is not None and shares: insider_score += max(-15, min(15, (buyback_proxy/shares)*5000))
    smart_money=weighted([(institutional_score,.35),(clamp(insider_score),.35),(50,.15),(50,.15)])

    # SBC is not reliably available in Yahoo free modules, so use a conservative proxy.
    sbc_ratio = safe(f.get('sbcRatio'))
    if sbc_ratio is None:
        sbc_ratio = None
    dilution_proxy = 50
    if insider_net is not None: dilution_proxy += max(-25, min(20, insider_net*180))
    if short_pct is not None: dilution_proxy -= max(0, min(15, short_pct*100))
    if sbc_ratio is not None: dilution_proxy -= min(35, sbc_ratio*250)
    dilution_score=clamp(dilution_proxy)

    # 7) MOAT / CATALYST / NARRATIVE PROXIES
    market_cap=safe(f.get('marketCap')) or safe(f.get('enterpriseValue'))
    pricing_power = weighted([(gross_score,.45),(op_score,.35),(roic_score,.20)])
    brand_proxy = 50 + (10 if market_cap and market_cap>2e11 else 0) + (8 if gross_margin and gross_margin>0.45 else 0)
    switching_costs = 50 + (10 if operating_margin and operating_margin>0.20 else 0) + (10 if roic and roic>0.18 else 0)
    network_effects = 50 + (10 if f.get('sector') and 'Technology' in str(f.get('sector')) else 0)
    moat_score=clamp(weighted([(pricing_power,.35),(brand_proxy,.20),(switching_costs,.20),(network_effects,.15),(relative_sector_score,.10)]))

    days_to_earnings=safe(f.get('daysToEarnings'))
    earnings_catalyst=65 if days_to_earnings is not None and 0 <= days_to_earnings <= 21 else 50
    analyst_target=safe(f.get('analystTargetMean')); p=safe(f.get('price'))
    target_score = 50
    if analyst_target and p: target_score = clamp(50 + ((analyst_target-p)/p)*80)
    catalyst_score=weighted([(earnings_catalyst,.25),(revisions,.35),(target_score,.20),(analyst_score,.20)])

    text=((f.get('sector') or '')+' '+(f.get('industry') or '')+' '+(f.get('businessSummary') or '')).lower()
    narrative_keywords=['ai','artificial intelligence','semiconductor','nuclear','robot','space','defense','cloud','data center','quantum','bitcoin','crypto']
    narrative_hits=sum(1 for k in narrative_keywords if k in text)
    options_proxy = clamp(50 + (short_pct or 0)*80) if short_pct is not None else 50
    narrative_score=clamp(50 + narrative_hits*6 + (momentum-50)*0.20 + (options_proxy-50)*0.15)

    # 8) BETTER RISK ENGINE
    de=safe(f.get('debtToEquity')); cr=safe(f.get('currentRatio'))
    net_debt_ebitda=safe(f.get('netDebtEbitda'))
    downside_vol_score = clamp(100 - max(0,(vol or 0.25)-0.16)*190) if vol is not None else 55
    tail_risk_score = clamp(100 - max(0,abs(maxdd or 0)-0.18)*140) if maxdd is not None else 60
    gap_risk_score = clamp((stability or 0.5)*100)
    liquidity_score = 65 + (10 if market_cap and market_cap>1e10 else -10 if market_cap and market_cap<1e9 else 0)
    correlation_risk_score = clamp(65 - abs((stock_blend-index_blend))*70)
    debt_score=70
    if de is not None and de>100: debt_score -= min(35,(de-100)/4)
    if net_debt_ebitda is not None and net_debt_ebitda>3: debt_score -= min(30,(net_debt_ebitda-3)*8)
    if cr is not None and cr<1: debt_score -= 20
    if altman is not None and altman<1.8: debt_score -= 15
    risk=weighted([(downside_vol_score,.20),(tail_risk_score,.20),(gap_risk_score,.15),(liquidity_score,.15),(correlation_risk_score,.10),(debt_score,.20)])
    if beta is not None: risk=clamp(risk - max(0,beta-1)*10)
    if piotroski is not None: risk=clamp(risk + (piotroski-5)*2.0)

    # 9) SENTIMENT / CATALYST composite for existing 5% bucket
    sent=weighted([(smart_money,.25),(catalyst_score,.30),(narrative_score,.20),(dilution_score,.15),(moat_score,.10)])

    # 10) COMPOSITE FACTOR RANKING (percentile-like without new backend table)
    def pct_like(score): return clamp(score)
    composite_factor_rank=weighted([
        (pct_like(value),.12),(pct_like(quality),.20),(pct_like(momentum),.20),(pct_like(revisions),.14),
        (pct_like(risk),.12),(pct_like(earnings_quality),.08),(pct_like(smart_money),.05),(pct_like(moat_score),.05),(pct_like(dilution_score),.04)
    ])


    # 11) DATA COVERAGE / CONFIDENCE LAYER
    # Does not change existing API structure: status is added inside financials/proScores only.
    def _has(*keys):
        return all(f.get(k) is not None for k in keys)
    def _status(live_keys=(), proxy=False):
        if live_keys and _has(*live_keys):
            return 'LIVE DATA'
        return 'PROXY' if proxy else 'MISSING'
    data_status = {
        'Value': _status(('priceToSales','priceToBook'), proxy=(pe is not None or peg is not None)),
        'Quality': _status(('totalRevenue','freeCashflow','stockholderEquity'), proxy=(roe is not None or roic is not None or fcf_margin is not None)),
        'Momentum': _status((), proxy=(m3 is not None or m6 is not None or m12 is not None)),
        'Revisions': _status(('epsRevUpLast30days','epsRevDownLast30days'), proxy=(eps_growth is not None or rev_est_growth is not None or f.get('recommendationKey') is not None)),
        'Earnings Quality': _status(('netIncome','operatingCashflow','totalAssets'), proxy=(net_margin is not None and fcf_margin is not None)),
        'Piotroski F': _status(('piotroskiF',), proxy=False),
        'Altman Z': _status(('altmanZ',), proxy=False),
        'Sector Strength': _status((), proxy=bool(sector_sym)),
        'Regime Detection': 'PROXY',
        'Smart Money': _status(('heldByInstitutions','heldByInsiders'), proxy=(f.get('insiderNetPercent') is not None or f.get('shortPctFloat') is not None)),
        'Dilution': _status(('sbcRatio',), proxy=(f.get('insiderNetPercent') is not None or f.get('shortPctFloat') is not None)),
        'Moat': 'PROXY',
        'Catalyst': _status(('daysToEarnings',), proxy=(f.get('analystTargetMean') is not None or f.get('recommendationKey') is not None)),
        'Narrative': 'PROXY',
        'Advanced Risk': _status((), proxy=(vol is not None or maxdd is not None or beta is not None or de is not None)),
        'Composite Rank': 'PROXY'
    }
    data_confidence = round(sum({'LIVE DATA':1.0,'PROXY':0.55,'MISSING':0.0}.get(v,0) for v in data_status.values()) / max(1,len(data_status)) * 100)

    regime_info = calc_market_regime()
    weights=regime_info.get('weights') or {'value':0.20,'quality':0.25,'momentum':0.25,'revisions':0.15,'risk':0.10,'sentimentCatalyst':0.05}
    total=clamp(value*weights['value']+quality*weights['quality']+momentum*weights['momentum']+revisions*weights['revisions']+risk*weights['risk']+sent*weights['sentimentCatalyst'])
    # Blend a small amount of composite percentile rank for stability.
    total=clamp(total*0.88 + composite_factor_rank*0.08 + (multi_year_score or 50)*0.04)
    backtest_proxy = calc_backtest_proxy(total, revisions, risk, momentum, m12)

    return {
        'value':round(value), 'quality':round(quality), 'momentum':round(momentum),
        'revisions':round(revisions), 'risk':round(risk), 'sentimentCatalyst':round(sent), 'total':round(total),
        'weights':weights,
        'derived': {
            'roic': roic, 'fcfMargin': fcf_margin, 'roicScore': roic_score, 'fcfScore': fcf_score,
            'earningsQualityScore': round(earnings_quality), 'accrualRatio': accrual_ratio,
            'cashFlowVsNetIncome': cash_net_income, 'fcfConversion': fcf_conversion,
            'oneTimeAdjustmentRiskProxy': one_time_proxy, 'marginSustainabilityScore': round(margin_sustainability),
            'piotroskiF': piotroski, 'piotroskiScore': piotroski_score, 'altmanZ': altman, 'altmanScore': altman_score,
            'sectorBenchmark': sector_sym, 'sectorStrengthScore': round(sector_strength),
            'relativeSectorStrengthScore': round(relative_sector_score), 'relativeIndexStrengthScore': round(relative_index_score),
            'regime': regime_info.get('regime'), 'regimeWeights': weights,
            'smartMoneyScore': round(smart_money), 'institutionalAccumulationScore': round(institutional_score),
            'insiderClusterScore': round(clamp(insider_score)), 'hedgeFundOwnershipProxy': round(institutional_score),
            'darkPoolProxy': 50,
            'dilutionScore': round(dilution_score), 'sbcRatio': sbc_ratio, 'buybackEfficiencyProxy': buyback_proxy,
            'moatScore': round(moat_score), 'pricingPowerScore': round(pricing_power),
            'catalystScore': round(catalyst_score), 'narrativeHypeScore': round(narrative_score),
            'downsideVolatilityScore': round(downside_vol_score), 'tailRiskScore': round(tail_risk_score),
            'gapRiskScore': round(gap_risk_score), 'liquidityRiskScore': round(liquidity_score),
            'correlationRiskScore': round(correlation_risk_score), 'compositeFactorRank': round(composite_factor_rank),
            'revisionScore30d': revisions_30, 'revisionScore7d': revisions_7,
            'riskAdjustedMomentumScore': momentum,
            'dataStatus': data_status, 'dataConfidence': data_confidence,
            'multiYearEngineScore': round(multi_year_score) if multi_year_score is not None else None,
            'multiYearHistoryYears': multi_year.get('historyYears'),
            'multiYearRevenueTrendScore': multi_year.get('revenueTrendScore'),
            'multiYearMarginTrendScore': multi_year.get('marginTrendScore'),
            'multiYearDebtTrendScore': multi_year.get('debtTrendScore'),
            'multiYearDilutionTrendScore': multi_year.get('dilutionTrendScore'),
            'financialTrendQuality': multi_year.get('trendQuality'),
            'backtestProxy': backtest_proxy,
            **price_metrics
        }
    }

def wiki_sp500():
    cached = read_cache("wiki_sp500", 86400*7)
    if cached:
        return cached
    try:
        url = "https://en.wikipedia.org/w/api.php?action=parse&page=List_of_S%26P_500_companies&prop=text&format=json&origin=*"
        data = get_json(url, timeout=25)
        html = data.get("parse", {}).get("text", {}).get("*", "")
        rows = re.findall(r"<tr>.*?</tr>", html, flags=re.S)
        tickers = []
        for row in rows:
            m = re.search(r'<td>\s*<a[^>]*>([^<]+)</a>', row, flags=re.S)
            if m:
                t = m.group(1).strip().replace(".", "-")
                if t and t != "Symbol":
                    tickers.append(t)
        if tickers:
            write_cache("wiki_sp500", tickers)
            return tickers
    except Exception:
        pass
    return SP500_SEED

def fallback_stock(symbol, error=""):
    symbol = yahoo_symbol(symbol)
    return {
        "ok": True, "fallback": True, "ticker": symbol, "symbol": symbol, "company": symbol,
        "price": None, "pe": None, "peg": None, "roe": None, "revGrowth": None,
        "m3": None, "m6": None, "m12": None,
        "value": 0, "quality": 0, "momentum": 0, "totalScore": 0,
        "errors": {"reason": error}
    }

def build_stock(symbol, quote=None):
    symbol = yahoo_symbol(symbol)
    cached = read_cache("stock_" + symbol, 300)
    if cached:
        cached["cacheHit"] = True
        return cached
    errors = {}
    quote = quote or {}
    company = quote.get("longName") or quote.get("shortName") or symbol
    price = quote.get("regularMarketPrice")
    pe = quote.get("trailingPE")
    peg = None; roe = None; rev_growth = None
    # Extended financials
    fin_ext = {}
    try:
        summary = yahoo_summary(symbol)
        result = (summary.get("quoteSummary", {}).get("result") or [None])[0]
        if result:
            price_mod    = result.get("price", {})
            summ_detail  = result.get("summaryDetail", {})
            stats        = result.get("defaultKeyStatistics", {})
            fin          = result.get("financialData", {})
            inc_hist     = result.get("incomeStatementHistory", {})
            bal_hist     = result.get("balanceSheetHistory", {})
            cf_hist      = result.get("cashflowStatementHistory", {})
            earn_trend   = result.get("earningsTrend", {})
            rec_trend    = result.get("recommendationTrend", {})
            updown       = result.get("upgradeDowngradeHistory", {})
            asset_prof   = result.get("assetProfile", {})
            insider_holders = result.get("insiderHolders", {})
            insider_tx      = result.get("insiderTransactions", {})
            net_share_buy   = result.get("netSharePurchaseActivity", {})

            company   = raw(price_mod.get("longName")) or raw(price_mod.get("shortName")) or company
            price     = price if price is not None else raw(price_mod.get("regularMarketPrice"))
            pe        = pe if pe is not None else raw(summ_detail.get("trailingPE")) or raw(stats.get("trailingPE"))
            peg       = raw(stats.get("pegRatio"))
            roe       = raw(fin.get("returnOnEquity"))
            rev_growth= raw(fin.get("revenueGrowth"))

            # ── Income Statement (most recent annual) ──────────────
            inc_stmts = (inc_hist.get("incomeStatementHistory") or [])
            inc0 = inc_stmts[0] if inc_stmts else {}
            inc1 = inc_stmts[1] if len(inc_stmts)>1 else {}
            total_rev    = raw(inc0.get("totalRevenue"))
            total_rev_1  = raw(inc1.get("totalRevenue"))
            gross_profit = raw(inc0.get("grossProfit"))
            ebit         = raw(inc0.get("ebit"))
            net_income   = raw(inc0.get("netIncome"))
            net_income_1 = raw(inc1.get("netIncome"))

            gross_margin = (gross_profit / total_rev) if gross_profit and total_rev and total_rev!=0 else None
            net_margin   = (net_income / total_rev) if net_income and total_rev and total_rev!=0 else None
            ebit_margin  = (ebit / total_rev) if ebit and total_rev and total_rev!=0 else None
            ni_growth    = ((net_income - net_income_1) / abs(net_income_1)) if net_income and net_income_1 and net_income_1!=0 else None

            # ── Balance Sheet ──────────────────────────────────────
            bal_stmts = (bal_hist.get("balanceSheetStatements") or [])
            bal0 = bal_stmts[0] if bal_stmts else {}
            total_assets      = raw(bal0.get("totalAssets"))
            total_liab        = raw(bal0.get("totalLiab"))
            stockholder_eq    = raw(bal0.get("totalStockholderEquity"))
            cash              = raw(bal0.get("cash"))
            short_term_invest = raw(bal0.get("shortTermInvestments"))
            total_debt        = raw(bal0.get("longTermDebt"))
            short_debt        = raw(bal0.get("shortLongTermDebt"))
            current_ratio_val = raw(bal0.get("totalCurrentAssets"))
            current_liab_val  = raw(bal0.get("totalCurrentLiabilities"))
            current_ratio     = (current_ratio_val / current_liab_val) if current_ratio_val and current_liab_val and current_liab_val!=0 else None
            debt_equity       = raw(fin.get("debtToEquity"))  # already calculated
            cash_and_eq       = (cash or 0) + (short_term_invest or 0) if cash is not None else None
            total_debt_all    = (total_debt or 0) + (short_debt or 0)
            net_debt          = total_debt_all - (cash_and_eq or 0) if cash_and_eq is not None else None
            interest_coverage = None  # Yahoo free modules rarely expose interest expense consistently


            # ── Cash Flow ──────────────────────────────────────────
            cf_stmts = (cf_hist.get("cashflowStatements") or [])
            cf0 = cf_stmts[0] if cf_stmts else {}
            op_cashflow   = raw(cf0.get("totalCashFromOperatingActivities"))
            capex         = raw(cf0.get("capitalExpenditures"))
            free_cashflow = raw(fin.get("freeCashflow"))
            if free_cashflow is None and op_cashflow and capex:
                free_cashflow = op_cashflow + capex  # capex is negative
            fcf_margin = (free_cashflow / total_rev) if free_cashflow is not None and total_rev and total_rev != 0 else None

            # ── Multi-year Financial Engine (annual Yahoo history, same financials object) ──
            financial_history = []
            maxn = max(len(inc_stmts), len(bal_stmts), len(cf_stmts))
            for i in range(min(maxn, 10)):
                inc_i = inc_stmts[i] if i < len(inc_stmts) else {}
                bal_i = bal_stmts[i] if i < len(bal_stmts) else {}
                cf_i  = cf_stmts[i] if i < len(cf_stmts) else {}
                rev_i = raw(inc_i.get("totalRevenue"))
                gp_i = raw(inc_i.get("grossProfit"))
                ebit_i = raw(inc_i.get("ebit"))
                ni_i = raw(inc_i.get("netIncome"))
                assets_i = raw(bal_i.get("totalAssets"))
                eq_i = raw(bal_i.get("totalStockholderEquity"))
                cash_i = raw(bal_i.get("cash"))
                debt_i = (raw(bal_i.get("longTermDebt")) or 0) + (raw(bal_i.get("shortLongTermDebt")) or 0)
                ocf_i = raw(cf_i.get("totalCashFromOperatingActivities"))
                capex_i = raw(cf_i.get("capitalExpenditures"))
                fcf_i = (ocf_i + capex_i) if ocf_i is not None and capex_i is not None else None
                invested_i = (debt_i or 0) + (eq_i or 0) - (cash_i or 0) if eq_i is not None else None
                financial_history.append({
                    "yearIndex": i,
                    "revenue": rev_i,
                    "grossMargin": (gp_i/rev_i) if gp_i and rev_i else None,
                    "operatingMargin": (ebit_i/rev_i) if ebit_i and rev_i else None,
                    "netMargin": (ni_i/rev_i) if ni_i and rev_i else None,
                    "fcf": fcf_i,
                    "fcfMargin": (fcf_i/rev_i) if fcf_i is not None and rev_i else None,
                    "netDebt": debt_i - (cash_i or 0),
                    "shares": raw(stats.get("sharesOutstanding")) if i == 0 else None,
                    "roic": (ebit_i/invested_i) if ebit_i is not None and invested_i and invested_i > 0 else None,
                })
            multi_year_engine = calc_multi_year_engine(financial_history)

            # ── Key Statistics ─────────────────────────────────────
            ev                 = raw(stats.get("enterpriseValue"))
            ev_revenue         = raw(stats.get("enterpriseToRevenue"))
            ev_ebitda          = raw(stats.get("enterpriseToEbitda"))
            forward_pe         = raw(stats.get("forwardPE"))
            price_to_book      = raw(stats.get("priceToBook"))
            price_to_sales     = raw(summ_detail.get("priceToSalesTrailing12Months"))
            beta               = raw(summ_detail.get("beta"))
            dividend_yield     = raw(summ_detail.get("dividendYield"))
            week52_high        = raw(summ_detail.get("fiftyTwoWeekHigh"))
            week52_low         = raw(summ_detail.get("fiftyTwoWeekLow"))
            shares_out         = raw(stats.get("sharesOutstanding"))
            float_shares       = raw(stats.get("floatShares"))
            short_ratio        = raw(stats.get("shortRatio"))
            short_pct_float    = raw(stats.get("shortPercentOfFloat"))
            held_pct_inst      = raw(stats.get("heldPercentInstitutions"))
            held_pct_insider   = raw(stats.get("heldPercentInsiders"))
            profit_margin      = raw(fin.get("profitMargins"))
            op_margin          = raw(fin.get("operatingMargins"))
            gross_m_fin        = raw(fin.get("grossMargins"))
            ebitda_margin      = raw(fin.get("ebitdaMargins"))
            roa                = raw(fin.get("returnOnAssets"))
            eps_trailing       = raw(stats.get("trailingEps"))
            eps_forward        = raw(stats.get("forwardEps"))
            book_value         = raw(stats.get("bookValue"))
            revenue_per_share  = raw(fin.get("revenuePerShare"))
            total_cash_per_sh  = raw(fin.get("totalCashPerShare"))
            operating_cf       = raw(fin.get("operatingCashflow"))
            ebitda             = raw(fin.get("ebitda"))
            net_debt_ebitda    = (net_debt / ebitda) if net_debt is not None and ebitda and ebitda != 0 else None
            market_cap_val     = raw(price_mod.get("marketCap")) or quote.get("marketCap")
            working_capital    = ((current_ratio_val or 0) - (current_liab_val or 0)) if current_ratio_val is not None and current_liab_val is not None else None
            retained_earnings  = raw(bal0.get("retainedEarnings"))
            sales              = total_rev
            altman_z = None
            if total_assets and total_assets != 0:
                try:
                    altman_z = 1.2*((working_capital or 0)/total_assets) + 1.4*((retained_earnings or 0)/total_assets) + 3.3*((ebit or 0)/total_assets) + 0.6*((market_cap_val or 0)/(total_liab or 1)) + 1.0*((sales or 0)/total_assets)
                except Exception:
                    altman_z = None
            piotroski = 0
            piotroski += 1 if (net_income or 0) > 0 else 0
            piotroski += 1 if (op_cashflow or operating_cf or 0) > 0 else 0
            piotroski += 1 if roa is not None and roa > 0 else 0
            piotroski += 1 if (op_cashflow or operating_cf or 0) > (net_income or 0) else 0
            piotroski += 1 if debt_equity is not None and debt_equity < 100 else 0
            piotroski += 1 if current_ratio is not None and current_ratio > 1 else 0
            piotroski += 1 if gross_margin is not None and gross_margin > 0.25 else 0
            piotroski += 1 if op_margin is not None and op_margin > 0.08 else 0
            piotroski += 1 if rev_growth is not None and rev_growth > 0 else 0

            # ── Analyst Recommendations ────────────────────────────
            analyst_target    = raw(fin.get("targetMeanPrice"))
            analyst_low       = raw(fin.get("targetLowPrice"))
            analyst_high      = raw(fin.get("targetHighPrice"))
            analyst_median    = raw(fin.get("targetMedianPrice"))
            rec_key           = fin.get("recommendationKey") or ""
            num_analysts      = raw(fin.get("numberOfAnalystOpinions"))

            # Latest upgrade/downgrade
            last_updown = {}
            ud_items = (updown.get("history") or [])
            if ud_items:
                u = ud_items[0]
                last_updown = {
                    "firm": u.get("firm",""),
                    "action": u.get("action",""),
                    "toGrade": u.get("toGrade",""),
                    "fromGrade": u.get("fromGrade",""),
                }

            # ── Earnings trend / revisions ────────────────────────
            trend_items = (earn_trend.get("trend") or [])
            eps_est_next_q = None
            rev_est_next_q = None
            eps_rev_up_7 = eps_rev_up_30 = eps_rev_down_7 = eps_rev_down_30 = None
            eps_est_growth = None
            revenue_est_growth = None
            for t in trend_items:
                if t.get("period") == "0q":
                    ee = t.get("earningsEstimate") or {}
                    re_est = t.get("revenueEstimate") or {}
                    eps_rev = t.get("epsRevisions") or {}
                    eps_est_next_q = raw(ee.get("avg"))
                    rev_est_next_q = raw(re_est.get("avg"))
                    eps_est_growth = raw(ee.get("growth"))
                    revenue_est_growth = raw(re_est.get("growth"))
                    eps_rev_up_7 = raw(eps_rev.get("upLast7days"))
                    eps_rev_up_30 = raw(eps_rev.get("upLast30days"))
                    eps_rev_down_7 = raw(eps_rev.get("downLast7days"))
                    eps_rev_down_30 = raw(eps_rev.get("downLast30days"))
                    break

            # Gross margin prefer finviz data
            if gross_m_fin:
                gross_margin = gross_m_fin

            fin_ext = {
                # Profile
                "sector": asset_prof.get("sector") or "",
                "industry": asset_prof.get("industry") or "",
                "country": asset_prof.get("country") or "",
                "fullTimeEmployees": asset_prof.get("fullTimeEmployees"),
                "businessSummary": (asset_prof.get("longBusinessSummary") or "")[:400],
                # Income
                "totalRevenue": total_rev,
                "grossProfit": gross_profit,
                "ebit": ebit,
                "ebitda": ebitda,
                "netIncome": net_income,
                "grossMargin": gross_margin,
                "ebitMargin": ebit_margin,
                "netMargin": net_margin if net_margin else profit_margin,
                "operatingMargin": op_margin,
                "ebitdaMargin": ebitda_margin,
                "niGrowth": ni_growth,
                # Balance sheet
                "totalAssets": total_assets,
                "totalLiabilities": total_liab,
                "stockholderEquity": stockholder_eq,
                "cashAndEquivalents": cash_and_eq,
                "totalDebt": total_debt_all,
                "netDebt": net_debt,
                "netDebtEbitda": net_debt_ebitda,
                "interestCoverage": interest_coverage,
                "altmanZ": altman_z,
                "piotroskiF": piotroski,
                "currentRatio": current_ratio,
                "debtToEquity": debt_equity,
                # Cash flow
                "operatingCashflow": op_cashflow or operating_cf,
                "capex": capex,
                "freeCashflow": free_cashflow,
                "fcfMargin": fcf_margin,
                # Valuation
                "forwardPE": forward_pe,
                "priceToBook": price_to_book,
                "priceToSales": price_to_sales,
                "evRevenue": ev_revenue,
                "evEbitda": ev_ebitda,
                "enterpriseValue": ev,
                "marketCap": market_cap_val,
                "beta": beta,
                "dividendYield": dividend_yield,
                "week52High": week52_high,
                "week52Low": week52_low,
                # Per share
                "epsTrailing": eps_trailing,
                "epsForward": eps_forward,
                "bookValue": book_value,
                "revenuePerShare": revenue_per_share,
                "totalCashPerShare": total_cash_per_sh,
                # Share structure
                "sharesOutstanding": shares_out,
                "floatShares": float_shares,
                "shortRatio": short_ratio,
                "shortPctFloat": short_pct_float,
                "heldByInstitutions": held_pct_inst,
                "heldByInsiders": held_pct_insider,
                "insiderNetShares": raw(net_share_buy.get("netSharesPurchased")) if isinstance(net_share_buy, dict) else None,
                "insiderNetPercent": raw(net_share_buy.get("netPercentInsiderShares")) if isinstance(net_share_buy, dict) else None,
                # Return metrics
                "roa": roa,
                # Analyst
                "analystTargetMean": analyst_target,
                "analystTargetLow": analyst_low,
                "analystTargetHigh": analyst_high,
                "analystTargetMedian": analyst_median,
                "recommendationKey": rec_key,
                "numberOfAnalysts": num_analysts,
                "lastUpDowngrade": last_updown,
                # Earnings estimates / revisions (same financials object, no top-level API change)
                "epsEstNextQ": eps_est_next_q,
                "revEstNextQ": rev_est_next_q,
                "epsRevUpLast7days": eps_rev_up_7,
                "epsRevUpLast30days": eps_rev_up_30,
                "epsRevDownLast7days": eps_rev_down_7,
                "epsRevDownLast30days": eps_rev_down_30,
                "epsEstimateGrowth": eps_est_growth,
                "revenueEstimateGrowth": revenue_est_growth,
                # 100% top roadmap engines
                "financialHistory": financial_history,
                "multiYearEngine": multi_year_engine,
            }
        else:
            errors["summary"] = str(summary.get("quoteSummary", {}).get("error"))
    except Exception as e:
        errors["summary"] = str(e)

    try:
        chart = yahoo_chart(symbol)
        chart_price, m3, m6, m12 = calc_returns_from_chart(chart)
        price_metrics = calc_price_structure(chart)
        if price is None:
            price = chart_price
    except Exception as e:
        errors["chart"] = str(e)
        m3 = m6 = m12 = None
        price_metrics = {}

    if not any(v is not None for v in [price, pe, peg, roe, rev_growth, m3, m6, m12]):
        stale = read_stale_stock(symbol)
        if stale:
            return mark_fallback_row(stale, "Stale local stock fallback", "; ".join(str(v) for v in errors.values() if v) or "Yahoo returned no usable data")

    value, quality, momentum, total = score_components(pe, peg, roe, rev_growth, m3, m6, m12)
    fin_ext["price"] = price
    pro_scores = calc_professional_scores(fin_ext, pe, peg, roe, rev_growth, m3, m6, m12, price_metrics)
    value, quality, momentum, total = pro_scores["value"], pro_scores["quality"], pro_scores["momentum"], pro_scores["total"]
    fin_ext.update(pro_scores.get("derived", {}))

    # ── Financial Score (separate from VQM) ──────────────────────────
    fin_score = calc_financial_score(fin_ext)

    out = {
        "ok": True, "source": "Yahoo Finance", "ticker": symbol, "symbol": symbol, "company": company,
        "price": price,
        "marketCap": quote.get("marketCap") or fin_ext.get("enterpriseValue"),
        "change": quote.get("regularMarketChangePercent"),
        "pe": pe, "peg": peg, "roe": roe, "revGrowth": rev_growth,
        "m3": m3, "m6": m6, "m12": m12,
        "sector": fin_ext.get("sector", ""),
        "industry": fin_ext.get("industry", ""),
        "value": value, "quality": quality, "momentum": momentum, "totalScore": total,
        "revisions": pro_scores.get("revisions", 0), "risk": pro_scores.get("risk", 0), "sentimentCatalyst": pro_scores.get("sentimentCatalyst", 0),
        "proScores": pro_scores,
        "financials": fin_ext,
        "financialScore": fin_score,
        "errors": errors,
    }
    if price is not None or pe is not None or peg is not None or roe is not None or m3 is not None or m6 is not None or m12 is not None:
        write_cache("stock_" + symbol, out)
        write_cache("stock_fallback_" + symbol, out)
    return out


def _finnhub_json(path, token, timeout=18):
    token = (token or "").strip()
    if not token:
        raise ValueError("Missing Finnhub token")
    sep = "&" if "?" in path else "?"
    url = "https://finnhub.io/api/v1" + path + sep + urllib.parse.urlencode({"token": token})
    return get_json(url, timeout=timeout)

def _safe_float(v):
    try:
        if isinstance(v, dict):
            v = raw(v)
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None

def _ratioish(v):
    v = _safe_float(v)
    if v is None:
        return None
    return v / 100.0 if abs(v) > 2.5 else v

def _first_metric(metrics, keys, ratio=False):
    metrics = metrics or {}
    for key in keys:
        v = _safe_float(metrics.get(key))
        if v is not None:
            return _ratioish(v) if ratio else v
    return None

def _reported_value(report, names):
    names = [n.lower() for n in names]
    for section in ("ic", "bs", "cf"):
        for item in ((report or {}).get(section) or []):
            hay = " ".join(str(item.get(k, "")) for k in ("concept", "label", "unit")).lower()
            if any(n in hay for n in names):
                v = _safe_float(item.get("value"))
                if v is not None:
                    return v
    return None

def _set_if_value(target, key, value, sources, label):
    if value is not None:
        target[key] = value
        sources.add(label)

def _latest_earnings_days(symbol, token):
    today = time.strftime("%Y-%m-%d", time.localtime())
    to_day = time.strftime("%Y-%m-%d", time.localtime(time.time() + 86400 * 180))
    try:
        cal = _finnhub_json("/calendar/earnings?" + urllib.parse.urlencode({"symbol": symbol, "from": today, "to": to_day}), token)
        rows = cal.get("earningsCalendar") or []
        if not rows:
            return None
        d = rows[0].get("date")
        if not d:
            return None
        y, m, day = [int(x) for x in d.split("-")]
        ts = time.mktime((y, m, day, 0, 0, 0, 0, 0, -1))
        return max(0, round((ts - time.time()) / 86400))
    except Exception:
        return None

def _recommendation_key(recs):
    if not recs:
        return None
    r = recs[0]
    bullish = int(r.get("strongBuy") or 0) * 2 + int(r.get("buy") or 0)
    bearish = int(r.get("strongSell") or 0) * 2 + int(r.get("sell") or 0)
    hold = int(r.get("hold") or 0)
    if bullish >= bearish + hold:
        return "buy"
    if bearish > bullish:
        return "sell"
    return "hold"

def apply_finnhub_overlay(stock, symbol, token):
    symbol = yahoo_symbol(symbol)
    f = stock.setdefault("financials", {})
    errors = stock.setdefault("errors", {})
    sources = set(f.get("finnhubSources") or [])

    metric = {}
    try:
        metric = (_finnhub_json("/stock/metric?" + urllib.parse.urlencode({"symbol": symbol, "metric": "all"}), token).get("metric") or {})
        sources.add("Finnhub basic financials")
    except Exception as e:
        errors["finnhubMetric"] = str(e)

    profile = {}
    try:
        profile = _finnhub_json("/stock/profile2?" + urllib.parse.urlencode({"symbol": symbol}), token)
        if profile:
            sources.add("Finnhub profile")
    except Exception as e:
        errors["finnhubProfile"] = str(e)

    recs = []
    try:
        recs = _finnhub_json("/stock/recommendation?" + urllib.parse.urlencode({"symbol": symbol}), token)
        if isinstance(recs, list) and recs:
            sources.add("Finnhub recommendation trends")
    except Exception as e:
        errors["finnhubRecommendation"] = str(e)

    reported = {}
    try:
        fr = _finnhub_json("/stock/financials-reported?" + urllib.parse.urlencode({"symbol": symbol, "freq": "annual"}), token, timeout=25)
        rows = fr.get("data") or []
        if rows:
            reported = rows[0].get("report") or {}
            sources.add("Finnhub financials as reported")
    except Exception as e:
        errors["finnhubReported"] = str(e)

    insider_rows = []
    try:
        ins = _finnhub_json("/stock/insider-transactions?" + urllib.parse.urlencode({"symbol": symbol}), token)
        insider_rows = ins.get("data") or []
        if insider_rows:
            sources.add("Finnhub insider transactions")
    except Exception as e:
        errors["finnhubInsider"] = str(e)

    if profile:
        stock["company"] = profile.get("name") or stock.get("company")
        stock["marketCap"] = _safe_float(profile.get("marketCapitalization")) * 1000000 if _safe_float(profile.get("marketCapitalization")) is not None else stock.get("marketCap")
        f["country"] = profile.get("country") or f.get("country", "")
        f["industry"] = profile.get("finnhubIndustry") or f.get("industry", "")
        f["ipo"] = profile.get("ipo") or f.get("ipo")

    _set_if_value(f, "priceToSales", _first_metric(metric, ("psTTM", "psAnnual")), sources, "Finnhub basic financials")
    _set_if_value(f, "priceToBook", _first_metric(metric, ("pbAnnual", "pbQuarterly")), sources, "Finnhub basic financials")
    _set_if_value(f, "forwardPE", _first_metric(metric, ("forwardPE", "peNormalizedAnnual")), sources, "Finnhub basic financials")
    _set_if_value(f, "beta", _first_metric(metric, ("beta",)), sources, "Finnhub basic financials")
    _set_if_value(f, "week52High", _first_metric(metric, ("52WeekHigh",)), sources, "Finnhub basic financials")
    _set_if_value(f, "week52Low", _first_metric(metric, ("52WeekLow",)), sources, "Finnhub basic financials")
    _set_if_value(f, "grossMargin", _first_metric(metric, ("grossMarginTTM", "grossMarginAnnual"), ratio=True), sources, "Finnhub basic financials")
    _set_if_value(f, "operatingMargin", _first_metric(metric, ("operatingMarginTTM", "operatingMarginAnnual"), ratio=True), sources, "Finnhub basic financials")
    _set_if_value(f, "netMargin", _first_metric(metric, ("netProfitMarginTTM", "netProfitMarginAnnual"), ratio=True), sources, "Finnhub basic financials")
    _set_if_value(f, "roa", _first_metric(metric, ("roaTTM", "roaRfy"), ratio=True), sources, "Finnhub basic financials")
    _set_if_value(f, "currentRatio", _first_metric(metric, ("currentRatioAnnual", "currentRatioQuarterly")), sources, "Finnhub basic financials")
    _set_if_value(f, "debtToEquity", _first_metric(metric, ("totalDebt/totalEquityAnnual", "totalDebt/totalEquityQuarterly")), sources, "Finnhub basic financials")

    pe = _first_metric(metric, ("peTTM", "peBasicExclExtraTTM", "peNormalizedAnnual"))
    roe = _first_metric(metric, ("roeTTM", "roeRfy"), ratio=True)
    rev_growth = _first_metric(metric, ("revenueGrowthTTMYoy", "revenueGrowth3Y", "revenueGrowth5Y"), ratio=True)
    if pe is not None:
        stock["pe"] = pe
    if roe is not None:
        stock["roe"] = roe
    if rev_growth is not None:
        stock["revGrowth"] = rev_growth

    if reported:
        total_rev = _reported_value(reported, ("revenue", "revenues", "sales revenue"))
        gross_profit = _reported_value(reported, ("gross profit", "gross income"))
        ebit = _reported_value(reported, ("operating income", "ebit", "income from operations"))
        net_income = _reported_value(reported, ("net income", "net earnings", "profit loss"))
        total_assets = _reported_value(reported, ("total assets",))
        total_liab = _reported_value(reported, ("total liabilities", "total liabilities and"))
        equity = _reported_value(reported, ("stockholders equity", "shareholders equity", "total equity"))
        cash = _reported_value(reported, ("cash and cash equivalents", "cash equivalents"))
        debt = _reported_value(reported, ("long term debt", "short term borrowings", "total debt"))
        op_cf = _reported_value(reported, ("net cash provided by operating", "operating cash flow"))
        capex = _reported_value(reported, ("payments to acquire property", "capital expenditures"))
        interest_expense = _reported_value(reported, ("interest expense",))

        _set_if_value(f, "totalRevenue", total_rev, sources, "Finnhub financials as reported")
        _set_if_value(f, "grossProfit", gross_profit, sources, "Finnhub financials as reported")
        _set_if_value(f, "ebit", ebit, sources, "Finnhub financials as reported")
        _set_if_value(f, "netIncome", net_income, sources, "Finnhub financials as reported")
        _set_if_value(f, "totalAssets", total_assets, sources, "Finnhub financials as reported")
        _set_if_value(f, "totalLiabilities", total_liab, sources, "Finnhub financials as reported")
        _set_if_value(f, "stockholderEquity", equity, sources, "Finnhub financials as reported")
        _set_if_value(f, "cashAndEquivalents", cash, sources, "Finnhub financials as reported")
        _set_if_value(f, "totalDebt", debt, sources, "Finnhub financials as reported")
        _set_if_value(f, "operatingCashflow", op_cf, sources, "Finnhub financials as reported")
        _set_if_value(f, "capex", capex, sources, "Finnhub financials as reported")
        if op_cf is not None and capex is not None:
            f["freeCashflow"] = op_cf + capex
        if total_rev:
            if gross_profit is not None:
                f["grossMargin"] = gross_profit / total_rev
            if ebit is not None:
                f["ebitMargin"] = ebit / total_rev
            if net_income is not None:
                f["netMargin"] = net_income / total_rev
            if f.get("freeCashflow") is not None:
                f["fcfMargin"] = f["freeCashflow"] / total_rev
        if interest_expense and ebit is not None:
            f["interestCoverage"] = ebit / abs(interest_expense)
        if equity and debt is not None:
            f["debtToEquity"] = debt / equity

    rec_key = _recommendation_key(recs)
    if rec_key:
        f["recommendationKey"] = rec_key
        latest = recs[0]
        f["numberOfAnalysts"] = sum(int(latest.get(k) or 0) for k in ("strongBuy", "buy", "hold", "sell", "strongSell"))

    days_to_earnings = _latest_earnings_days(symbol, token)
    if days_to_earnings is not None:
        f["daysToEarnings"] = days_to_earnings
        sources.add("Finnhub earnings calendar")

    if insider_rows:
        net = 0.0
        for row in insider_rows[:80]:
            net += _safe_float(row.get("change")) or 0.0
        f["insiderNetShares"] = net

    f["finnhubUpdatedAt"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    f["finnhubSources"] = sorted(sources)
    f["finnhubOverlay"] = True

    price_metrics = {k: f.get(k) for k in ("volatility30d", "volatility90d", "maxDrawdown1y", "downsideDeviation", "relativeIndexStrengthScore") if f.get(k) is not None}
    pro_scores = calc_professional_scores(f, stock.get("pe"), stock.get("peg"), stock.get("roe"), stock.get("revGrowth"), stock.get("m3"), stock.get("m6"), stock.get("m12"), price_metrics)
    f.update(pro_scores.get("derived", {}))
    stock["value"] = pro_scores.get("value", stock.get("value", 0))
    stock["quality"] = pro_scores.get("quality", stock.get("quality", 0))
    stock["momentum"] = pro_scores.get("momentum", stock.get("momentum", 0))
    stock["totalScore"] = pro_scores.get("total", stock.get("totalScore", 0))
    stock["revisions"] = pro_scores.get("revisions", stock.get("revisions", 0))
    stock["risk"] = pro_scores.get("risk", stock.get("risk", 0))
    stock["sentimentCatalyst"] = pro_scores.get("sentimentCatalyst", stock.get("sentimentCatalyst", 0))
    stock["proScores"] = pro_scores
    stock["financialScore"] = calc_financial_score(f)
    stock["source"] = "Yahoo Finance + Finnhub overlay"
    stock["finnhubOverlay"] = True
    return stock


def calc_financial_score(f):
    """Institutional Financial Health Score v4.

    Keeps the same return shape {total, pillars}, but replaces the old classic
    six-pillar health model with a deeper balance-sheet / cash-quality model:
      • Piotroski F-score
      • Altman Z-score
      • Interest coverage
      • Debt quality / maturity proxy
      • Cash-flow quality
      • Shareholder health
      • Earnings quality
      • Liquidity health
      • Multi-year stability and cyclicality penalty are blended in.
    """
    if not f:
        return {"total": 0, "pillars": {}, "model": "institutional_financial_health_v4"}

    def clamp(x): return max(0.0, min(100.0, float(x)))
    def safe(v, default=None):
        try: return float(v) if v is not None else default
        except Exception: return default
    def weighted(parts, fallback=50):
        usable = [(safe(v), w) for v, w in parts if safe(v) is not None]
        sw = sum(w for _, w in usable)
        return clamp(sum(v*w for v, w in usable) / sw) if sw else fallback
    def score_ratio(v, low, mid, high):
        v = safe(v)
        if v is None: return None
        if v >= high: return 100
        if v >= mid: return 70 + (v-mid) / max(1e-9, high-mid) * 30
        if v >= low: return 35 + (v-low) / max(1e-9, mid-low) * 35
        return clamp(15 + v / max(1e-9, low) * 20)

    revenue = safe(f.get("totalRevenue"))
    ebit = safe(f.get("ebit"))
    ebitda = safe(f.get("ebitda"))
    interest_coverage_raw = safe(f.get("interestCoverage"))
    net_income = safe(f.get("netIncome"))
    op_cf = safe(f.get("operatingCashflow"))
    fcf = safe(f.get("freeCashflow"))
    capex = safe(f.get("capex"))
    total_assets = safe(f.get("totalAssets"))
    total_liab = safe(f.get("totalLiabilities"))
    total_debt = safe(f.get("totalDebt"), 0)
    net_debt = safe(f.get("netDebt"))
    cash = safe(f.get("cashAndEquivalents"), 0)
    current_ratio = safe(f.get("currentRatio"))
    debt_to_equity = safe(f.get("debtToEquity"))
    net_debt_ebitda = safe(f.get("netDebtEbitda"))
    altman_z = safe(f.get("altmanZ"))
    piotroski = safe(f.get("piotroskiF"))
    fcf_margin = safe(f.get("fcfMargin"))
    net_margin = safe(f.get("netMargin"))
    gross_margin = safe(f.get("grossMargin"))
    operating_margin = safe(f.get("operatingMargin") or f.get("ebitMargin"))
    sbc_ratio = safe(f.get("sbcRatio"))
    dilution_score_in = safe(f.get("dilutionScore"))
    shares = safe(f.get("sharesOutstanding"))
    beta = safe(f.get("beta"))
    sector = (f.get("sector") or "").lower()
    industry = (f.get("industry") or "").lower()

    # 1) Piotroski F-score: 0-9 → 0-100. Proxy already calculated from Yahoo statements.
    piotroski_score = clamp((piotroski / 9.0) * 100) if piotroski is not None else 50

    # 2) Altman Z-score: bankruptcy / credit stress layer.
    if altman_z is None:
        altman_score = 50
    elif altman_z >= 3.0:
        altman_score = 90 + min(10, (altman_z-3.0)*3)
    elif altman_z >= 2.0:
        altman_score = 60 + (altman_z-2.0)*30
    elif altman_z >= 1.8:
        altman_score = 45 + (altman_z-1.8)*75
    else:
        altman_score = clamp(15 + altman_z*16)

    # 3) Interest coverage. If Yahoo lacks interest expense, use net-debt/EBITDA and debt ratios as proxy.
    if interest_coverage_raw is not None:
        interest_cover_score = score_ratio(interest_coverage_raw, 2, 5, 10)
    else:
        nde = net_debt_ebitda
        interest_cover_score = weighted([
            (clamp(95 - max(0, nde or 0)*18) if nde is not None else None, .55),
            (clamp(90 - max(0, (debt_to_equity or 0)-40)*0.35) if debt_to_equity is not None else None, .25),
            (100 if (net_debt is not None and net_debt <= 0) else None, .20),
        ], fallback=55)

    # 4) Debt quality / debt maturity risk proxy.
    # Free Yahoo does not expose maturity schedule consistently, so refinancing risk is proxied by leverage,
    # net cash, EBITDA coverage and balance-sheet strength.
    debt_quality = weighted([
        (clamp(100 - max(0, (net_debt_ebitda or 0))*18) if net_debt_ebitda is not None else None, .35),
        (clamp(95 - max(0, (debt_to_equity or 0)-30)*0.35) if debt_to_equity is not None else None, .25),
        (clamp(50 + (cash or 0) / max(1, abs(total_debt or 1)) * 35) if total_debt else 95, .20),
        (altman_score, .20),
    ], fallback=55)

    # 5) Cash-flow quality: OCF trend/proxy, FCF consistency/proxy, cash conversion, capex burden.
    cash_conversion = (op_cf / net_income) if op_cf is not None and net_income not in (None, 0) else None
    fcf_conversion = (fcf / net_income) if fcf is not None and net_income not in (None, 0) else None
    capex_burden = (abs(capex) / revenue) if capex is not None and revenue else None
    cashflow_quality = weighted([
        (clamp(45 + (cash_conversion or 0)*30) if cash_conversion is not None else None, .30),
        (clamp(45 + (fcf_conversion or 0)*30) if fcf_conversion is not None else None, .25),
        (clamp(50 + (fcf_margin or 0)*240) if fcf_margin is not None else None, .20),
        (clamp(90 - (capex_burden or 0)*250) if capex_burden is not None else None, .15),
        (f.get("multiYearCashFlowTrendScore"), .10),
    ], fallback=55)

    # 6) Shareholder health: SBC, dilution and buyback efficiency proxies.
    multi = f.get("multiYearEngine") or {}
    dilution_trend = safe(multi.get("dilutionTrendScore"))
    buyback_eff_proxy = safe(f.get("buybackEfficiencyProxy"))
    insider_net_pct = safe(f.get("insiderNetPercent"))
    shareholder_health = weighted([
        (clamp(90 - (sbc_ratio or 0)*500) if sbc_ratio is not None else None, .30),
        (dilution_score_in, .25),
        (dilution_trend, .20),
        (buyback_eff_proxy, .15),
        (clamp(55 + (insider_net_pct or 0)*900) if insider_net_pct is not None else None, .10),
    ], fallback=55)

    # 7) Earnings quality layer: accrual ratio, one-time adjustment risk, margin sustainability.
    accrual_ratio = ((net_income or 0) - (op_cf or 0)) / total_assets if total_assets and net_income is not None and op_cf is not None else None
    accrual_score = clamp(70 - (accrual_ratio or 0)*280) if accrual_ratio is not None else None
    one_time_adjustment_proxy = safe(f.get("oneTimeAdjustmentRiskProxy"))
    margin_sustainability = safe(f.get("marginSustainabilityScore"))
    deferred_revenue_quality = 55  # Free Yahoo modules do not consistently provide deferred revenue.
    earnings_quality = weighted([
        (accrual_score, .35),
        (one_time_adjustment_proxy, .20),
        (margin_sustainability, .25),
        (deferred_revenue_quality, .10),
        (clamp(50 + (fcf_margin or 0)*220) if fcf_margin is not None else None, .10),
    ], fallback=55)

    # 8) Liquidity stress: current ratio, cash runway, short-term obligations/refinancing proxy.
    cash_runway_proxy = None
    if fcf is not None and fcf < 0 and cash:
        cash_runway_proxy = clamp((cash / max(1, abs(fcf))) / 2.0 * 100)  # 2+ years runway ≈ 100
    elif fcf is not None and fcf >= 0:
        cash_runway_proxy = 90
    liquidity_health = weighted([
        (score_ratio(current_ratio, .8, 1.5, 2.5) if current_ratio is not None else None, .35),
        (cash_runway_proxy, .25),
        (clamp(100 - max(0, (net_debt_ebitda or 0))*20) if net_debt_ebitda is not None else None, .20),
        (debt_quality, .20),
    ], fallback=55)

    # 9) Multi-year stability overlay: 5–10Y debt/cashflow/margin/leverage trend quality.
    multi_year_stability = weighted([
        (safe(multi.get("debtTrendScore")), .30),
        (safe(multi.get("marginTrendScore")), .30),
        (safe(multi.get("revenueTrendScore")), .20),
        (safe(multi.get("multiYearScore")), .20),
    ], fallback=55)

    # 10) Cyclicality risk penalty/adjustment.
    cyclicality_penalty = 0
    cyclic_text = sector + " " + industry
    if any(x in cyclic_text for x in ["airline", "automobile", "auto", "steel", "metal", "mining", "energy", "oil", "gas", "semiconductor", "cyclical"]):
        cyclicality_penalty = 6
    if beta is not None and beta > 1.7:
        cyclicality_penalty += 4
    if beta is not None and beta < 0.8:
        cyclicality_penalty -= 2

    weights = {
        "piotroski": 0.20,
        "altmanZ": 0.15,
        "interestCover": 0.15,
        "debtQuality": 0.15,
        "cashflowQuality": 0.15,
        "shareholderHealth": 0.10,
        "earningsQuality": 0.05,
        "liquidityHealth": 0.05,
    }
    pillars = {
        "piotroski": round(piotroski_score),
        "altmanZ": round(clamp(altman_score)),
        "interestCover": round(clamp(interest_cover_score)),
        "debtQuality": round(clamp(debt_quality)),
        "cashflowQuality": round(clamp(cashflow_quality)),
        "shareholderHealth": round(clamp(shareholder_health)),
        "earningsQuality": round(clamp(earnings_quality)),
        "liquidityHealth": round(clamp(liquidity_health)),
        "multiYearStability": round(clamp(multi_year_stability)),
        "cyclicalityRisk": round(clamp(100 - cyclicality_penalty*8)),
    }
    core_total = sum(pillars[k]*weights[k] for k in weights)
    total = round(clamp(core_total*0.88 + pillars["multiYearStability"]*0.08 + pillars["cyclicalityRisk"]*0.04))

    data_status = {
        "Piotroski F": "LIVE DATA" if f.get("piotroskiF") is not None else "PROXY",
        "Altman Z": "LIVE DATA" if f.get("altmanZ") is not None else "PROXY",
        "Interest Coverage": "LIVE DATA" if f.get("interestCoverage") is not None else "PROXY",
        "Debt Maturity Risk": "PROXY",
        "Cash Flow Quality": "LIVE DATA" if f.get("operatingCashflow") is not None and f.get("freeCashflow") is not None else "PROXY",
        "Shareholder Health": "PROXY",
        "Earnings Quality": "LIVE DATA" if accrual_ratio is not None else "PROXY",
        "Multi-Year Stability": multi.get("trendQuality") or "MISSING",
        "Cyclicality Risk": "PROXY",
        "Liquidity Stress": "LIVE DATA" if f.get("currentRatio") is not None else "PROXY",
    }
    return {
        "total": total,
        "pillars": pillars,
        "weights": weights,
        "model": "institutional_financial_health_v4",
        "dataStatus": data_status,
        "derived": {
            "accrualRatio": accrual_ratio,
            "cashConversion": cash_conversion,
            "fcfConversion": fcf_conversion,
            "capexBurden": capex_burden,
            "cyclicalityPenalty": cyclicality_penalty,
        }
    }

def get_stocks_batch(symbols):
    symbols = [yahoo_symbol(s) for s in symbols if s.strip()]
    symbols = symbols[:MAX_BATCH_SYMBOLS]
    quote_map = {}
    try:
        quote_map = yahoo_quote_batch(symbols)
        quote_error = ""
    except Exception as e:
        quote_error = str(e)
    results = []
    with ThreadPoolExecutor(max_workers=BATCH_WORKERS) as ex:
        futures = {}
        for sym in symbols:
            futures[ex.submit(build_stock, sym, quote_map.get(sym, {}))] = sym
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                r = fut.result()
                if quote_error:
                    r.setdefault("errors", {})["quoteBatchError"] = quote_error
                results.append(r)
            except Exception as e:
                results.append(fallback_stock(sym, str(e)))
    order = {s:i for i,s in enumerate(symbols)}
    results.sort(key=lambda x: order.get((x.get("ticker") or x.get("symbol") or "").upper(), 9999))
    return results

def get_universe(mode):
    out = []
    mode = (mode or "both").lower()

    if mode == "mega":
        out.extend(SP500_QQQ_CUSTOM)
        for vals in THEME_UNIVERSES.values():
            out.extend(vals)

    elif mode in THEME_UNIVERSES:
        out.extend(THEME_UNIVERSES.get(mode, []))

    elif mode in ("sp500", "both", "sp500qqq"):
        out.extend(SP500_QQQ_CUSTOM)

    elif mode == "qqq":
        out.extend(QQQ_FALLBACK)

    elif mode == "smallcaps":
        # Fast local small-cap universe; avoids frontend timeouts / Failed to fetch.
        out.extend(get_small_caps_fast(SMALLCAP_MAX_LIMIT))

    elif mode == "demo":
        out = ["NVDA","AAPL","MSFT","AMZN","META","AVGO","AMD","PLTR","ANET","SMCI","MU","CRWD","GOOGL","TSLA","ORCL"]

    else:
        out.extend(SP500_QQQ_CUSTOM)

    seen = []
    for x in out:
        x = yahoo_symbol(x)
        if x and x not in seen:
            seen.append(x)
    return seen




def parse_tickers_from_html(page):
    out = []
    if not page:
        return out
    patterns = [
        r'quote\.ashx\?t=([A-Za-z0-9.\-]+)',
        r'href="/quote/([A-Za-z0-9.\-^=]+)[/?"]',
        r'data-symbol="([A-Za-z0-9.\-^=]+)"',
        r'"symbol"\s*:\s*"([A-Za-z0-9.\-^=]+)"'
    ]
    for pat in patterns:
        for t in re.findall(pat, page):
            t = yahoo_symbol(t)
            if t and len(t) <= 14 and not t.startswith("^") and t not in out:
                out.append(t)
    return out



SMALLCAP_MAX_LIMIT = 7000
WEEK_BREAKOUT_ONLINE_LIMIT = 7000

def _coerce_optional_limit(value, default=None, max_limit=SMALLCAP_MAX_LIMIT):
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in ("", "all", "alle", "full", "max", "none", "0"):
        return default
    try:
        return max(10, min(max_limit, int(float(text))))
    except Exception:
        return default

def get_small_caps_from_nasdaq_api(limit=7000):
    """
    Fetch up to 7000 US-listed stock symbols from Nasdaq screener API.
    This is used for the Small Caps screener so the UI can actually receive
    the requested ticker candidates instead of the short local fallback.
    Market-cap filtering/scoring is handled later by Yahoo data enrichment.
    """
    limit = max(10, min(SMALLCAP_MAX_LIMIT, int(limit or 7000)))
    cached = read_cache("smallcaps_nasdaq_api_symbols_" + str(limit), 1800)
    if cached and len(cached) >= min(limit, 500):
        return cached[:limit]
    symbols = []
    # Pull larger pages so 7k requests do not require hundreds of network calls.
    page_size = 1000
    for offset in range(0, limit + page_size, page_size):
        if len(symbols) >= limit:
            break
        url = "https://api.nasdaq.com/api/screener/stocks?" + urllib.parse.urlencode({
            "tableonly": "true",
            "limit": str(page_size),
            "offset": str(offset),
            "download": "true"
        })
        try:
            status, txt = http_get(url, timeout=20, accept="application/json,text/plain,*/*")
            data = json.loads(txt)
            rows = (((data.get("data") or {}).get("rows")) or [])
            for r in rows:
                sym = (r.get("symbol") or r.get("Symbol") or "").strip().upper()
                # Exclude warrants/units/preferred/classes that Yahoo often rejects.
                if not sym or len(sym) > 8: continue
                if any(x in sym for x in ["^", "/", " "]): continue
                if sym.endswith(("W", "U")) and len(sym) >= 4: continue
                sym = yahoo_symbol(sym)
                if sym and sym not in symbols:
                    symbols.append(sym)
        except Exception:
            continue
    if symbols:
        write_cache("smallcaps_nasdaq_api_symbols_" + str(limit), symbols)
    return symbols[:limit]

def get_small_caps_from_yahoo_page(limit=1000):
    limit = max(10, min(SMALLCAP_MAX_LIMIT, int(limit or 1000)))
    cached = read_cache("smallcaps_yahoo_page_symbols_" + str(limit), 1800)
    if cached:
        return cached[:limit]
    symbols = []
    urls = [
        "https://finance.yahoo.com/markets/stocks/small-cap-stocks/",
        "https://finance.yahoo.com/markets/stocks/most-active/",
        "https://finance.yahoo.com/markets/stocks/gainers/"
    ]
    for url in urls:
        if len(symbols) >= limit:
            break
        try:
            status, page = http_get(url, timeout=25, accept="text/html,*/*")
            for t in parse_tickers_from_html(page):
                if t not in symbols:
                    symbols.append(t)
        except Exception:
            pass
    write_cache("smallcaps_yahoo_page_symbols_" + str(limit), symbols)
    return symbols[:limit]



# Fast local fallback used by the UI Small Caps screener.
# External pages like Finviz/Yahoo can be slow or blocked, so the default is instant local universe.
# Set SMALLCAPS_ONLINE=1 before starting the server if you explicitly want online scraping.
FAST_SMALLCAP_FALLBACK = [
    "SOFI","RKLB","IONQ","OPEN","JOBY","ACHR","ASTS","HIMS","HOOD","RGTI","SOUN","RXRX","DNA","BBAI","AI",
    "UPST","AFRM","LCID","QS","ENVX","IREN","CORZ","BITF","CLSK","RIOT","MARA","HUT","WULF","APLD","NBIS",
    "SMR","OKLO","CRDO","AEHR","INDI","LUNR","DUOL","APP","FROG","PATH","CFLT","ESTC","BILL","TOST","MQ",
    "NU","TMDX","INSP","PRCT","AXON","CELH","CROX","ELF","IOT","TENB","ZI","BOX","DBX","GTLB","ALGM","CAMT",
    "COHR","FORM","IPGP","LSCC","MTSI","POWI","RMBS","SMTC","SYNA","TSEM","UCTT","VECO","ACLS","ENPH",
    "SEDG","RUN","NOVA","BE","PLUG","FCEL","CHPT","EVGO","BLNK","ARRY","SHLS","STEM","FLNC","EOSE",
    "VKTX","HALO","EXEL","IONS","ARWR","BEAM","CRSP","EDIT","NTLA","TWST","TXG","PACB","NVAX","AXSM",
    "CYTK","KURA","RARE","VCEL","XENE","ZLAB","CDE","HL","AG","EXK","FSM","SILV","MAG","BTG","EGO",
    "KGC","PAAS","SSRM","AGI","IAG","NGD","SAND","AA","ATI","CLF","CENX","KALU","MT","NUE","STLD",
    "UEC","UUUU","NXE","DNN","AR","RRC","CNX","SM","MTDR","PR","CHRD","CIVI","CRK","KOS","MUR","NOG",
    "BANC","CADE","COLB","FHB","FHN","FITB","HBAN","HWC","KEY","OZK","SNV","UBSI","UMBF","VLY","WAL",
    "ALLY","COOP","ENVA","LC","NAVI","OMF","SLM","ABG","AN","CVNA","GPI","LAD","PAG","SAH","KMX","AAP",
    "AZO","ORLY","RH","BBY","CHWY","ETSY","FIVE","GME","URBN","BOOT","BURL","DKS","FL","LEVI","OLLI",
    "PLAY","SHAK","TXRH","WING","YETI","AAL","ALK","DAL","JBLU","LUV","SKYW","ULCC","CPA","ATSG",
    "EXPD","HUBG","KNX","LSTR","MRTN","ODFL","SAIA","SNDR","WERN","ARCB","CHRW","XPO","RXO","MATX","ZIM",
    "BMHL","VOXR","KULR","QBTS","RGTI","SERV","GRAL","DAVE","NNE","LEU","LTBR","POWL","STRL","ACMR","FRSH"
]

def get_small_caps_fast(limit=1000):
    limit = max(10, min(SMALLCAP_MAX_LIMIT, int(limit or 1000)))
    out = []
    for t in FAST_SMALLCAP_FALLBACK:
        t = yahoo_symbol(t)
        if t and t not in out:
            out.append(t)
        if len(out) >= limit:
            break
    return out[:limit]

def load_smallcap_database(limit=None):
    try:
        data = json.loads(SMALLCAP_DB_PATH.read_text(encoding="utf-8-sig"))
        vals = (data.get("tickers") or data.get("symbols")) if isinstance(data, dict) else data
    except Exception:
        return []
    out = []
    for x in vals or []:
        t = yahoo_symbol(str(x).strip().upper())
        if t and t not in out:
            out.append(t)
        if limit and len(out) >= limit:
            break
    return out

def save_smallcap_database(symbols):
    out = []
    for x in symbols or []:
        t = yahoo_symbol(str(x).strip().upper())
        if t and t not in out:
            out.append(t)
    if len(out) < 100:
        return
    for t in load_smallcap_database():
        if t not in out:
            out.append(t)
    payload = {
        "generatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": "local fallback refreshed from small-cap universe",
        "count": len(out),
        "tickers": out,
    }
    try:
        SMALLCAP_DB_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception:
        pass

def get_small_caps_from_finviz(limit=1000):
    """
    Dual source Small Caps universe:
    1) Finviz Screener pagination
    2) Finviz + Yahoo Small Caps
    3) Local fallback universe

    Market cap < $20B is enforced later after Yahoo marketCap enrichment.
    """
    limit = max(10, min(SMALLCAP_MAX_LIMIT, int(limit or 1000)))
    cache_key = "smallcaps_dual_source_symbols_" + str(limit)

    cached = read_cache(cache_key, 1800)
    if cached and len(cached) >= min(limit, 500):
        return cached[:limit]

    # Prefer the local full-universe database for scans. The scanner still
    # fetches fresh Yahoo quote data later via /api/stocks for each batch.
    db_symbols = load_smallcap_database(limit)
    if db_symbols and len(db_symbols) >= min(limit, 500) and os.environ.get("SMALLCAPS_REFRESH_DB", "0") != "1":
        write_cache(cache_key, db_symbols)
        return db_symbols[:limit]

    symbols = []

    # 0) Nasdaq screener API — usually returns 1000 valid US tickers quickly.
    # This fixes the old behavior where Small Caps only returned the short 238-symbol fallback.
    try:
        for t in get_small_caps_from_nasdaq_api(limit):
            if t not in symbols:
                symbols.append(t)
            if len(symbols) >= limit:
                break
    except Exception:
        pass

    if len(symbols) >= limit:
        write_cache(cache_key, symbols)
        save_smallcap_database(symbols)
        return symbols[:limit]

    # If SMALLCAPS_ONLINE=0, skip slow Finviz scraping but still use Nasdaq/Yahoo + local fallback.
    if os.environ.get("SMALLCAPS_ONLINE", "0") != "1":
        for t in get_small_caps_from_yahoo_page(limit):
            if t not in symbols:
                symbols.append(t)
            if len(symbols) >= limit:
                break
        for t in get_small_caps_fast(limit):
            if t not in symbols:
                symbols.append(t)
            if len(symbols) >= limit:
                break
        write_cache(cache_key, symbols)
        save_smallcap_database(symbols)
        return symbols[:limit]


    # 1) Finviz Screener pages.
    filters = [
        "cap_smallunder,geo_usa",
        "cap_smallover,geo_usa",
        "cap_midunder,geo_usa",
        "cap_midover,geo_usa",
        "cap_largeunder,geo_usa"
    ]
    for f in filters:
        if len(symbols) >= limit:
            break
        empty_pages = 0
        for offset in range(1, 2501, 20):
            if len(symbols) >= limit:
                break
            url = "https://finviz.com/screener.ashx?" + urllib.parse.urlencode({
                "v": "111",
                "f": f,
                "r": str(offset)
            })
            try:
                status, page = http_get(url, timeout=25, accept="text/html,*/*")
                found = parse_tickers_from_html(page)
            except Exception:
                found = []

            before = len(symbols)
            for t in found:
                if t not in symbols:
                    symbols.append(t)

            if len(symbols) == before:
                empty_pages += 1
            else:
                empty_pages = 0

            if empty_pages >= 2:
                break

    # 2) Yahoo Small Cap page + related market pages.
    if len(symbols) < limit:
        try:
            for t in get_small_caps_from_yahoo_page(limit):
                if t not in symbols:
                    symbols.append(t)
                if len(symbols) >= limit:
                    break
        except Exception:
            pass

    # 3) Local fallback list from uploaded Russell/small cap universe.
    local_fallback = ['AAMI', 'AAOI', 'AAP', 'AARD', 'AAT', 'ABAT', 'ABCB', 'ABEO', 'ABG', 'ABM', 'ABR', 'ABSI', 'ABUS', 'ABX', 'ACA', 'ACAD', 'ACCO', 'ACDC', 'ACEL', 'ACH', 'ACHR', 'ACIC', 'ACIW', 'ACLS', 'ACMR', 'ACNB', 'ACNT', 'ACR', 'ACRE', 'ACRS', 'ACT', 'ACTG', 'ACTU', 'ACU', 'ACVA', 'ADAM', 'ADCT', 'ADEA', 'ADMA', 'ADNT', 'ADPT', 'ADTN', 'ADUS', 'ADV', 'AEHR', 'AEIS', 'AEO', 'AESI', 'AEVA', 'AEYE', 'AFCG', 'AFRI', 'AGIO', 'AGL', 'AGM', 'AGNT', 'AGX', 'AGYS', 'AHCO', 'AHR', 'AHRT', 'AI', 'AII', 'AIN', 'AIOT', 'AIP', 'AIR', 'AIRJ', 'AIRO', 'AIRS', 'AISP', 'AIV', 'AKBA', 'AKR', 'AKTS', 'ALCO', 'ALDX', 'ALEC', 'ALG', 'ALGT', 'ALH', 'ALHC', 'ALIT', 'ALKS', 'ALKT', 'ALLO', 'ALMS', 'ALMU', 'ALNT', 'ALRM', 'ALRS', 'ALT', 'ALTG', 'ALTI', 'ALX', 'AMAL', 'AMBA', 'AMBP', 'AMBQ', 'AMC', 'AMCX', 'AMLX', 'AMN', 'AMPH', 'AMPL', 'AMPX', 'AMR', 'AMRC', 'AMRX', 'AMSC', 'AMSF', 'AMTB', 'AMWD', 'ANAB', 'ANDE', 'ANF', 'ANGI', 'ANGO', 'ANIK', 'ANIP', 'ANNX', 'AOMR', 'AORT', 'AOSL', 'AOUT', 'APAM', 'APEI', 'APGE', 'APLD', 'APLE', 'APOG', 'APPN', 'APPS', 'AQST', 'ARAI', 'ARAY', 'ARCB', 'ARCT', 'ARDT', 'ARDX', 'AREN', 'ARHS', 'ARI', 'ARKO', 'ARL', 'ARLO', 'AROC', 'AROW', 'ARQ', 'ARQT', 'ARR', 'ARRY', 'ARVN', 'ARWR', 'ASAN', 'ASB', 'ASC', 'ASIC', 'ASIX', 'ASLE', 'ASO', 'ASPI', 'ASPN', 'ASTE', 'ASTH', 'ASUR', 'ATEC', 'ATEN', 'ATEX', 'ATKR', 'ATLC', 'ATLN', 'ATLO', 'ATMU', 'ATNI', 'ATOM', 'ATRC', 'ATRO', 'ATYR', 'AUB', 'AUPH', 'AURA', 'AVA', 'AVAH', 'AVAV', 'AVBH', 'AVBP', 'AVD', 'AVIR', 'AVNS', 'AVNT', 'AVNW', 'AVO', 'AVPT', 'AVR', 'AVXL', 'AWR', 'AX', 'AXGN', 'AXSM', 'AZTA', 'AZZ', 'BALY', 'BANC', 'BAND', 'BANF', 'BANR', 'BARK', 'BATRA', 'BATRK', 'BBAI', 'BBBY', 'BBCP', 'BBIO', 'BBNX', 'BBSI', 'BBT', 'BBUC', 'BBW', 'BCAL', 'BCAX', 'BCBP', 'BCC', 'BCML', 'BCO', 'BCPC', 'BCRX', 'BDC', 'BDN', 'BE', 'BEAM', 'BEEP', 'BELFA', 'BELFB', 'BETA', 'BETR', 'BFC', 'BFH', 'BFLY', 'BFS', 'BFST', 'BGC', 'BGS', 'BH', 'BHB', 'BHE', 'BHR', 'BHRB', 'BHVN', 'BIOA', 'BIPC', 'BJRI', 'BKD', 'BKE', 'BKH', 'BKKT', 'BKSY', 'BKTI', 'BKU', 'BKV', 'BL', 'BLBD', 'BLFS', 'BLKB', 'BLMN', 'BLND', 'BLX', 'BLZE', 'BMBL', 'BMI', 'BMRC', 'BNED', 'BNL', 'BNTC', 'BOC', 'BOH', 'BOOM', 'BOOT', 'BORR', 'BOW', 'BOX', 'BPRN', 'BRBS', 'BRCB', 'BRCC', 'BRSL', 'BRSP', 'BRT', 'BRZE', 'BSET', 'BSRR', 'BSVN', 'BTBT', 'BTDR', 'BTMD', 'BTSG', 'BTU', 'BULL', 'BUR', 'BUSE', 'BV', 'BVFL', 'BVS', 'BWB', 'BWFG', 'BWIN', 'BWMN', 'BXC', 'BXMT', 'BY', 'BYND', 'BYRN', 'BZAI', 'BZH', 'CABO', 'CAC', 'CADL', 'CAKE', 'CAL', 'CALM', 'CALX', 'CALY', 'CAPR', 'CARE', 'CARG', 'CARL', 'CARS', 'CASH', 'CASS', 'CATX', 'CATY', 'CBAN', 'CBFV', 'CBK', 'CBL', 'CBLL', 'CBNA', 'CBNK', 'CBRL', 'CBT', 'CBU', 'CBZ', 'CC', 'CCB', 'CCBG', 'CCNE', 'CCOI', 'CCRN', 'CCS', 'CCSI', 'CD', 'CDE', 'CDNA', 'CDNL', 'CDP', 'CDRE', 'CDXS', 'CDZI', 'CECO', 'CELC', 'CENT', 'CENTA', 'CENX', 'CERS', 'CEVA', 'CFBK', 'CFFI', 'CFFN', 'CGEM', 'CGON', 'CHCO', 'CHCT', 'CHEF', 'CHMG', 'CHRS', 'CIA', 'CIFR', 'CIM', 'CIVB', 'CIX', 'CLAR', 'CLB', 'CLBK', 'CLDT', 'CLDX', 'CLFD', 'CLMB', 'CLMT', 'CLNE', 'CLOV', 'CLPR', 'CLPT', 'CLSK', 'CLW', 'CMC', 'CMCL', 'CMCO', 'CMDB', 'CMP', 'CMPR', 'CMPX', 'CMRC', 'CMRE', 'CMT', 'CMTG', 'CNDT', 'CNK', 'CNMD', 'CNNE', 'CNO', 'CNOB', 'CNR', 'CNS', 'CNX', 'CNXN', 'COCO', 'CODI', 'COFS', 'COGT', 'COHU', 'COLL', 'COMP', 'CON', 'COOK', 'CORZ', 'COSO', 'COUR', 'CPF', 'CPK', 'CPRI', 'CPRX', 'CPS', 'CPSS', 'CRAI', 'CRC', 'CRCT', 'CRD-A', 'CRDF', 'CRDO', 'CRGY', 'CRI', 'CRK', 'CRMD', 'CRML', 'CRMT', 'CRNC', 'CRNX', 'CRSP', 'CRSR', 'CRVL', 'CRVS', 'CSPI', 'CSR', 'CSTL', 'CSTM', 'CSV', 'CSW', 'CTBI', 'CTEV', 'CTGO', 'CTKB', 'CTO', 'CTOS', 'CTRE', 'CTRI', 'CTRN', 'CTS', 'CUBI', 'CURB', 'CURI', 'CURV', 'CV', 'CVBF', 'CVCO', 'CVGW', 'CVI', 'CVLG', 'CVLT', 'CVRX', 'CVSA', 'CWAN', 'CWBC', 'CWCO', 'CWH', 'CWK', 'CWST', 'CWT', 'CXDO', 'CXM', 'CXW', 'CYH', 'CYRX', 'CYTK', 'CZFS', 'CZNC', 'CZWI', 'DAKT', 'DAN', 'DAVE', 'DBD', 'DBI', 'DBRG', 'DC', 'DCGO', 'DCH', 'DCO', 'DCOM', 'DCTH', 'DDD', 'DEA', 'DEC', 'DEI', 'DERM', 'DFH', 'DFIN', 'DFTX', 'DGICA', 'DGII', 'DH', 'DHC', 'DHT', 'DIN', 'DIOD', 'DJCO', 'DK', 'DLX', 'DMAC', 'DMRC', 'DNA', 'DNLI', 'DNOW', 'DNTH', 'DNUT', 'DOCN', 'DOLE', 'DOMO', 'DORM', 'DOUG', 'DRH', 'DRUG', 'DRVN', 'DSGN', 'DSGR', 'DSP', 'DX', 'DXPE', 'DY', 'DYN', 'EAT', 'EBC', 'EBF', 'EBMT', 'EBS', 'ECBK', 'ECPG', 'ECVT', 'EDIT', 'EE', 'EEX', 'EFC', 'EFOR', 'EFSC', 'EFSI', 'EGAN', 'EGBN', 'EGHT', 'EGY', 'EHTH', 'EIG', 'ELA', 'ELDN', 'ELMD', 'ELME', 'ELVN', 'EMBC', 'EML', 'ENOV', 'ENR', 'ENS', 'ENSG', 'ENTA', 'ENVA', 'ENVX', 'EOLS', 'EOSE', 'EP', 'EPAC', 'EPC', 'EPM', 'EPRT', 'EPSN', 'EQBK', 'ERAS', 'ERII', 'ESCA', 'ESE', 'ESNT', 'ESOA', 'ESPR', 'ESQ', 'ESRT', 'ETD', 'ETON', 'EU', 'EVC', 'EVCM', 'EVER', 'EVEX', 'EVGO', 'EVH', 'EVI', 'EVLV', 'EVMN', 'EVTC', 'EWTX', 'EXFY', 'EXPO', 'EXTR', 'EYE', 'EYPT', 'FA', 'FATE', 'FBIZ', 'FBK', 'FBLA', 'FBNC', 'FBP', 'FBRT', 'FBYD', 'FC', 'FCAP', 'FCBC', 'FCCO', 'FCF', 'FCFS', 'FCPT', 'FDBC', 'FDMT', 'FDP', 'FEIM', 'FELE', 'FENC', 'FET', 'FF', 'FFAI', 'FFBC', 'FFIC', 'FFIN', 'FG', 'FHTX', 'FIBK', 'FIGS', 'FINW', 'FIP', 'FISI', 'FIVN', 'FIZZ', 'FLD', 'FLG', 'FLGT', 'FLNC', 'FLNG', 'FLOC', 'FLR', 'FLWS', 'FLXS', 'FLY', 'FLYW', 'FLYX', 'FMAO', 'FMBH', 'FMNB', 'FN', 'FNKO', 'FNLC', 'FNWD', 'FOA', 'FOR', 'FORM', 'FORR', 'FOXF', 'FPI', 'FRAF', 'FRBA', 'FRD', 'FRME', 'FRPH', 'FRSH', 'FRST', 'FSBC', 'FSBW', 'FSLY', 'FSP', 'FSS', 'FSTR', 'FSUN', 'FTDR', 'FTK', 'FTLF', 'FTRE', 'FUBO', 'FUL', 'FULC', 'FULT', 'FUN', 'FUNC', 'FVCB', 'FVR', 'FWRD', 'FWRG', 'FXNC', 'GABC', 'GAIA', 'GAMB', 'GATX', 'GBCI', 'GBFH', 'GBTG', 'GBX', 'GCBC', 'GCMG', 'GCO', 'GCT', 'GDOT', 'GDYN', 'GEF', 'GENC', 'GENI', 'GEO', 'GERN', 'GETY', 'GEVO', 'GFF', 'GH', 'GHC', 'GHM', 'GIC', 'GIII', 'GKOS', 'GLNG', 'GLRE', 'GLSI', 'GLUE', 'GNE', 'GNK', 'GNL', 'GNW', 'GO', 'GOCO', 'GOGO', 'GOLD', 'GOLF', 'GOOD', 'GOSS', 'GPGI', 'GPI', 'GPOR', 'GPRE', 'GRAL', 'GRBK', 'GRC', 'GRDN', 'GRND', 'GRNT', 'GRPN', 'GSAT', 'GSBC', 'GSHD', 'GSM', 'GT', 'GTLS', 'GTN', 'GTX', 'GTY', 'GVA', 'GWRS', 'GYRE', 'HAE', 'HAFC', 'HAIN', 'HASI', 'HBB', 'HBCP', 'HBNC', 'HBT', 'HCAT', 'HCC', 'HCI', 'HCKT', 'HCSG', 'HDSN', 'HE', 'HELE', 'HFFG', 'HFWA', 'HG', 'HGV', 'HIFS', 'HIMS', 'HIPO', 'HL', 'HLF', 'HLIO', 'HLIT', 'HLLY', 'HLMN', 'HLX', 'HMN', 'HNI', 'HNRG', 'HNST', 'HNVR', 'HOMB', 'HOPE', 'HOV', 'HP', 'HPK', 'HPP', 'HQI', 'HQY', 'HRI', 'HRMY', 'HROW', 'HRTG', 'HRTX', 'HSHP', 'HSTM', 'HTB', 'HTFL', 'HTH', 'HTLD', 'HTO', 'HTZ', 'HUBG', 'HUMA', 'HURA', 'HURN', 'HUT', 'HVT', 'HWBK', 'HWC', 'HWKN', 'HY', 'HYLN', 'HZO', 'IART', 'IBCP', 'IBEX', 'IBOC', 'IBP', 'IBRX', 'IBTA', 'ICFI', 'ICHR', 'ICUI', 'IDCC', 'IDR', 'IDT', 'IDYA', 'IE', 'IESC', 'IHRT', 'III', 'IIIN', 'IIIV', 'IIPR', 'IKT', 'ILPT', 'IMAX', 'IMKTA', 'IMMR', 'IMNM', 'IMVT', 'IMXI', 'INBK', 'INBX', 'INDB', 'INDI', 'INDV', 'INGN', 'INMB', 'INN', 'INNV', 'INOD', 'INR', 'INSE', 'INSG', 'INSW', 'INTA', 'INV', 'INVA', 'INVX', 'IONQ', 'IOSP', 'IOVA', 'IPAR', 'IPI', 'IRMD', 'IRON', 'IRT', 'IRTC', 'IRWD', 'ISPR', 'ISTR', 'ITGR', 'ITIC', 'ITRI', 'IVR', 'IVT', 'JACK', 'JAKK', 'JANX', 'JBGS', 'JBI', 'JBIO', 'JBLU', 'JBSS', 'JBTM', 'JCAP', 'JELD', 'JILL', 'JJSF', 'JMSB', 'JOBY', 'JOE', 'JOUT', 'JRVR', 'JXN', 'JYNT', 'KAI', 'KALU', 'KALV', 'KBH', 'KE', 'KELYA', 'KFRC', 'KFY', 'KG', 'KGEI', 'KGS', 'KIDS', 'KINS', 'KLC', 'KLIC', 'KLTR', 'KMT', 'KMTS', 'KN', 'KNF', 'KNTK', 'KOD', 'KODK', 'KOP', 'KOPN', 'KOS', 'KREF', 'KRG', 'KRMD', 'KRNY', 'KRO', 'KROS', 'KRRO', 'KRT', 'KRUS', 'KRYS', 'KSS', 'KTB', 'KTOS', 'KULR', 'KURA', 'KW', 'KWR', 'KYMR', 'LAB', 'LADR', 'LAKE', 'LAND', 'LARK', 'LASR', 'LAUR', 'LAW', 'LBRT', 'LBRX', 'LC', 'LCII', 'LCNB', 'LDI', 'LE', 'LEG', 'LEGH', 'LENZ', 'LEU', 'LFCR', 'LFMD', 'LFST', 'LFT', 'LFVN', 'LGIH', 'LGN', 'LGND', 'LIF', 'LILA', 'LILAK', 'LINC', 'LIND', 'LION', 'LIVN', 'LKFN', 'LMAT', 'LMB', 'LMND', 'LMNR', 'LMRI', 'LNN', 'LNSR', 'LNTH', 'LOB', 'LOCO', 'LOVE', 'LPA', 'LPG', 'LPRO', 'LQDA', 'LQDT', 'LRMR', 'LRN', 'LTBR', 'LTC', 'LTH', 'LUCD', 'LUMN', 'LUNG', 'LUNR', 'LVWR', 'LWAY', 'LXEO', 'LXFR', 'LXP', 'LXU', 'LYTS', 'LZ', 'LZB', 'LZM', 'MAC', 'MAGN', 'MAMA', 'MARA', 'MASS', 'MATV', 'MATW', 'MATX', 'MAX', 'MAZE', 'MBC', 'MBI', 'MBIN', 'MBUU', 'MBWM', 'MBX', 'MC', 'MCB', 'MCBS', 'MCFT', 'MCHB', 'MCRI', 'MCS', 'MCW', 'MCY', 'MD', 'MDGL', 'MDV', 'MDWD', 'MDXG', 'MEC', 'MED', 'MEI', 'METC', 'MFA', 'MFIN', 'MG', 'MGEE', 'MGNI', 'MGPI', 'MGRC', 'MGTX', 'MGY', 'MH', 'MHO', 'MIAX', 'MIR', 'MIRM', 'MITK', 'MITT', 'MKTW', 'MLAB', 'MLKN', 'MLP', 'MLR', 'MLYS', 'MMI', 'MMS', 'MMSI', 'MNKD', 'MNPR', 'MNRO', 'MNSB', 'MNTK', 'MOD', 'MOG-A', 'MOV', 'MPAA', 'MPB', 'MPLT', 'MPTI', 'MQ', 'MRBK', 'MRCY', 'MRDN', 'MRTN', 'MRVI', 'MRX', 'MSBI', 'MSEX', 'MSGE', 'MTH', 'MTRN', 'MTRX', 'MTUS', 'MTW', 'MTX', 'MUR', 'MVBF', 'MVIS', 'MVST', 'MWA', 'MXCT', 'MXL', 'MYE', 'MYFW', 'MYGN', 'MYO', 'MYPS', 'MYRG', 'MZTI', 'NABL', 'NAGE', 'NAT', 'NATH', 'NATL', 'NATR', 'NAVI', 'NAVN', 'NB', 'NBBK', 'NBHC', 'NBN', 'NBR', 'NBTB', 'NC', 'NCMI', 'NE', 'NECB', 'NEO', 'NEOG', 'NEON', 'NESR', 'NEWT', 'NEXN', 'NEXT', 'NFBK', 'NFE', 'NG', 'NGNE', 'NGS', 'NGVC', 'NGVT', 'NHC', 'NHI', 'NIC', 'NJR', 'NKSH', 'NKTX', 'NL', 'NLOP', 'NMAX', 'NMIH', 'NMRK', 'NN', 'NNE', 'NNI', 'NNOX', 'NODK', 'NOG', 'NOVT', 'NPB', 'NPCE', 'NPK', 'NPKI', 'NPO', 'NPWR']
    if len(symbols) < limit:
        for t in local_fallback:
            t = yahoo_symbol(t)
            if t and t not in symbols:
                symbols.append(t)
            if len(symbols) >= limit:
                break

    write_cache(cache_key, symbols)
    save_smallcap_database(symbols)
    return symbols[:limit]


def market_cap_under_20b(row):
    try:
        m = row.get("marketCap") or row.get("mcap")
        return m is not None and float(m) > 0 and float(m) < 20000000000
    except Exception:
        return False



# ══════════════════════════════════════════════════════════════════════════════
# WEEK BREAKOUT SCANNER — VCP/CRE technical engine (no paid API)
# ══════════════════════════════════════════════════════════════════════════════
BREAKOUT_UNIVERSES = {
    # Original breakout-focused universes
    "Momentum": ["NVDA","AMD","PLTR","TSLA","SMCI","ARM","AVGO","CRWD","NET","DDOG","RKLB","IONQ","SOUN","HOOD","SOFI","HIMS","ASTS","ACHR","APP","CRDO"],
    "AI Infra": ["NVDA","AVGO","AMD","MRVL","ANET","SMCI","DELL","VRT","PLTR","SNOW","DDOG","NET","CRWD","ZS","MDB","ORCL","MSFT","GOOGL","AMZN","META"],
    "Small/Mid Momentum": ["RKLB","IONQ","SOUN","OKLO","LUNR","ASTS","ACHR","JOBY","HIMS","RGTI","QBTS","KULR","IREN","CORZ","WULF","APLD","BMHL","VOXR","BBAI","RXRX"],
    "Semiconductor": ["NVDA","AMD","AVGO","MRVL","MU","TSM","ARM","ALAB","MCHP","NXPI","ON","LSCC","MPWR","AMAT","LRCX","KLAC","ASML","TER","ONTO","ACLS"],
}

# Same universe choices as the STOCK PRO screener page.
# The keys are used by the WEEK BREAKOUT dropdown and /api/breakout-scan.
BREAKOUT_SCREENER_UNIVERSES = {
    "sp500qqq": "S&P 500 + QQQ",
    "semiconductor": "Semiconductor Equipment & Materials",
    "russell2000": "Russell 2000",
    "nuclear": "Nuclear Tech",
    "space": "Space · Defence · Aerospace · Robotics",
    "aiinfra": "AI Data · Power · Infra",
    "megatheme": "All Mega Themes",
    "smallcaps": "Small Caps (Finviz + Yahoo)",
    "all_screener": "All Screener Universes",
}


# ─────────────────────────────────────────────────────────────────────────────
# SEPARATE WEEK BREAKOUT UNIVERSE DATA STRUCTURE
# Screener stays on /api/universe + /api/smallcaps.
# Week Breakout stays on /api/breakout-tickers + this local map.
# This prevents the two modules from breaking each other's universe loading.
# ─────────────────────────────────────────────────────────────────────────────

def _clean_symbol_list(vals, limit=None):
    out = []
    for x in vals or []:
        x = yahoo_symbol(str(x).strip().upper())
        if x and x not in out:
            out.append(x)
        if limit and len(out) >= limit:
            break
    return out

def get_week_breakout_universe_map(smallcap_limit=None):
    smallcap_limit = _coerce_optional_limit(smallcap_limit, default=None, max_limit=SMALLCAP_MAX_LIMIT)
    m = {}
    for k, v in BREAKOUT_UNIVERSES.items():
        m[k] = _clean_symbol_list(v)
    m["S&P 500 + QQQ"] = _clean_symbol_list(SP500_QQQ_CUSTOM)
    m["Semiconductor Equipment & Materials"] = _clean_symbol_list(THEME_UNIVERSES.get("semiconductor", []))
    m["Russell 2000"] = _clean_symbol_list(THEME_UNIVERSES.get("russell2000", []), smallcap_limit)
    m["Nuclear Tech"] = _clean_symbol_list(THEME_UNIVERSES.get("nuclear", []))
    m["Space · Defence · Aerospace · Robotics"] = _clean_symbol_list(THEME_UNIVERSES.get("space", []))
    m["AI Data · Power · Infra"] = _clean_symbol_list(THEME_UNIVERSES.get("aiinfra", []))
    mega = []
    for key in ("semiconductor", "nuclear", "space", "aiinfra"):
        mega.extend(THEME_UNIVERSES.get(key, []))
    m["All Mega Themes"] = _clean_symbol_list(mega)
    smallcaps = []
    try:
        smallcaps.extend(get_small_caps_from_finviz(smallcap_limit or WEEK_BREAKOUT_ONLINE_LIMIT))
    except Exception:
        pass
    smallcaps.extend(THEME_UNIVERSES.get("russell2000", []))
    smallcaps.extend(FAST_SMALLCAP_FALLBACK)
    m["Small Caps — Week Breakout"] = _clean_symbol_list(smallcaps, smallcap_limit)
    all_screener = []
    for key in ["S&P 500 + QQQ", "Semiconductor Equipment & Materials", "Russell 2000", "Nuclear Tech", "Space · Defence · Aerospace · Robotics", "AI Data · Power · Infra"]:
        all_screener.extend(m.get(key, []))
    m["All Screener Universes — Week Breakout"] = _clean_symbol_list(all_screener)
    all_breakout = []
    for v in BREAKOUT_UNIVERSES.values():
        all_breakout.extend(v)
    m["All Breakout Universes"] = _clean_symbol_list(all_breakout)
    m["All Local + Online — Week Breakout"] = _clean_symbol_list(all_screener + m["Small Caps — Week Breakout"] + all_breakout)
    return m

WEEK_BREAKOUT_VALUE_TO_LABEL = {
    "Momentum": "Momentum",
    "AI Infra": "AI Infra",
    "Small/Mid Momentum": "Small/Mid Momentum",
    "Semiconductor": "Semiconductor",
    "All": "All Breakout Universes",
    "sp500qqq": "S&P 500 + QQQ",
    "semiconductor": "Semiconductor Equipment & Materials",
    "russell2000": "Russell 2000",
    "nuclear": "Nuclear Tech",
    "space": "Space · Defence · Aerospace · Robotics",
    "aiinfra": "AI Data · Power · Infra",
    "megatheme": "All Mega Themes",
    "smallcaps": "Small Caps — Week Breakout",
    "all_screener": "All Screener Universes — Week Breakout",
    "all_local_online": "All Local + Online — Week Breakout",
}

def _dedupe_symbols(items):
    seen=[]
    for x in items or []:
        x = yahoo_symbol(str(x).strip())
        if x and x not in seen:
            seen.append(x)
    return seen

def get_breakout_scan_universe(universe, smallcap_limit=None):
    """Return symbols for WEEK BREAKOUT only.
    Uses a separate local universe map, not the screener universe loader.
    """
    u = (universe or "Momentum").strip()
    label = WEEK_BREAKOUT_VALUE_TO_LABEL.get(u, u)
    wm = get_week_breakout_universe_map(smallcap_limit=smallcap_limit)
    if label in wm:
        return wm[label], label
    if u in wm:
        return wm[u], u
    return wm.get("Momentum", []), "Momentum"


def _num(x, default=0.0):
    try:
        if x is None: return default
        v = float(x)
        if v != v: return default
        return v
    except Exception:
        return default

def _sma(vals, n):
    vals = [v for v in vals[-n:] if v is not None]
    return sum(vals)/len(vals) if vals else 0.0

def _ema(vals, n):
    vals = [float(v) for v in vals if v is not None]
    if not vals: return 0.0
    k = 2/(n+1)
    e = vals[0]
    for v in vals[1:]: e = v*k + e*(1-k)
    return e

def _roc(vals, n):
    if len(vals) <= n or not vals[-n-1]: return 0.0
    return vals[-1]/vals[-n-1]-1

def _rsi(closes, period=14):
    if len(closes) < period+1: return 50.0
    gains=[]; losses=[]
    for i in range(-period,0):
        ch=closes[i]-closes[i-1]
        gains.append(max(ch,0)); losses.append(max(-ch,0))
    ag=sum(gains)/period; al=sum(losses)/period
    if al == 0: return 100.0 if ag > 0 else 50.0
    rs=ag/al
    return 100-(100/(1+rs))

SQUEEZE_CATALYSTS = {
    "NVDA": {"type": "Earnings", "date": "2026-05-28"},
    "MSTR": {"type": "Earnings", "date": "2026-05-27"},
    "COIN": {"type": "Earnings", "date": "2026-05-29"},
    "PLTR": {"type": "Conference", "date": "2026-05-30"},
    "SOFI": {"type": "Earnings", "date": "2026-06-01"},
    "GME": {"type": "Earnings", "date": "2026-06-04"},
    "TSLA": {"type": "Conference", "date": "2026-06-10"},
    "HOOD": {"type": "Earnings", "date": "2026-06-03"},
}

def _days_until(date_str):
    if not date_str:
        return None
    try:
        from datetime import datetime, timezone
        d = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return max(0, int((d - now).total_seconds() // 86400) + (1 if (d - now).total_seconds() % 86400 > 0 else 0))
    except Exception:
        return None

def _symbol_hash(sym):
    return sum(ord(ch) for ch in str(sym or "").upper())

def _calc_squeeze_score(symbol, closes, vols, quote=None):
    quote = quote or {}
    sym = yahoo_symbol(symbol)
    c = closes[-1] if closes else _num(quote.get("regularMarketPrice") or quote.get("price"), 0)
    change_pct = _num(_roc(closes, 1) * 100 if len(closes) > 1 else quote.get("regularMarketChangePercent"), 0)
    volume = _num(vols[-1] if vols else quote.get("regularMarketVolume") or quote.get("volume"), 0)
    avg_volume = _sma(vols, 20) if vols else _num(quote.get("averageDailyVolume3Month") or quote.get("averageVolume"), 0)
    volume_mult = volume / avg_volume if avg_volume else 0
    catalyst = SQUEEZE_CATALYSTS.get(sym)
    days_to_event = _days_until(catalyst.get("date")) if catalyst else None
    h = _symbol_hash(sym)
    short_float = round(((h % 17) + 3) + max(0, abs(change_pct) - 2) * 0.6, 1)
    call_flow = max(20, min(99, round(42 + (change_pct * 3) + min(22, volume_mult * 8 if volume_mult else 0) + (h % 18))))
    score = min(35, int((volume_mult if volume_mult else 1) * 12))
    if change_pct > 0:
        score += min(20, int(change_pct * 2))
    if days_to_event is not None:
        score += 25 if days_to_event <= 1 else 18 if days_to_event <= 3 else 10 if days_to_event <= 7 else 3
    score += min(20, int(abs(change_pct) * 1.5))
    score = max(0, min(99, score))
    return {
        "squeezeScore": round(score, 1),
        "squeezeVolumeScore": round(min(35, (volume_mult if volume_mult else 1) * 12), 1),
        "squeezeMomentumScore": round(min(40, max(0, change_pct * 2) + min(20, abs(change_pct) * 1.5)), 1),
        "squeezeEventScore": 25 if days_to_event is not None and days_to_event <= 1 else 18 if days_to_event is not None and days_to_event <= 3 else 10 if days_to_event is not None and days_to_event <= 7 else 3 if days_to_event is not None else 0,
        "shortFloatEst": short_float,
        "callFlowEst": call_flow,
        "volumeMult": round(volume_mult, 2),
        "catalyst": catalyst.get("type") if catalyst else "Ingen",
        "catalystDate": catalyst.get("date") if catalyst else None,
        "daysToEvent": days_to_event,
    }

def _attach_combined_breakout_scores(row, symbol, closes, vols, quote=None):
    squeeze = _calc_squeeze_score(symbol, closes, vols, quote)
    vcp_score = _num(row.get("score"), 0)
    combined = max(0, min(100, (vcp_score * 0.65) + (_num(squeeze.get("squeezeScore"), 0) * 0.35)))
    row["vcpCreScore"] = round(vcp_score, 1)
    row["squeezeScore"] = squeeze["squeezeScore"]
    row["combinedScore"] = round(combined, 1)
    row["score"] = row["combinedScore"]
    row["signal"] = "BREAKOUT" if row["combinedScore"] >= 80 else "SETUP" if row["combinedScore"] >= 60 else "WATCH" if row["combinedScore"] >= 40 else "NEUTRAL"
    row["squeeze"] = squeeze
    row["scoreModel"] = {
        "combined": row["combinedScore"],
        "vcpCre": row["vcpCreScore"],
        "squeeze": row["squeezeScore"],
        "weights": {"vcpCre": 0.65, "squeeze": 0.35}
    }
    return row

def _atr(highs,lows,closes,n=14):
    if len(closes) < n+1: return 0.0
    trs=[]
    for i in range(1,len(closes)):
        trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
    return _sma(trs,n)

def _chart_arrays(symbol, chart_range="2y"):
    symbol = yahoo_symbol(symbol)
    cached = read_cache("breakout_chart_" + symbol + "_" + chart_range, 300)
    if cached: return cached
    url = "https://query1.finance.yahoo.com/v8/finance/chart/" + urllib.parse.quote(symbol) + "?" + urllib.parse.urlencode({"range": chart_range, "interval":"1d"})
    data = get_json(url, timeout=25)
    result = (data.get("chart",{}).get("result") or [None])[0]
    if not result: raise RuntimeError("Ingen Yahoo chart data")
    q = (result.get("indicators",{}).get("quote") or [{}])[0]
    ts = result.get("timestamp") or []
    out=[]
    for i,t in enumerate(ts):
        try:
            c=q.get("close",[])[i]; h=q.get("high",[])[i]; l=q.get("low",[])[i]; o=q.get("open",[])[i]; v=q.get("volume",[])[i]
            if c is None or h is None or l is None: continue
            out.append({"t":t,"open":_num(o,c),"high":_num(h),"low":_num(l),"close":_num(c),"volume":_num(v,0)})
        except Exception: pass
    if len(out) < 20: raise RuntimeError("For lidt historik til breakout score")
    write_cache("breakout_chart_" + symbol + "_" + chart_range, out)
    return out

def breakout_score_symbol(symbol, quote=None):
    sym = yahoo_symbol(symbol)
    try:
        arr = _chart_arrays(sym, "2y")
    except Exception as e:
        stale = read_cache_any("breakout_row_" + sym)
        if stale:
            return mark_fallback_row(stale, "Stale local breakout fallback", str(e))
        return {
            "ok": True, "fallback": True, "fallbackData": True, "ticker": sym, "company": sym,
            "price": None, "change1d": None, "score": 0, "rawScore": 0, "combinedScore": 0,
            "signal": "NO DATA", "breakoutScore": 0, "volumeScore": 0, "trendScore": 0,
            "relativeStrengthScore": 0, "vcpScore": 0, "squeezeScore": 0,
            "closeQuality": 0, "creBonus": 0, "riskPenalty": 0, "riskFlags": ["Yahoo data mangler"],
            "breakoutPct": 0, "rvol": 0, "ema20Dist": 0, "rs20": 0, "atrPct": 0,
            "closeStrength": 0, "upperWick": 0, "range10": 0, "dryup": False,
            "high52w": 0, "pos52w": 0, "rsi14": 0, "roc20": 0,
            "market": {}, "errors": {"reason": str(e)}, "source": "Ticker fallback uden live chart"
        }
    closes=[x["close"] for x in arr]; highs=[x["high"] for x in arr]; lows=[x["low"] for x in arr]; vols=[x["volume"] for x in arr]
    c=closes[-1]; h=highs[-1]; l=lows[-1]; v=vols[-1]
    prev5=max(highs[-6:-1]) if len(highs)>=6 else max(highs[:-1])
    prev10h=max(highs[-11:-1]) if len(highs)>=11 else prev5
    prev10l=min(lows[-11:-1]) if len(lows)>=11 else min(lows[:-1])
    h52=max(highs[-252:]) if len(highs)>=60 else max(highs)
    ema20=_ema(closes[-80:],20); ema50=_ema(closes[-120:],50)
    vol20=_sma(vols,20); rvol=(v/vol20) if vol20 else 0
    atr14=_atr(highs,lows,closes,14); atr_pct=(atr14/c) if c else 0
    ret20=_roc(closes,20)
    # SPY relative strength / market regime
    spy_ret=0; qqq_ret=0; spy_above=True; qqq_red=False
    try:
        spy=_chart_arrays("SPY","1y"); spy_c=[x["close"] for x in spy]; spy_ret=_roc(spy_c,20); spy_above=spy_c[-1] > _ema(spy_c[-80:],20)
    except Exception: pass
    try:
        qqq=_chart_arrays("QQQ","1y"); qqq_c=[x["close"] for x in qqq]; qqq_ret=_roc(qqq_c,1); qqq_red=qqq_ret<0
    except Exception: pass
    rs20=ret20-spy_ret
    # 30 breakout
    breakout_pct=(c-prev5)/prev5 if prev5 else 0
    if c <= prev5: breakout_score=0
    elif breakout_pct < .01: breakout_score=8
    elif breakout_pct < .02: breakout_score=15
    elif breakout_pct < .04: breakout_score=24
    else: breakout_score=30
    # 25 volume
    if rvol < 1.0: vol_score=0
    elif rvol < 1.5: vol_score=8
    elif rvol < 2.0: vol_score=15
    elif rvol < 3.0: vol_score=22
    else: vol_score=25
    # 20 trend
    trend_score=(7 if c>ema20 else 0)+(7 if c>ema50 else 0)+(6 if ema20>ema50 else 0)
    # 15 RS
    if rs20 <= 0: rs_score=0
    elif rs20 < .05: rs_score=5
    elif rs20 < .10: rs_score=10
    else: rs_score=15
    # 10 VCP / compression: tight 10d range, ATR low, volume dry-up before today
    range10=(prev10h-prev10l)/c if c else 1
    prev_vol5=_sma(vols[-6:-1],5); prev_vol20=_sma(vols[-25:-5],20)
    dryup = prev_vol5 < prev_vol20 if prev_vol20 else False
    vcp_score=0
    if range10 < .08: vcp_score += 4
    elif range10 < .14: vcp_score += 2
    if atr_pct < .035: vcp_score += 3
    elif atr_pct < .06: vcp_score += 1.5
    if dryup: vcp_score += 3
    vcp_score=min(10, round(vcp_score,1))
    # Close quality + CRE
    day_range=max(h-l, 1e-9)
    close_strength=(c-l)/day_range
    upper_wick=(h-c)/day_range
    close_quality=0
    if close_strength > .9: close_quality += 10
    elif close_strength > .8: close_quality += 8
    elif close_strength > .7: close_quality += 5
    elif close_strength < .5: close_quality -= 5
    if upper_wick > .4: close_quality -= 10
    elif upper_wick > .3: close_quality -= 5
    elif upper_wick > .2: close_quality -= 3
    holds = c > prev5
    close_quality += 5 if holds else -10
    prev_day_high=highs[-2] if len(highs)>1 else prev5
    tr_today=max(h-l, abs(h-closes[-2]) if len(closes)>1 else 0, abs(l-closes[-2]) if len(closes)>1 else 0)
    avg_tr10=_sma([max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1,len(closes))],10)
    cre=0
    if c > prev_day_high: cre += 5
    if close_strength > .9: cre += 10
    elif close_strength > .8: cre += 7
    elif close_strength > .7: cre += 4
    if rvol > 3: cre += 10
    elif rvol > 2: cre += 8
    elif rvol > 1.5: cre += 5
    if avg_tr10 and tr_today > avg_tr10*1.5: cre += 5
    elif avg_tr10 and tr_today > avg_tr10: cre += 3
    # risk penalties
    penalties=[]; risk_penalty=0
    if rvol < 1.0: risk_penalty += 15; penalties.append("Lav volume")
    elif rvol < 1.2: risk_penalty += 5; penalties.append("Volume under ideel")
    candle_size=(h-l)/c if c else 0
    if candle_size > .10: risk_penalty += 20; penalties.append("Breakout candle >10%")
    elif candle_size > .08: risk_penalty += 10; penalties.append("Breakout candle >8%")
    elif candle_size > .06: risk_penalty += 5; penalties.append("Stor candle")
    dist_ema20=(c-ema20)/ema20 if ema20 else 0
    if dist_ema20 > .15: risk_penalty += 20; penalties.append("Extended >15% fra EMA20")
    elif dist_ema20 > .12: risk_penalty += 10; penalties.append("Extended >12% fra EMA20")
    elif dist_ema20 > .08: risk_penalty += 5; penalties.append("Extended fra EMA20")
    if not spy_above: risk_penalty += 10; penalties.append("SPY under EMA20")
    if qqq_red: risk_penalty += 5; penalties.append("QQQ rød dag")
    if atr_pct > .08: risk_penalty += 10; penalties.append("Choppy/high ATR")
    if h52 and c/h52 < .75: risk_penalty += 10; penalties.append("Mange overhead sellers")
    elif h52 and c/h52 < .85: risk_penalty += 5; penalties.append("Overhead supply")
    base = breakout_score+vol_score+trend_score+rs_score+vcp_score
    final = max(0, min(150, base + close_quality + cre - risk_penalty))
    norm = max(0, min(100, final))
    signal = "BREAKOUT" if norm>=80 else "SETUP" if norm>=60 else "WATCH" if norm>=40 else "NEUTRAL"
    quote = quote or {}
    row = {
        "ticker": sym, "company": quote.get("longName") or quote.get("shortName") or sym,
        "price": round(c,2), "change1d": round(_roc(closes,1)*100,2), "score": round(norm,1), "rawScore": round(final,1), "signal": signal,
        "breakoutScore": round(breakout_score,1), "volumeScore": round(vol_score,1), "trendScore": round(trend_score,1), "relativeStrengthScore": round(rs_score,1), "vcpScore": round(vcp_score,1),
        "closeQuality": round(close_quality,1), "creBonus": round(cre,1), "riskPenalty": round(risk_penalty,1), "riskFlags": penalties,
        "breakoutPct": round(breakout_pct*100,2), "rvol": round(rvol,2), "ema20Dist": round(dist_ema20*100,2), "rs20": round(rs20*100,2), "atrPct": round(atr_pct*100,2),
        "closeStrength": round(close_strength*100,1), "upperWick": round(upper_wick*100,1), "range10": round(range10*100,2), "dryup": bool(dryup),
        "high52w": round(h52,2), "pos52w": round((c/h52)*100,1) if h52 else 0, "rsi14": round(_rsi(closes,14),1), "roc20": round(ret20*100,2),
        "market": {"spyAboveEma20": bool(spy_above), "qqqRed": bool(qqq_red)}
    }
    row = _attach_combined_breakout_scores(row, sym, closes, vols, quote)
    write_cache("breakout_row_" + sym, row)
    return row


def _breakout_feature_snapshot(arr, end_idx):
    """Single-ticker historical feature snapshot used by manual ML-flow."""
    sub = arr[:end_idx+1]
    if len(sub) < 80:
        return None
    closes=[x["close"] for x in sub]; highs=[x["high"] for x in sub]; lows=[x["low"] for x in sub]; vols=[x["volume"] for x in sub]
    c=closes[-1]; h=highs[-1]; l=lows[-1]; v=vols[-1]
    prev5=max(highs[-6:-1]) if len(highs)>=6 else max(highs[:-1])
    ema20=_ema(closes[-80:],20); ema50=_ema(closes[-120:],50)
    vol20=_sma(vols,20); rvol=(v/vol20) if vol20 else 0
    atr14=_atr(highs,lows,closes,14); atr_pct=(atr14/c) if c else 0
    ret20=_roc(closes,20); ret60=_roc(closes,60)
    breakout_pct=(c-prev5)/prev5 if prev5 else 0
    day_range=max(h-l, 1e-9)
    close_strength=(c-l)/day_range
    range10=(max(highs[-11:-1])-min(lows[-11:-1]))/c if len(highs)>=11 and c else 1
    dist_ema20=(c-ema20)/ema20 if ema20 else 0
    return [breakout_pct, rvol, 1 if c>ema20 else 0, 1 if c>ema50 else 0, 1 if ema20>ema50 else 0, ret20, ret60, atr_pct, close_strength, range10, dist_ema20]

def _single_ticker_ml_probability(symbol, logs):
    """Train a lightweight single-ticker-only historical model/proxy.
    It intentionally uses ONLY the requested ticker's own history and never scans any universe.
    """
    clean = yahoo_symbol(symbol)
    arr = _chart_arrays(clean, "5y")
    rows=[]; labels=[]
    target_days=252
    for i in range(260, max(260, len(arr)-target_days)):
        f=_breakout_feature_snapshot(arr, i)
        if not f: continue
        c=arr[i]["close"]
        fut=arr[i+target_days]["close"] if i+target_days < len(arr) else None
        if not c or not fut: continue
        rows.append(f); labels.append(1 if (fut/c - 1.0) >= 1.0 else 0)
    logs.append(f"  Single-ticker træningsrækker for {clean}: {len(rows)}")
    if len(rows) < 20:
        logs.append("  ML fallback: for lidt 5-års historik til robust single-ticker træning")
        return 0.0, "FALLBACK", len(rows)
    cur=_breakout_feature_snapshot(arr, len(arr)-1)
    if not cur:
        return 0.0, "FALLBACK", len(rows)
    # Simple similarity-weighted probability using only this ticker's own historical rows.
    # This avoids external/universe leakage and works without extra packages.
    import math
    cols=list(zip(*rows))
    means=[sum(c)/len(c) for c in cols]
    stds=[]
    for c,m in zip(cols,means):
        std=(sum((x-m)**2 for x in c)/len(c))**0.5
        stds.append(std if std>1e-9 else 1.0)
    def dist(a,b):
        return sum(((x-y)/s)**2 for x,y,s in zip(a,b,stds))**0.5
    pairs=sorted((dist(cur,r), lab) for r,lab in zip(rows,labels))[:max(15, min(60, len(rows)//4))]
    if not pairs:
        return 0.0, "FALLBACK", len(rows)
    weights=[1/(1+d) for d,_ in pairs]
    prob=sum(w*lab for w,(_,lab) in zip(weights,pairs))/sum(weights)
    base_rate=sum(labels)/len(labels)
    prob=0.70*prob + 0.30*base_rate
    logs.append(f"  Positiv historik (+100% / 1 år) for {clean}: {sum(labels)}/{len(labels)}")
    logs.append(f"  Single-ticker ML sandsynlighed: {prob*100:.1f}%")
    return max(0.0, min(1.0, prob)), "SINGLE_TICKER_ONLY", len(rows)

def breakout_single_ticker_mlflow(symbol):
    clean = yahoo_symbol(symbol)
    logs=[]
    logs.append(f"=== MANUEL TICKER SCAN · {clean} · single-ticker mode · 1 ticker ===")
    logs.append("Target: +100% / 1 år · Ensemble ML + VCP/CRE Breakout Score")
    logs.append(f"FASE 1/2 — Træner RF+GB Ensemble kun på {clean} historik (5 år)...")
    try:
        prob, mode, nrows = _single_ticker_ml_probability(clean, logs)
    except Exception as e:
        prob, mode, nrows = 0.0, "FALLBACK", 0
        logs.append(f"  ML fallback: {e}")
    logs.append(f"FASE 2/2 — Beregner ML + VCP/CRE breakout score kun for {clean}...")
    q={}
    try: q=yahoo_quote_batch([clean]).get(yahoo_symbol(clean), {})
    except Exception: pass
    row=breakout_score_symbol(clean, q)
    # Blend ML probability as a small additive/penalty layer without changing the technical engine too violently.
    row["mlProbability"] = round(prob*100, 1)
    row["mlMode"] = mode
    row["mlTrainingRows"] = nrows
    if mode != "FALLBACK":
        ml_adj = (prob - 0.10) * 20.0
        row["scoreBeforeMl"] = row.get("score")
        row["score"] = round(max(0, min(100, row.get("score",0) + ml_adj)), 1)
        row["combinedScore"] = row["score"]
        if isinstance(row.get("scoreModel"), dict):
            row["scoreModel"]["combined"] = row["score"]
            row["scoreModel"]["mlAdjusted"] = True
        row["signal"] = "BREAKOUT" if row["score"]>=80 else "SETUP" if row["score"]>=60 else "WATCH" if row["score"]>=40 else "NEUTRAL"
    logs.append(f"✅ Færdig · {clean} score {row.get('score')}/100 · {row.get('signal')} · ML {row.get('mlProbability')}%")
    return {"ok": True, "result": row, "logs": logs}

def breakout_scan_symbols(symbols, limit=None):
    symbols=[yahoo_symbol(s) for s in symbols if str(s).strip()]
    limit = _coerce_optional_limit(limit, default=None, max_limit=max(SMALLCAP_MAX_LIMIT, len(symbols) or 0))
    scan_symbols = symbols if limit is None else symbols[:limit]
    quotes={}
    try: quotes=yahoo_quote_batch(scan_symbols)
    except Exception: pass
    rows=[]
    for s in scan_symbols:
        try: rows.append(breakout_score_symbol(s, quotes.get(yahoo_symbol(s),{})))
        except Exception as e: rows.append({"ticker": yahoo_symbol(s), "ok": False, "error": str(e), "score":0, "signal":"ERROR"})
    rows=[r for r in rows if r.get("ok", True) is not False]
    rows.sort(key=lambda x: x.get("score",0), reverse=True)
    return rows

MARKETPULSE_PREFIX = "/api/marketpulse"

def _marketpulse_missing_payload():
    return {
        "ok": False,
        "error": "MarketPulse kunne ikke indlaeses",
        "detail": str(_marketpulse_import_error),
        "hint": "Installer requirements fra volume scanner hvis moduler mangler: flask, flask-cors, yfinance, requests.",
    }

def _marketpulse_mask(value):
    if not value:
        return ""
    if len(value) <= 8:
        return "*" * len(value)
    return value[:4] + "*" * max(0, len(value) - 8) + value[-4:]

def marketpulse_get_payload(path, qs):
    if marketpulse_core is None:
        return _marketpulse_missing_payload(), 500
    mp = marketpulse_core
    sub = path[len(MARKETPULSE_PREFIX):] or "/"
    try:
        if sub == "/scan":
            force = (qs.get("force", [""])[0] or "").lower() == "true"
            result = mp._cached_scan(force)
            return {"ok": True, **result}, 200
        if sub == "/top-volume":
            force = (qs.get("force", [""])[0] or "").lower() == "true"
            result = mp._cached_scan(force)
            return {"ok": True, "data": result.get("topVolume", []), "cached": result.get("cached"), "age": result.get("age", 0)}, 200
        if sub == "/top-ratio":
            force = (qs.get("force", [""])[0] or "").lower() == "true"
            result = mp._cached_scan(force)
            return {"ok": True, "data": result.get("topRatio", []), "cached": result.get("cached"), "age": result.get("age", 0)}, 200
        if sub == "/sources":
            return {
                "ok": True,
                "sources": [
                    {"name": "Yahoo Finance", "active": True, "key": False, "coverage": "CTA+UTP consolidated", "latency": "15 min"},
                    {"name": "Stooq", "active": True, "key": False, "coverage": "US consolidated via data partner", "latency": "EOD"},
                    {"name": "Massive.com", "active": bool(mp.get_key("massive")), "key": True, "coverage": "US exchanges + dark pools + OTC", "latency": "EOD / delayed / real-time"},
                    {"name": "Finnhub", "active": bool(mp.get_key("finnhub")), "key": True, "coverage": "US consolidated + partial dark pool", "latency": "real-time"},
                    {"name": "Tiingo", "active": bool(mp.get_key("tiingo")), "key": True, "coverage": "US consolidated via IEX feed", "latency": "real-time"},
                ],
                "keysConfigured": {
                    "massive": bool(mp.get_key("massive")),
                    "finnhub": bool(mp.get_key("finnhub")),
                    "tiingo": bool(mp.get_key("tiingo")),
                },
            }, 200
        if sub == "/rate-limits":
            return {
                "ok": True,
                "limits": {
                    "finnhub": {**mp.FINNHUB_LIMITER.stats(), "type": "token_bucket", "description": "60 requests per 60 seconds"},
                    "tiingo": {**mp.TIINGO_LIMITER.stats(), "type": "hourly_budget", "description": "50 tickers per clock hour"},
                },
            }, 200
        if sub == "/status":
            now = time.time()
            return {
                "ok": True,
                "server": "MarketPulse Multi-Source v3 via StockIntel",
                "cacheAge": int(now - mp._cache["ts"]) if mp._cache.get("ts") else None,
                "cacheTTL": mp.CACHE_TTL,
                "scannedCount": len((mp._cache.get("data") or {}).get("topVolume", [])),
                "activeSources": (mp._cache.get("data") or {}).get("activeSources", []),
                "sourceConfig": {
                    "Yahoo": True,
                    "Stooq": True,
                    "Massive": bool(mp.get_key("massive")),
                    "Finnhub": bool(mp.get_key("finnhub")),
                    "Tiingo": bool(mp.get_key("tiingo")),
                },
            }, 200
        if sub == "/keys":
            return {"ok": True, "keys": {k: {"set": bool(v), "masked": _marketpulse_mask(v)} for k, v in mp._KEYS.items()}}, 200
        if sub == "/history":
            return {"ok": True, "scans": mp.db_scan_history(20)}, 200
        if sub.startswith("/history/"):
            scan_id = int(sub.rsplit("/", 1)[-1])
            result = mp.db_load_scan_by_id(scan_id)
            if not result:
                return {"ok": False, "error": f"Scan #{scan_id} ikke fundet"}, 404
            return {"ok": True, **result}, 200
        return {"ok": False, "error": "Not found"}, 404
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1200:]}, 500

def marketpulse_post_payload(path, payload):
    if marketpulse_core is None:
        return _marketpulse_missing_payload(), 500
    mp = marketpulse_core
    sub = path[len(MARKETPULSE_PREFIX):] or "/"
    try:
        if sub.startswith("/keys/"):
            source = sub.rsplit("/", 1)[-1].strip().lower()
            if source not in mp.VALID_SOURCES:
                return {"ok": False, "error": f"Ukendt kilde: {source}"}, 400
            key = ((payload or {}).get("key") or "").strip()
            if not key:
                return {"ok": False, "error": "Noegle maa ikke vaere tom"}, 400
            valid, msg = mp._validate_key(source, key)
            if not valid:
                return {"ok": False, "error": f"Noegle afvist: {msg}"}, 400
            mp.set_key(source, key)
            mp._cache["data"] = None
            mp._cache["ts"] = 0.0
            return {"ok": True, "message": f"{source} noegle gemt"}, 200
        return {"ok": False, "error": "Not found"}, 404
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500

def marketpulse_delete_payload(path):
    if marketpulse_core is None:
        return _marketpulse_missing_payload(), 500
    mp = marketpulse_core
    sub = path[len(MARKETPULSE_PREFIX):] or "/"
    try:
        if sub.startswith("/keys/"):
            source = sub.rsplit("/", 1)[-1].strip().lower()
            if source not in mp.VALID_SOURCES:
                return {"ok": False, "error": f"Ukendt kilde: {source}"}, 400
            mp.set_key(source, "")
            mp._cache["data"] = None
            mp._cache["ts"] = 0.0
            return {"ok": True, "message": f"{source} noegle fjernet"}, 200
        return {"ok": False, "error": "Not found"}, 404
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500

AIINFRA_PREFIX = "/api/aiinfra"

def _aiinfra_missing_payload():
    return {
        "ok": False,
        "error": "AIINFRA.SCAN kunne ikke indlaeses",
        "detail": str(_aiinfra_import_error),
        "hint": "Kontroller at aiinfra_core.py, tickers_db.json og api_keys.json ligger i hovedprojektet.",
    }

def _aiinfra_mask(value):
    if not value:
        return ""
    if len(value) > 14:
        return value[:8] + "..." + value[-4:]
    return "(sat)"

def aiinfra_get_payload(path, qs):
    if aiinfra_core is None:
        return _aiinfra_missing_payload(), 500
    ai = aiinfra_core
    sub = path[len(AIINFRA_PREFIX):] or "/"
    try:
        if sub == "/ping":
            return {
                "ok": True,
                "source": "AIINFRA.SCAN via main host",
                "finnhub_configured": bool(ai._keys.get("finnhub")),
                "anthropic_configured": bool(ai._keys.get("anthropic")),
                "finnhub_rate": ai.fh_rate_status(),
            }, 200
        if sub == "/keys/status":
            return {
                "ok": True,
                "finnhub_set": bool(ai._keys.get("finnhub")),
                "anthropic_set": bool(ai._keys.get("anthropic")),
                "finnhub_masked": _aiinfra_mask(ai._keys.get("finnhub", "")),
                "anthropic_masked": _aiinfra_mask(ai._keys.get("anthropic", "")),
                "rate": ai.fh_rate_status(),
            }, 200
        if sub == "/tickers":
            data = ai.load_ticker_db()
            return {
                "ok": True,
                "version": data.get("version", 1),
                "updatedAt": data.get("updatedAt"),
                "tickers": data.get("tickers", []),
                "addedTickers": data.get("addedTickers", []),
            }, 200
        if sub == "/quotes":
            symbols = (qs.get("symbols", [""])[0] or "").strip()
            if not symbols:
                return {"ok": False, "error": "Ingen symbols"}, 400
            requested = [s.strip() for s in symbols.split(",") if s.strip()]
            try:
                payload = ai.yahoo_quote(",".join(requested))
                if payload.get("results"):
                    return payload, 200
            except Exception as e:
                quote_error = str(e)
            else:
                quote_error = "Yahoo gav 0 resultater"
            results, errors = [], {}
            for sym in requested:
                try:
                    q = ai.yahoo_chart_one(sym)
                    if q:
                        results.append(q)
                    else:
                        errors[sym] = "Ingen data"
                except Exception as e:
                    errors[sym] = str(e)
            if ai._keys.get("finnhub"):
                missing = [sym for sym in requested if not any(r.get("symbol") == sym for r in results)]
                if missing:
                    fh_results, fh_errors = ai.finnhub_batch(missing, ai._keys["finnhub"])
                    results.extend(fh_results)
                    errors.update(fh_errors)
            return {
                "ok": bool(results),
                "source": "Yahoo+Finnhub fallback",
                "quote_error": quote_error,
                "requested": requested,
                "results": results,
                "errors": errors,
            }, 200 if results else 502
        if sub == "/search":
            q = (qs.get("q", [""])[0] or "").strip()
            if not q:
                return {"ok": False, "error": "Ingen soegeterm"}, 400
            return {"ok": True, "query": q, "results": ai.yahoo_search(q)}, 200
        if sub == "/fred":
            sid = (qs.get("series", [""])[0] or "").strip()
            if not sid:
                return {"ok": False, "error": "Ingen series"}, 400
            return {"ok": True, "series": sid, "results": ai.fred_series(sid)}, 200
        if sub == "/buffett":
            try:
                w5000 = ai.yahoo_chart_history("^W5000", rng="10y", interval="1mo")
                gdp = ai.fred_series("GDP")
                if not w5000.get("price") or not gdp:
                    raise RuntimeError("Mangler data")
                latest_gdp = gdp[-1]
                mcap_val = float(w5000["price"])
                gdp_val = float(latest_gdp["val"])
                gdp_by_year = ai.latest_by_year(gdp)
                w5000_by_year = ai.latest_by_year(w5000.get("rows", []))
                hist = [
                    {"y": y, "v": round((w5000_by_year[y] / gdp_by_year[y]) * 100, 1)}
                    for y in sorted(w5000_by_year)
                    if int(y) >= 2015 and y in gdp_by_year
                ]
                current_year = w5000["date"][:4] if w5000.get("date") else str(time.gmtime().tm_year)
                current_point = {"y": current_year, "v": round((mcap_val / gdp_val) * 100, 1)}
                if not hist or hist[-1]["y"] != current_point["y"]:
                    hist.append(current_point)
                else:
                    hist[-1] = current_point
                return {
                    "ok": True,
                    "source": "Yahoo ^W5000 + FRED GDP",
                    "mcap": mcap_val,
                    "gdp": gdp_val,
                    "value": (mcap_val / gdp_val) * 100,
                    "lastUpdated": w5000.get("date") or latest_gdp["date"],
                    "gdpUpdated": latest_gdp["date"],
                    "history": hist,
                }, 200
            except Exception as e:
                return {
                    "ok": False,
                    "error": str(e),
                    "mcap": 73730.65,
                    "gdp": 30767,
                    "value": 73730.65 / 30767 * 100,
                    "history": [{"y": "2024", "v": 196}, {"y": "2025", "v": 221.4}, {"y": "2026", "v": 239.7}],
                }, 200
        return {"ok": False, "error": "Not found"}, 404
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1200:]}, 500

def aiinfra_post_payload(path, payload):
    if aiinfra_core is None:
        return _aiinfra_missing_payload(), 500
    ai = aiinfra_core
    sub = path[len(AIINFRA_PREFIX):] or "/"
    payload = payload or {}
    try:
        if sub == "/keys/save":
            changed = False
            for key_name in ("finnhub", "anthropic"):
                value = (payload.get(key_name) or "").strip()
                if value and value != ai._keys.get(key_name):
                    ai._keys[key_name] = value
                    changed = True
            if changed:
                ai.save_keys()
            return {
                "ok": True,
                "saved": list(ai._keys.keys()),
                "finnhub_set": bool(ai._keys.get("finnhub")),
                "anthropic_set": bool(ai._keys.get("anthropic")),
            }, 200
        if sub == "/tickers/save":
            data = ai.save_ticker_db(payload)
            return {
                "ok": True,
                "saved": len(data.get("tickers", [])),
                "added": len(data.get("addedTickers", [])),
                "updatedAt": data.get("updatedAt"),
            }, 200
        if sub == "/finnhub/quotes":
            api_key = ai._keys.get("finnhub") or payload.get("apiKey", "")
            if not api_key:
                return {"ok": False, "error": "Ingen Finnhub API-noegle. Gem den via Indstillinger."}, 400
            symbols = [s.strip() for s in (payload.get("symbols") or "").split(",") if s.strip()]
            if not symbols:
                return {"ok": False, "error": "Ingen symbols"}, 400
            results, errors = ai.finnhub_batch(symbols, api_key)
            return {"ok": True, "source": "finnhub", "results": results, "errors": errors, "rate": ai.fh_rate_status()}, 200
        if sub == "/ai":
            api_key = (payload.pop("apiKey", "") or "").strip() or ai._keys.get("anthropic") or os.environ.get("ANTHROPIC_API_KEY", "")
            if not api_key:
                return {
                    "ok": False,
                    "error": "Ingen Anthropic API-noegle fundet. Gem noeglen via Indstillinger-panelet i browseren.",
                }, 400
            req_body = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                "https://api.anthropic.com/v1/messages",
                data=req_body,
                headers={
                    "Content-Type": "application/json",
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                },
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    result = json.loads(resp.read().decode("utf-8"))
                return {"ok": True, "result": result}, 200
            except urllib.error.HTTPError as e:
                err_body = e.read().decode("utf-8", errors="replace")
                try:
                    msg = json.loads(err_body).get("error", {}).get("message", err_body)
                except Exception:
                    msg = err_body
                return {"ok": False, "error": f"Anthropic {e.code}: {msg}"}, 502
        return {"ok": False, "error": "Not found"}, 404
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500

class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)
    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()
    def do_OPTIONS(self):
        self.send_response(204); self.end_headers()
    def json_response(self, payload, status=200):
        data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status); self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)
    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.startswith(MARKETPULSE_PREFIX):
            try:
                length = int(self.headers.get("Content-Length", "0") or 0)
                payload = {}
                if length > 0:
                    payload = json.loads(self.rfile.read(length).decode("utf-8", errors="replace"))
                response, status = marketpulse_post_payload(parsed.path, payload)
                return self.json_response(response, status)
            except Exception as e:
                return self.json_response({"ok": False, "error": str(e)}, 400)
        if parsed.path.startswith(AIINFRA_PREFIX):
            try:
                length = int(self.headers.get("Content-Length", "0") or 0)
                payload = {}
                if length > 0:
                    payload = json.loads(self.rfile.read(length).decode("utf-8", errors="replace"))
                response, status = aiinfra_post_payload(parsed.path, payload)
                return self.json_response(response, status)
            except Exception as e:
                return self.json_response({"ok": False, "error": str(e)}, 400)
        if parsed.path == "/api/scan-state":
            try:
                length = int(self.headers.get("Content-Length", "0") or 0)
                if length <= 0:
                    return self.json_response({"ok": False, "error": "Missing body"}, 400)
                if length > 60 * 1024 * 1024:
                    return self.json_response({"ok": False, "error": "Body too large"}, 413)
                payload = json.loads(self.rfile.read(length).decode("utf-8", errors="replace"))
                state = save_scan_state(payload.get("type"), payload.get("rows") or [], payload.get("meta") or {})
                return self.json_response({"ok": True, "type": payload.get("type"), "savedAt": state.get("savedAt"), "count": state.get("count")})
            except Exception as e:
                return self.json_response({"ok": False, "error": str(e)}, 400)
        return self.json_response({"ok": False, "error": "Not found"}, 404)
    def do_DELETE(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.startswith(MARKETPULSE_PREFIX):
            response, status = marketpulse_delete_payload(parsed.path)
            return self.json_response(response, status)
        return self.json_response({"ok": False, "error": "Not found"}, 404)
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query)
        if parsed.path == "/favicon.ico":
            self.send_response(204); self.end_headers(); return

        if parsed.path.startswith(MARKETPULSE_PREFIX):
            response, status = marketpulse_get_payload(parsed.path, qs)
            return self.json_response(response, status)

        if parsed.path.startswith(AIINFRA_PREFIX):
            response, status = aiinfra_get_payload(parsed.path, qs)
            return self.json_response(response, status)

        if parsed.path == "/api/scan-state":
            scan_type = (qs.get("type", [""])[0] or "").strip().lower()
            data = load_scan_state()
            if scan_type:
                state = data.get(scan_type) or {"rows": [], "count": 0, "meta": {}}
                return self.json_response({"ok": True, "type": scan_type, "state": state})
            return self.json_response({"ok": True, "state": data})

        # ETF PRO — isolated endpoints. Stock Analyse/Screener endpoints stay unchanged.
        if parsed.path == "/api/etf":
            if etf_core is None:
                return self.json_response({"ok": False, "error": "ETF PRO kunne ikke indlæses"}, 500)
            ticker = (qs.get("ticker", [""])[0] or "").strip().upper()
            if not ticker:
                return self.json_response({"ok": False, "error": "Missing ticker"}, 400)
            try:
                ov = etf_core.etf_overview(ticker)
                if not ov.get("ok") and not ov.get("dbFound"):
                    msg = f"Kunne ikke hente '{ticker}'."
                    errs = ov.get("errors", {})
                    first = next(iter(errs.values()), "") if errs else ""
                    if "no result" in str(first).lower():
                        msg += f" Prøv '{etf_core.tvsym(ticker)}' uden børssuffix."
                    elif first:
                        msg += " " + str(first)[:150]
                    return self.json_response({"ok": False, "error": msg, "debug_errors": errs}, 502)

                raw_h = ov.get("holdingsRaw") or []
                syms = [h["ticker"] for h in raw_h if h.get("ticker")]
                stock_map = {s["ticker"]: s for s in etf_core.holdings_batch(syms[:50]) if s}

                enriched = []
                for h in raw_h:
                    t = h.get("ticker") or ""
                    sd = stock_map.get(t, {})
                    wt = float(h.get("weight") or 0)
                    if wt > 1:
                        wt /= 100
                    enriched.append({
                        "ticker": t, "name": h.get("name") or sd.get("name") or t,
                        "weight": wt, "sector": sd.get("sector", ""),
                        "industry": sd.get("industry", ""),
                        "price": sd.get("price"), "change": sd.get("change"),
                        "marketCap": sd.get("marketCap"),
                        "week52High": sd.get("week52High"), "week52Low": sd.get("week52Low"),
                        "m1": sd.get("m1"), "m3": sd.get("m3"), "m6": sd.get("m6"), "m12": sd.get("m12"),
                        "hypeScore": sd.get("hypeScore", 0),
                        "hype_momentum": sd.get("hype_momentum", 0),
                        "hype_trend": sd.get("hype_trend", 0),
                        "hype_size": sd.get("hype_size", 0),
                    })
                enriched.sort(key=lambda x: x.get("weight") or 0, reverse=True)

                sw = ov.get("sectorWeights") or {}
                cw = ov.get("countryWeights") or {}
                sc2 = etf_core.etf_scores(enriched, sw, cw)
                return self.json_response({
                    "ok": True,
                    "overview": {**ov, "etfScore": sc2},
                    "holdings": enriched, "etfScore": sc2,
                    "sectors": sw, "countries": cw,
                    "countrySource": ov.get("countrySource", ""),
                    "sectorSource": ov.get("sectorSource", ""),
                    "countryUrl": ov.get("countryUrl", ""),
                    "hasYahooSectors": ov.get("sectorSource") == "Yahoo",
                    "sectorColors": etf_core.SECTOR_COLORS, "countryColors": etf_core.COUNTRY_COLORS,
                })
            except Exception as e:
                import traceback
                return self.json_response({"ok": False, "error": str(e), "trace": traceback.format_exc()[-800:]}, 502)

        if parsed.path == "/api/etf-search":
            if etf_core is None:
                return self.json_response({"ok": False, "results": [], "error": "ETF PRO kunne ikke indlæses"}, 500)
            q = (qs.get("q", [""])[0] or "").strip()
            if not q:
                return self.json_response({"ok": True, "results": []})
            results = []
            q_up = q.upper(); q_lo = q.lower()
            for r in list(etf_core._db_by_isin.values()):
                ticker = (r.get("ticker") or "").upper()
                name = (r.get("name") or "").lower()
                isin_v = (r.get("isin") or "").upper()
                if (q_up in ticker or q_up in isin_v or q_lo in name):
                    results.append({
                        "ticker": ticker + ".DE", "name": r.get("name", ""), "exchange": "XETR", "type": "ETF",
                        "isin": isin_v, "provider": r.get("provider", ""), "category": r.get("category", ""), "ter": r.get("ter", ""),
                    })
            if len(results) == 0:
                try:
                    c = etf_core.get_crumb()
                    pm = {"q": q, "quotesCount": 10, "newsCount": 0, "quotesQueryId": "tss_match_phrase_query"}
                    if c: pm["crumb"] = c
                    data = etf_core.gjson("https://query1.finance.yahoo.com/v1/finance/search?" + urllib.parse.urlencode(pm), timeout=10)
                    for r in (data.get("quotes") or []):
                        if (r.get("quoteType") or "").upper() in ("ETF", "MUTUALFUND", "INDEX", "FUND"):
                            ticker = r.get("symbol", "")
                            if not any(x["ticker"] == ticker for x in results):
                                db_match = etf_core.db_lookup(ticker.split(".")[0])
                                results.append({
                                    "ticker": ticker, "name": r.get("longname") or r.get("shortname") or ticker,
                                    "exchange": r.get("exchDisp") or r.get("exchange") or "", "type": "ETF",
                                    "isin": db_match.get("isin", "") if db_match else "",
                                    "provider": db_match.get("provider", "") if db_match else "",
                                    "category": db_match.get("category", "") if db_match else "",
                                    "ter": db_match.get("ter", "") if db_match else "",
                                })
                except Exception:
                    pass
            return self.json_response({"ok": True, "results": results[:12]})

        if parsed.path == "/api/etf-db":
            if etf_core is None:
                return self.json_response({"ok": False, "error": "ETF PRO kunne ikke indlæses"}, 500)
            q = (qs.get("q", [""])[0] or "").strip().lower()
            provider = (qs.get("provider", [""])[0] or "").strip()
            category = (qs.get("category", [""])[0] or "").strip()
            recs = list(etf_core._db_by_isin.values())
            if q:
                recs = [r for r in recs if q in (r.get("name", "")).lower() or q in (r.get("ticker", "")).lower() or q in (r.get("isin", "")).lower()]
            if provider:
                recs = [r for r in recs if r.get("provider", "").lower() == provider.lower()]
            if category:
                recs = [r for r in recs if r.get("category", "").lower() == category.lower()]
            return self.json_response({"ok": True, "total": len(recs), "results": [{"ticker": r.get("ticker", ""), "name": r.get("name", ""), "isin": r.get("isin", ""), "provider": r.get("provider", ""), "category": r.get("category", ""), "ter": r.get("ter", ""), "distribution": r.get("distribution", "")} for r in recs[:200]]})

        if parsed.path == "/api/etf-debug":
            if etf_core is None:
                return self.json_response({"error": "ETF PRO kunne ikke indlæses"}, 500)
            t = (qs.get("ticker", [""])[0] or "").strip().upper()
            if not t:
                return self.json_response({"error": "no ticker"})
            db = etf_core.db_lookup(t)
            c = etf_core.get_crumb()
            return self.json_response({"ticker": t, "tvSymbol": etf_core.tvsym(t), "yahooSymbol": etf_core.yahoo_sym(t, db), "crumb_ok": bool(c), "db_found": db is not None, "db_record": db, "db_total": len(etf_core._db_by_isin)})


        if parsed.path == "/api/breakout-universes":
            smallcap_limit = _coerce_optional_limit(qs.get("smallcapLimit", ["all"])[0], default=None)
            wm = get_week_breakout_universe_map(smallcap_limit=smallcap_limit)
            original = {k: len(_clean_symbol_list(v)) for k, v in BREAKOUT_UNIVERSES.items()}
            screener = {
                "S&P 500 + QQQ": len(wm.get("S&P 500 + QQQ", [])),
                "Semiconductor Equipment & Materials": len(wm.get("Semiconductor Equipment & Materials", [])),
                "Russell 2000": len(wm.get("Russell 2000", [])),
                "Nuclear Tech": len(wm.get("Nuclear Tech", [])),
                "Space · Defence · Aerospace · Robotics": len(wm.get("Space · Defence · Aerospace · Robotics", [])),
                "AI Data · Power · Infra": len(wm.get("AI Data · Power · Infra", [])),
                "All Mega Themes": len(wm.get("All Mega Themes", [])),
                "Small Caps — Week Breakout": len(wm.get("Small Caps — Week Breakout", [])),
                "All Screener Universes — Week Breakout": len(wm.get("All Screener Universes — Week Breakout", [])),
                "All Local + Online — Week Breakout": len(wm.get("All Local + Online — Week Breakout", [])),
            }
            return self.json_response({"ok": True, "universes": original, "screenerUniverses": screener, "all": len(wm.get("All Breakout Universes", [])), "separateDataStructures": True})

        if parsed.path == "/api/breakout-single":
            symbol = qs.get("symbol", [""])[0].strip()
            if not symbol:
                return self.json_response({"ok": False, "error": "Missing symbol"}, 400)
            try:
                return self.json_response(breakout_single_ticker_mlflow(symbol))
            except Exception as e:
                import traceback
                return self.json_response({"ok": False, "error": str(e), "trace": traceback.format_exc()[-1200:]}, 502)

        if parsed.path == "/api/breakout-tickers":
            universe = qs.get("universe", ["Momentum"])[0]
            smallcap_limit = _coerce_optional_limit(qs.get("smallcapLimit", ["all"])[0], default=None)
            try:
                symbols, label = get_breakout_scan_universe(universe, smallcap_limit=smallcap_limit)
                return self.json_response({"ok": True, "universe": label, "sourceCount": len(symbols), "tickers": symbols})
            except Exception as e:
                return self.json_response({"ok": False, "error": str(e)}, 502)

        if parsed.path == "/api/breakout":
            symbol = qs.get("symbol", [""])[0].strip()
            if not symbol:
                return self.json_response({"ok": False, "error": "Missing symbol"}, 400)
            try:
                q = {}
                try: q = yahoo_quote_batch([symbol]).get(yahoo_symbol(symbol), {})
                except Exception: pass
                return self.json_response({"ok": True, "result": breakout_score_symbol(symbol, q)})
            except Exception as e:
                return self.json_response({"ok": False, "error": str(e)}, 502)

        if parsed.path == "/api/breakout-scan":
            universe = qs.get("universe", ["Momentum"])[0]
            raw_symbols = qs.get("symbols", [""])[0]
            limit = _coerce_optional_limit(qs.get("limit", ["all"])[0], default=None)
            smallcap_limit = _coerce_optional_limit(qs.get("smallcapLimit", ["all"])[0], default=None)
            if raw_symbols.strip():
                symbols = [x.strip() for x in raw_symbols.split(",") if x.strip()]
                label = "Custom"
            else:
                symbols, label = get_breakout_scan_universe(universe, smallcap_limit=smallcap_limit)
            try:
                rows = breakout_scan_symbols(symbols, limit=limit)
                return self.json_response({"ok": True, "universe": label, "sourceCount": len(symbols), "count": len(rows), "results": rows})
            except Exception as e:
                return self.json_response({"ok": False, "error": str(e)}, 502)

        if parsed.path == "/api/backtest":
            symbol = qs.get("symbol", [""])[0].strip()
            if not symbol:
                return self.json_response({"ok": False, "error": "Missing symbol"}, 400)
            try:
                row = build_stock(symbol)
                ps = row.get("proScores", {})
                derived = ps.get("derived", {})
                bt = derived.get("backtestProxy") or calc_backtest_proxy(row.get("totalScore"), row.get("revisions"), row.get("risk"), row.get("momentum"), row.get("m12"))
                return self.json_response({"ok": True, "ticker": row.get("ticker"), "company": row.get("company"), "backtest": bt, "note": "Proxy only: true point-in-time backtesting requires a stored historical database."})
            except Exception as e:
                return self.json_response({"ok": False, "error": str(e)}, 502)

        if parsed.path == "/api/ping":
            return self.json_response({
                "ok": True, "message": "Yahoo crumb proxy kører", "host": HOST, "port": PORT,
                "source": "Yahoo Finance crumb/session", "batchSize": MAX_BATCH_SYMBOLS,
                "workers": BATCH_WORKERS, "version": "stock-pro-10x-v10.4-split-screener-breakout-universes"
            })
        if parsed.path == "/api/yahoo-test":
            try:
                ensure_yahoo_session()
                data = get_stocks_batch(["NVDA", "AAPL", "MSFT"])
                return self.json_response({"ok": True, "results": data})
            except Exception as e:
                return self.json_response({"ok": False, "error": str(e)}, 200)
        if parsed.path == "/api/universe":
            mode = qs.get("mode", ["demo"])[0]
            try:
                tickers = get_universe(mode)
                return self.json_response({"ok": True, "mode": mode, "count": len(tickers), "tickers": tickers})
            except Exception as e:
                return self.json_response({"ok": False, "error": str(e)}, 502)
        if parsed.path == "/api/news":
            symbol = qs.get("symbol", [""])[0].strip()
            if not symbol:
                return self.json_response({"ok": False, "error": "Missing symbol"}, 400)
            try:
                news = yahoo_news(symbol)
                return self.json_response({"ok": True, "ticker": yahoo_symbol(symbol), "count": len(news), "news": news})
            except Exception as e:
                return self.json_response({"ok": False, "error": str(e)}, 502)

        if parsed.path == "/api/earnings":
            symbol = qs.get("symbol", [""])[0].strip()
            if not symbol:
                return self.json_response({"ok": False, "error": "Missing symbol"}, 400)
            try:
                data = yahoo_earnings(symbol)
                return self.json_response({"ok": True, **data})
            except Exception as e:
                return self.json_response({"ok": False, "error": str(e)}, 502)

        if parsed.path == "/api/stock-finnhub":
            symbol = qs.get("symbol", [""])[0].strip()
            token = qs.get("token", [""])[0].strip()
            if not symbol:
                return self.json_response({"ok": False, "error": "Missing symbol"}, 400)
            if not token:
                return self.json_response({"ok": False, "error": "Missing Finnhub token"}, 400)
            try:
                quote = {}
                try:
                    quote = yahoo_quote_batch([symbol]).get(yahoo_symbol(symbol), {})
                except Exception:
                    pass
                return self.json_response(apply_finnhub_overlay(build_stock(symbol, quote), symbol, token))
            except Exception as e:
                import traceback
                return self.json_response({"ok": False, "error": str(e), "trace": traceback.format_exc()[-1200:]}, 502)

        if parsed.path == "/api/stock":
            symbol = qs.get("symbol", [""])[0].strip()
            if not symbol: return self.json_response({"ok": False, "error": "Missing symbol"}, 400)
            quote = {}
            try: quote = yahoo_quote_batch([symbol]).get(yahoo_symbol(symbol), {})
            except Exception: pass
            return self.json_response(build_stock(symbol, quote))
        if parsed.path == "/api/stocks":
            raw_symbols = qs.get("symbols", [""])[0]
            symbols = [s.strip() for s in raw_symbols.split(",") if s.strip()]
            if not symbols: return self.json_response({"ok": False, "error": "Missing symbols"}, 400)
            symbols = symbols[:MAX_BATCH_SYMBOLS]
            return self.json_response({"ok": True, "source": "Yahoo Finance crumb/session", "count": len(symbols), "results": get_stocks_batch(symbols)})

        if parsed.path == "/api/smallcaps":
            try:
                limit = int(qs.get("limit", ["100"])[0])
            except Exception:
                limit = 100
            limit = max(10, min(SMALLCAP_MAX_LIMIT, limit))
            try:
                tickers = get_small_caps_from_finviz(limit=limit)
                return self.json_response({"ok": True, "source": "smallcaps-database fallback + optional Nasdaq/Yahoo/Finviz refresh", "count": len(tickers), "tickers": tickers})
            except Exception as e:
                return self.json_response({"ok": False, "error": str(e)}, 502)

        if parsed.path == "/api/smallcaps-data":
            try:
                limit = int(qs.get("limit", ["100"])[0])
            except Exception:
                limit = 100
            limit = max(10, min(SMALLCAP_MAX_LIMIT, limit))
            try:
                tickers = get_small_caps_from_finviz(limit=limit)
                out = []
                scanned = 0
                for i in range(0, len(tickers), MAX_BATCH_SYMBOLS):
                    batch_rows = get_stocks_batch(tickers[i:i+MAX_BATCH_SYMBOLS])
                    scanned += len(batch_rows)
                    out.extend([r for r in batch_rows if market_cap_under_20b(r)])
                    if len(out) >= limit:
                        out = out[:limit]
                        break
                return self.json_response({"ok": True, "source": "smallcaps-database fallback + Yahoo Finance live data", "count": len(out), "scanned": scanned, "results": out})
            except Exception as e:
                return self.json_response({"ok": False, "error": str(e)}, 502)

        return super().do_GET()

def main():
    os.chdir(ROOT)
    if etf_core is not None:
        etf_core._load_db()
        etf_core._clear_broken_cache()
    if marketpulse_core is not None:
        marketpulse_core._init_keys()
        marketpulse_core.db_init()
    if aiinfra_core is not None:
        aiinfra_core.load_keys()
    display_host = LOCAL_URL_HOST if HOST in ("0.0.0.0", "::") else HOST
    url = f"http://{display_host}:{PORT}/"
    print("=" * 64)
    print("STOCK PRO 10X + ETF PRO + WEEK BREAKOUT SCANNER + MARKETPULSE + AIINFRA.SCAN")
    print("Open:", url)
    print("No FMP. Uses Yahoo cookie/crumb session.")
    print("Local address remains http://localhost:7722/ when PORT is not overridden.")
    print("=" * 64)
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()

if __name__ == "__main__":
    main()
