import os,time,pandas as pd
from delta import run  

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

MAX_ROWS = 1437
def save_initial_data(symbol, df):
    df.to_parquet(f"{DATA_DIR}/{symbol}.parquet", compression='snappy')
    print(f"Initial data for {symbol} saved.")

def update_symbol_data(symbol: str, new_row: pd.DataFrame):
    filename = f"{DATA_DIR}/{symbol}.parquet"
    if os.path.exists(filename):
        df = pd.read_parquet(filename)
        df = pd.concat([df, new_row], ignore_index=True)
        if len(df) > MAX_ROWS:
            df = df.iloc[-MAX_ROWS:]
    else:
        df = new_row
    df.to_parquet(filename, index=False)
    print(f"Updated data for {symbol} saved.")

def loop_update():
    while True:
        symbol_data = run(60)
        for symbol, df in symbol_data.items():
            update_symbol_data(symbol, df)
        time.sleep(60)

if __name__ == "__main__":
    a=run(60*60*24)
    for symbol, df in a.items():
        if df is None:continue
        save_initial_data(symbol, df)
    loop_update()