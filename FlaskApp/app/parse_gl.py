import pandas as pd
import os
import numpy as np

from FlaskApp.app import app, db
import FlaskApp.app.common as common

def parse_gl():
    # Load GL file
    common.logger.debug(f"PWD = {os.getcwd()}")
    gl_file = "FlaskApp/app/assets/General_ledger.csv"
    df = pd.read_csv(gl_file,thousands=',',na_values=["","NA","null"])  # adjust path if needed
    #remove non transaction rows:
    mask = df['Date'].notna()
    df = df[mask]
    
    #clean up dataframe
    df['Date'] = pd.to_datetime(df['Date'],dayfirst=True)
    df[['Debit','Credit','No.']] = df[['Debit','Credit','No.']].fillna(0)
    df['Name'] = df['Name'].fillna('None')
    
    #look for matching debit and credit entries
    df['Amount'] = df['Debit'] + df['Credit']
    sort_columns = ['Date','Amount','No.','Transaction Type','Name']
    df = df.sort_values(by=sort_columns)
    df_double = df.groupby(by=['Date','Transaction Type','No.','Name'])[['Debit','Credit']].agg('sum')
    df = df.set_index(sort_columns)
    
    # Find matching Debit and Credit in groupby and assign a unique transaction id.
    # change to string to avoid rounding errors
    df_double['Debit'] = df_double['Debit'].apply(lambda x: f"{x:.2f}")
    df_double['Credit'] = df_double['Credit'].apply(lambda x: f"{x:.2f}")
    #mask_double = df_double.duplicated(subset=["Debit", "Credit"], keep=False)
    mask_double = df_double['Debit']==df_double['Credit']

    # Assign transaction IDs for matches, 'check' otherwise
    df_double["transaction_id"] = np.where(
        mask_double,
        range(1, len(df_double)+1),  # unique ID per group
        "check"
    )

    df = pd.merge(df,df_double[['transaction_id']],left_index=True,right_index=True,how='left')

    common.logger.debug(f"Number of transactions to check = {len(df[df['transaction_id']=='check'])}")

    df.to_csv(gl_file)
    df_double.to_csv(gl_file.replace('.csv','_db.csv'))

if __name__ == "__main__":
    # Run inside Flask app context
    with app.app_context():
        parse_gl()

