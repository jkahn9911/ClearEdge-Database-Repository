from datetime import date, timedelta
import os
import sys

home_dir = os.path.dirname(os.path.dirname(os.getcwd()))
sys.path.insert(0, home_dir)
from util import db, nlp

# Get datetime for SELECT query
yesterday = date.today() - timedelta(days = 31)
yes_str = yesterday.strftime('%Y-%m-%d')

# TODO: change to 'dbo.Case_Detail' for production
table = 'import.Case_Copy'

# Open Connection
    # TODO: change to is_prod=True for production
conn = db.open_conn()

# SELECT new rows from yesterday
new_rows = db.get_table_pd(f"SELECT * FROM {table} WHERE Update_Date > '{yes_str}'", conn)

# If not new_rows:
#   return date.today().strftime('%Y-%m-%d')+": no updated rows"

# Cleaning and appending to create the Full Position Title

    # Columns that create the full title
title_cols = ['item_description', 'product_family','oem_company', 'level']
    # if NA imput '' else lower case
new_rows = nlp.str_impute_lower(new_rows, title_cols)
    # TODO:change .full_position_title to detail_notes for production
new_rows.full_position_title = new_rows[title_cols].agg(" ".join, axis=1)

token_full = nlp.tokenize_col(new_rows.full_position_title)
cleaned_matched_pos = nlp.normalize_tokens(token_full)
new_rows.full_position_title = [" ".join(row) for row in cleaned_matched_pos]


for i in range(len(new_rows)):
        #TODO: Full_Position_Title to Detail_Notes
    conn.cursor.execute(f"UPDATE {table} SET Full_Position_Title='{new_rows.full_position_title[i]}' WHERE ID = {new_rows.id[i]}")
    conn.cnxn.commit()
conn.cnxn.close()
