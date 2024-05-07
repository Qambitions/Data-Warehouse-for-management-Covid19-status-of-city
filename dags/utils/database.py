import numpy as np
from tqdm import tqdm
##########to do list: ignore update columns
def normalize_value_sql(string):
    if string is not None:
        if type(string) == str: 
            string = string.replace("'","''")
        if isinstance(string, (int, float, complex)) and not isinstance(string, bool):
            return f""" {string} """
        return f""" '{string}' """
        
    else: 
        return """ NULL  """

def _generate_data(data, chunk_size=10):
    for i in range(0, len(data), chunk_size):
        yield data[i:i+chunk_size]

def insert_update_condition_sql(df_clean, tableName, update = True,lookup_col = None, indentity_insert = False, chunk_size =100):
    df_clean.replace({np.nan: None},inplace=True)
    cols = df_clean.columns.tolist()
    sub_cols=cols.copy()
    target_fields_list = ", ".join(cols)
    target_fields = "({})".format(target_fields_list)

    for i in range(len(sub_cols)):
        sub_cols[i] = 's.' + sub_cols[i]
    sub_target_fields  = ", ".join(sub_cols)

    nCol = df_clean.shape[1]

    table = tableName
    list_script = []
    chunks = _generate_data(df_clean, chunk_size)
    
    for chunk in tqdm(chunks,
                      total=np.ceil(len(df_clean)/chunk_size)):
        rows = list(chunk.itertuples(index=False, name=None))
        
        string_1 = f""""""
        for index in range(len(rows)):
          row = rows[index]
          string_1 += f""" ( """
          for i in range(nCol-1):
              string_1 +=  normalize_value_sql(row[i]) + ""","""
          string_1 += normalize_value_sql(row[nCol-1]) + """)""" 
          if index!= len(rows) - 1:  string_1 += """, \n"""

        if lookup_col is not None:
            string_2 = f""""""
            count = 0
            for i in range(nCol-1):
                if cols[i] not in lookup_col:
                    count+=1
                    string_2 += f""" {cols[i]}=s.{cols[i]} , """
            if cols[nCol-1] not in lookup_col: 
                count+=1
                string_2 += f""" {cols[nCol-1]}=s.{cols[nCol-1]} """
            if count == 1: string_2 = string_2.replace(",", "")
            
            string_3 = f""""""
            for i in range(len(lookup_col)-1):
                string_3 += f""" t.{lookup_col[i]} = s.{lookup_col[i]} AND """
            string_3 += f""" t.{lookup_col[len(lookup_col)-1]}=s.{lookup_col[len(lookup_col)-1]} """
        else: 
            string_2 = f""""""
            for i in range(nCol-1):
                if i != 0:
                    string_2 += f""" {cols[i]}=s.{cols[i]} , """
            string_2 += f""" {cols[nCol-1]}=s.{cols[nCol-1]} """
            string_3 = f""" t.{cols[0]} = s.{cols[0]} """

        
        sql_script = f""""""
        if indentity_insert:
            sql_script += f""" SET IDENTITY_INSERT {table} ON; 
                            """
        
        sql_script += f"""
                            MERGE INTO { table } AS t
                            USING 
                                (VALUES 
                                    {string_1}  
                                ) AS s{target_fields} 
                            ON  {  string_3 }
                        """
        if update:
            sql_script += f""" WHEN MATCHED THEN
                                UPDATE SET {string_2}  
                        """
        sql_script += f""" WHEN NOT MATCHED THEN
                                INSERT {target_fields}
                                VALUES ({sub_target_fields});
                        """
        if indentity_insert:
            sql_script += f""" SET IDENTITY_INSERT {table} OFF; 
                            """

        list_script.append(sql_script)
    return list_script

def insert_sql(df_clean, tableName, indentity_insert = False, chunk_size =100):
    df_clean.replace({np.nan: None},inplace=True)
    cols = df_clean.columns.tolist()
    target_fields_list = ", ".join(cols)
    target_fields = "({})".format(target_fields_list)

    nCol = df_clean.shape[1]

    table = tableName
    list_script = []
    chunks = _generate_data(df_clean, chunk_size)
    
    for chunk in tqdm(chunks,
                      total=np.ceil(len(df_clean)/chunk_size)):
        rows = list(chunk.itertuples(index=False, name=None))
        
        string_1 = f""""""
        for index in range(len(rows)):
          row = rows[index]
          string_1 += f""" ( """
          for i in range(nCol-1):
              string_1 +=  normalize_value_sql(row[i]) + ""","""
          string_1 += normalize_value_sql(row[nCol-1]) + """)""" 
          if index!= len(rows) - 1:  string_1 += """, \n"""
        
        sql_script = """"""
        if indentity_insert:
            sql_script += f""" SET IDENTITY_INSERT {table} ON; 
                            """
        sql_script += f"""
                        INSERT INTO {table}  
                        {target_fields}
                        VALUES 
                                    {string_1};    
                      """ 
        if indentity_insert:
            sql_script += f""" SET IDENTITY_INSERT {table} OFF; 
                            """
        list_script.append(sql_script)
    return list_script


def truncate_table_sql(table_name):
    sql_script = f"""
                TRUNCATE TABLE { table_name } 
            """
    return sql_script