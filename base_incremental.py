# import pandas as pd
# import os
# from getpass import getuser
# from main import Extrat , file_save_path_tickets, calcular_tempo
# from datetime import datetime
# from copy import deepcopy
# from typing import Literal
# import traceback

# file_save_path_tickets_daily = f"C:/Users/{getuser()}/PATRIMAR ENGENHARIA S A/RPA - Documentos/RPA - Dados/XRM - Relacionamento Com Cliente/json/diary_tickets.json"

# class Incremental(Extrat):
#     @property
#     def dateSTR(self) -> str:
#         return self.__dateSTR
    
#     @property
#     def dateToSave(self) -> str:
#         return self.__dateToSave
    
#     @property
#     def df_filtered(self) -> pd.DataFrame:
#         df:pd.DataFrame = deepcopy(self.__df_filtered[self.__df_filtered['Data de Criação'] >= self.dateSTR])
#         df["data_extracao"] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S+00:00')
#         return df
    
#     @property
#     def df_daily(self) -> pd.DataFrame:
#         try:
#             self.__df_daily:pd.DataFrame = self.__df_daily[self.__df_daily['Data de Criação'] < self.dateSTR]
#         except:
#             pass
#         return self.__df_daily
#     @df_daily.setter
#     def df_daily(self, value:pd.DataFrame) -> None:
#         self.__df_daily = value
        
#     @property
#     def df_final(self) -> pd.DataFrame:
#         return self.__df_final
#     @df_final.setter
#     def df_final(self, value: pd.DataFrame) -> None:
#         self.__df_final = value
        
#     @calcular_tempo
#     def __init__(self, *, date:datetime, pathFullBase:str) -> None:
#         super().__init__(username="", password="", url="")
        
#         self.__dateSTR:str = date.strftime("%Y-%m-%d")
#         self.__dateToSave:str = datetime.now().strftime('%Y-%m-%dT%H:%M:%S+00:00')
        
#         if os.path.exists(pathFullBase):
#             self.__df_filtered:pd.DataFrame = pd.read_json(pathFullBase)
#         else:
#             raise FileNotFoundError(f"aquivo não encontrado {pathFullBase=}")
        
#         self.__df_daily:pd.DataFrame = pd.DataFrame()
#         self.__df_final:pd.DataFrame = pd.DataFrame()
        
#     @calcular_tempo
#     def start(self):
#         if os.path.exists(file_save_path_tickets_daily):
#             self.df_daily = pd.read_json(file_save_path_tickets_daily)
            
#         self.df_final = pd.concat([self.df_daily, self.df_filtered], ignore_index=True)
#         return self
    
#     @calcular_tempo
#     def save(self, path:str, *, 
#             orient:Literal['records']|Literal['index']|Literal['columns']|Literal['values']|Literal['table']|Literal['split']='records') -> None:
        
#         if self.df_final.empty:
#             raise ValueError("Dataframe Vazio execute o modulo start primeiro")
#         self.df_final.to_json(path, orient=orient, date_format='iso')
#         self.df_final = pd.DataFrame()
#         return
        

# if __name__ == "__main__":
#     try:
#         date:datetime = datetime.now()
        
#         Incremental(date=date, pathFullBase=file_save_path_tickets).start().save(file_save_path_tickets_daily)
#     except Exception as error:
#         path:str = "logs/"
#         if not os.path.exists(path):
#             os.makedirs(path)
#         file_name = path + f"LogError_{datetime.now().strftime('%d%m%Y%H%M%Y')}.txt"
#         with open(file_name, 'w', encoding='utf-8')as _file:
#             _file.write(traceback.format_exc())
#         raise error
                