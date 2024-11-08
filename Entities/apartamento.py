import pandas as pd
import os

class Apartamento:
    def __init__(self, file_path:str) -> None:
        if not os.path.exists(file_path):
            raise Exception(f"o arquivo '{file_path}' não foi encontrado!")
        if not file_path.endswith(".json"):
            raise Exception("somente é aceito arquivos .json")
        
        self.__df:pd.DataFrame = pd.read_json(file_path)
        
    def find(self, *, empreendimento:str, bloco:str, unidade:str) -> int:
        result = self.__df[
            (self.__df['Empreendimento_c'].str.contains(empreendimento, na=False, case=False)) &
            (self.__df['Bloco_c'] == bloco) &
            (self.__df['Unidade_c'] == unidade)
        ]
        
        if len(result) <= 0:
            #raise Exception(f"apartamento [{empreendimento=}; {bloco=}, {unidade=}] não encontrado!")
            return -1
        elif len(result) >= 2:
            raise Exception("mais de 1 apartamento encontrado!")
        
        #print(result['RecordName'].values[0])
        
        return int(result['Id'].values[0])
        