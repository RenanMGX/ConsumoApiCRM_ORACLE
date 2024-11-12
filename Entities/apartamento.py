import pandas as pd
import os

class Apartamento:
    def __init__(self, *, empreendimento_file_path:str, unidades_oracle_file_path:str) -> None:
        if not os.path.exists(empreendimento_file_path):
            raise Exception(f"o arquivo '{empreendimento_file_path}' não foi encontrado!")
        if not empreendimento_file_path.endswith(".json"):
            raise Exception("somente é aceito arquivos .json")
        
        self.__df:pd.DataFrame = pd.read_json(empreendimento_file_path)
        self.__unidades:pd.DataFrame = pd.read_json(unidades_oracle_file_path)
        
    def find(self, *, empreendimento:str, bloco:str, unidade:str) -> int:
        result = self.__df[
            (self.__df['Nome Do Empreendimento'] == empreendimento) &
            (self.__df['Nome Do Bloco'] == bloco) &
            (self.__df['Código Da Unidade'] == unidade)
        ]['ID Da Unidade']
        
        if len(result) <= 0:
            #raise Exception(f"apartamento [{empreendimento=}; {bloco=}, {unidade=}] não encontrado!")
            return -1
        elif len(result) >= 2:
            raise Exception("mais de 1 apartamento encontrado!")
        
        id_unidade = self.__unidades[
            self.__unidades['IDDaUnidadeNoImobme_c'] == int(result.values[0])
        ]
        
        if len(id_unidade) <= 0:
            #raise Exception(f"apartamento [{empreendimento=}; {bloco=}, {unidade=}] não encontrado!")
            return -1
        elif len(id_unidade) >= 2:
            raise Exception("mais de 1 apartamento encontrado!")
        
        #print(result['RecordName'].values[0])
        
        return int(id_unidade['Id'].values[0])
        