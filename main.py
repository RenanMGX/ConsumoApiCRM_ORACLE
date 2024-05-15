from typing import Literal
from Entities.apiXrm import ApiXrm
from Entities.tratarDados import RelatRelacionementoCliente
from Entities.crenciais import Credential
import multiprocessing
from datetime import datetime
import pandas as pd
from getpass import getuser
from time import sleep
import os
import traceback

file_save_path_tickets:str = f"C:/Users/{getuser()}/PATRIMAR ENGENHARIA S A/RPA - Documentos/RPA - Dados/XRM - Relacionamento Com Cliente/json/all_tickets.json"
file_save_path_empreendimentos:str = f"C:/Users/{getuser()}/PATRIMAR ENGENHARIA S A/RPA - Documentos/RPA - Dados/XRM - Relacionamento Com Cliente/json/empreendimentos.json"

def calcular_tempo(f):
    def wrap(*args, **kwargs):
        agora = datetime.now()
        result = f(*args, **kwargs)
        print(f"tempo de execução do '{f.__name__}' levou {datetime.now() - agora} para ser executado")
        return result
    return wrap

class Extrat(ApiXrm):
    @property
    def df(self):
        return self.__df
    @df.setter
    def df(self, value:pd.DataFrame):
        if not isinstance(value, pd.DataFrame):
            raise TypeError("é permitido apenas DataFrame ou DataSeries")
        self.__df = value
    
    def __init__(self, *, username: str, password: str, url: str) -> None:
        super().__init__(username=username, password=password, url=url)
        
        self.__df:pd.DataFrame = pd.DataFrame()
    
    @calcular_tempo
    def extrair(self, *, 
                offset: int = 0, 
                pages: int = 0, 
                limit: int = 500,
                endpoint:Literal['tickets']|Literal['empreendimentos'] = "tickets", 
                num_threads:int = 1, 
                #attemps:int = 5
                ):
        
        threads_to_consume:int = 30
        error:Exception = Exception("não aconteceu erro mas gerou exceção - analize")
        for _ in range(6):
            if threads_to_consume <= 0:
                threads_to_consume = multiprocessing.cpu_count()
            try:
                self.df = pd.DataFrame(self.multi_request(offset=offset, pages=pages, limit=limit, endpoint=endpoint, num_threads=threads_to_consume))
                return self
            except Exception as erro:
                threads_to_consume -= 5
                error = erro
                sleep(1)
        
        raise error
    
    @calcular_tempo
    def tratar_tickets(self):
        self.df = RelatRelacionementoCliente.tratar(self.df)
        return self
    
    @calcular_tempo
    def salvar(self, *, path:str, 
               orient:Literal['records']|Literal['index']|Literal['columns']|Literal['values']|Literal['table']|Literal['split']='records') -> None:
        if self.df.empty:
            raise ValueError("Dataframe Vazio execute o modulo extrair primeiro")
        self.df.to_json(path, orient=orient)
        self.df = pd.DataFrame()
        return

if __name__ == "__main__":
    try:
        print("executado pelo main.py")
        multiprocessing.freeze_support()
        
        crd:dict = Credential("XRM_API_PRD").load()
        
        api = Extrat(username=crd["user"], password=crd["password"], url="https://fa-etyz-saasfaprod1.fa.ocs.oraclecloud.com/")
        #tickets.q_param = "TipoDeFormulario_c!=PER_SVR_FORM_VENDAS_2 or IS NULL;CreationDate>2024-05-03"
        api.q_param = "TipoDeFormulario_c!=PER_SVR_FORM_VENDAS_2 or IS NULL"
        
        api.extrair(endpoint="tickets", num_threads=30).tratar_tickets().salvar(path=file_save_path_tickets)

        api.extrair(endpoint="empreendimentos").salvar(path=file_save_path_empreendimentos)
    except Exception as error:
        path:str = "logs/"
        if not os.path.exists(path):
            os.makedirs(path)
        file_name = path + f"LogError_{datetime.now().strftime('%d%m%Y%H%M%Y')}.txt"
        with open(file_name, 'w', encoding='utf-8')as _file:
            _file.write(traceback.format_exc())
        raise error
            
    