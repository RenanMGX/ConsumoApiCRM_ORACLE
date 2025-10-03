from typing import Literal
from Entities.apiXrm import ApiXrm
from Entities.tratarDados import RelatRelacionementoCliente
#from Entities.dependencies.config import Config
#from Entities.dependencies.credenciais import Credential
#from Entities.dependencies.logs import Logs, traceback
import multiprocessing
from datetime import datetime
import pandas as pd
from time import sleep
import os

from botcity.maestro import * # type: ignore
maestro = BotMaestroSDK.from_sys_args()
try:
    execution = maestro.get_execution()
except:
    maestro = None

#file_save_path_tickets:str = 
#file_save_path_empreendimentos:str = 

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
                endpoint:Literal['tickets', 'empreendimentos', 'unidades'] = "tickets", 
                num_threads:int = 1, 
                #attemps:int = 5
                ):
        
        threads_to_consume:int = num_threads
        error:Exception = Exception("não aconteceu erro mas gerou exceção - analize")
        for _ in range(3):
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
        self.df.to_json(path, orient=orient, date_format="iso")
        self.df = pd.DataFrame()
        return

if __name__ == "__main__":
    from patrimar_dependencies.sharepointfolder import SharePointFolders
    from patrimar_dependencies.credenciais import Credential
    
    print("executado pelo main.py")
    multiprocessing.freeze_support()
        
    crd:dict = Credential(
        path_raiz=SharePointFolders(r'RPA - Dados\CRD\.patrimar_rpa\credenciais').value,
        name_file="XRM_API_PRD"
    ).load()
        
    api = Extrat(username=crd["user"], password=crd["password"], url=crd["url"])        
        
    api.extrair(endpoint="tickets", num_threads=40).tratar_tickets().salvar(path=os.path.join(SharePointFolders(r'RPA - Dados\XRM - Relacionamento Com Cliente\json').value, 'all_tickets.json'))

    api.extrair(endpoint="empreendimentos").salvar(path=os.path.join(SharePointFolders(r'RPA - Dados\XRM - Relacionamento Com Cliente\json').value, 'empreendimentos.json'))
        
    api.extrair(endpoint="unidades", num_threads=40).salvar(path=os.path.join(SharePointFolders(r'RPA - Dados\XRM - Relacionamento Com Cliente\json').value, 'unidades.json'))
            
    