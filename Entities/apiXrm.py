import multiprocessing.context
import multiprocessing.queues
import requests
import multiprocessing
from time import sleep
from typing import List, Dict
from datetime import datetime
from crenciais import Credential

#https://fa-etyz-saasfaprod1.fa.ocs.oraclecloud.com/crmRestApi/resources/11.13.18.05/serviceRequests?offset={offset}&limit=500&q=TipoDeFormulario_c!=PER_SVR_FORM_VENDAS_2 or IS NULL"
class ApiXrm:
    @property
    def url(self):
        return self.__url
    @property
    def q_param(self):
        return self.__q_param
    @q_param.setter
    def q_param(self, value:str):
        if not value.startswith("&q="):
            value = "&q=" + value
        self.__q_param = value
    
    def __init__(self, *, username:str, password:str, url:str) -> None:
        self.__username:str = username
        self.__password:str = password
        if url.endswith("/"):
            url = url[0:-1]
        self.__url:str = url
        
        self.__q_param:str = ""
        
        
    def request(self, *, offset:int, limit:int=500):
        url:str = f"{self.url}/crmRestApi/resources/11.13.18.05/serviceRequests?limit={limit}&offset={offset}{self.q_param}"
        #print(url)
        response = requests.request("GET", url,  auth=(self.__username, self.__password))
        print(url)
        return response
    
    def _inner_request(self, queue:multiprocessing.Queue, offset:int, limit:int=500):
        response = self.request(offset=offset, limit=limit)
        #print(response)
        queue.put(response)
    
    def multi_request(self, offset:int=0, pages:int=0, limit:int=500):
        list_contents:list = []
        stop_paginate:bool = False
        
        num_threads:int = multiprocessing.cpu_count() * 4
        
        contador_paginas = 1
        while True:
            if (pages != 0) and (contador_paginas >= (pages + 1)):
                break
            else:
                contador_paginas += 1
            if stop_paginate:
                break        
            
            list_process:List[multiprocessing.context.Process] = []
            list_queue:List[multiprocessing.queues.Queue] = [multiprocessing.Queue() for _ in range(num_threads)]
            
            for num in range(num_threads):
                list_process.append(multiprocessing.Process(target=self._inner_request, args=(list_queue[num], offset, limit)))
                offset += 500
            
            
            for process in list_process:
                process.start()
            
            for queue_response in list_queue:# type: ignore
                queue_response:requests.models.Response = queue_response.get() # type: ignore
                if queue_response.status_code == 200:
                    queue_json:dict = queue_response.json()
                    
                    if queue_json.get("count") > 0:# type: ignore
                        list_contents += queue_json.get("items")# type: ignore
                    else:
                        stop_paginate = True
                else:
                    print(type(queue_response))
                    print(queue_response.status_code, queue_response.reason)
                    break
        
            print(len(list_contents))
        
        return list_contents

if __name__ == "__main__":
    multiprocessing.freeze_support()
    crd:dict = Credential("XRM_API_PRD").load()
    
    bot = ApiXrm(username=crd["user"], password=crd["password"], url="https://fa-etyz-saasfaprod1.fa.ocs.oraclecloud.com/")
    bot.q_param = "TipoDeFormulario_c!=PER_SVR_FORM_VENDAS_2 or IS NULL"
    
    agora = datetime.now()
    bot.multi_request()
    print(datetime.now() - agora)
    
        