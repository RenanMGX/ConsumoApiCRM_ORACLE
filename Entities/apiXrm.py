import multiprocessing.context
import multiprocessing.queues
import requests
import multiprocessing
from time import sleep
from typing import List, Dict, Literal
from datetime import datetime
from crenciais import Credential
import json

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
        if value == "":
            self.__q_param = value
            return
        elif not value.startswith("&q="):
            value = "&q=" + value
        self.__q_param = value
    
    def __init__(self, *, username:str, password:str, url:str="https://fa-etyz-saasfaprod1.fa.ocs.oraclecloud.com/") -> None:
        self.__username:str = username
        self.__password:str = password
        if url.endswith("/"):
            url = url[0:-1]
        self.__url:str = url
        
        self.__q_param:str = ""
        
    def alter(self, *, endpoint:str, new_values:dict):
        headersList = {
        "Content-Type": "application/json" 
        }       
        
        payload = json.dumps(new_values)
        
        response = requests.request("PATCH", endpoint, data=payload, headers=headersList, auth=(self.__username, self.__password))
        if response.status_code == 200:        
            return response.reason
        else:
            raise Exception(f"erro ao consumir api\n{' '*11}{response.status_code=}\n{' '*11}{response.reason=}\n{' '*11}{response.text=}")
        
        
    def request(self, *, offset:int, limit:int=500, endpoint:Literal["tickets", "empreendimentos"] = "tickets"):
        endpoint_url:str
        if endpoint == "tickets":
            endpoint_url = "/crmRestApi/resources/11.13.18.05/serviceRequests"
        elif endpoint == "empreendimentos":
            endpoint_url = "/crmRestApi/resources/11.13.18.05/Empreendimento_c"
            self.q_param = ""

        
        url:str = f"{self.url}{endpoint_url}?onlyData=true&limit={limit}&offset={offset}{self.q_param}"
        
        #print(url)
        for _ in range(3):
            response = requests.request("GET", url,  auth=(self.__username, self.__password))
            if (response.status_code == 200) or (response.status_code != 504):
                break
            print((response.status_code, f"reiniciando {url}"))
            sleep(1)
        #print(url)
        return response
    
    def _inner_request(self, queue:multiprocessing.Queue, offset:int, limit:int=500, endpoint:Literal["tickets"]|Literal['empreendimentos'] = "tickets"):
        response = self.request(offset=offset, limit=limit, endpoint=endpoint)
        #print(response)
        queue.put(response)
    
    def multi_request(self, *,offset:int=0, pages:int=0, limit:int=500, endpoint:Literal["tickets"]|Literal['empreendimentos'] = "tickets", num_threads:int=1):
        list_contents:list = []
        stop_paginate:bool = False
        
        #num_threads:int = multiprocessing.cpu_count() * 5
        
        contador_paginas = 1
        print(f"inicio multi_request {endpoint=}, {num_threads=}")
        while True:
            if (pages != 0) and (contador_paginas >= (pages + 1)):
                break
            else:
                contador_paginas += 1
            if stop_paginate:
                break        
                
            list_process:List[multiprocessing.context.Process] = []
            list_queue:List[multiprocessing.queues.Queue] = [multiprocessing.Queue() for _ in range(num_threads)]
                
            try:
                for num in range(num_threads):
                    list_process.append(multiprocessing.Process(target=self._inner_request, args=(list_queue[num], offset, limit, endpoint)))
                    offset += 500
                    
                print("inicio")    
                for process in list_process:
                    
                    print(str(process), end="; ")
                    process.start()
                    
                print("\n\nFinalizou:")
                for queue_response in list_queue:# type: ignore
                    
                    print(str(queue_response), end="; ")
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
                        stop_paginate = True
                        for process in list_process:
                            process.kill()
                        raise Exception(queue_response.status_code, queue_response.reason)
                    queue_response.close()
                        
            
                print(len(list_contents))
            except Exception as error:
                print("Erro execução Process", type(error), error)
                for process in list_process:
                    process.kill()
                raise Exception(type(error), error)
        
        return list_contents

if __name__ == "__main__":
    import pandas as pd
    print("executado pelo apiXrm.py")
    #exit()
    multiprocessing.freeze_support()
    crd:dict = Credential("XRM_API_PRD").load()
    
    bot = ApiXrm(username=crd["user"], password=crd["password"], url="https://fa-etyz-saasfaprod1.fa.ocs.oraclecloud.com/")
    bot.q_param = "TipoDeFormulario_c!=PER_SVR_FORM_VENDAS_2 or IS NULL"
    
    agora = datetime.now()
    response:dict = bot.request(offset=0).json()
    df = pd.DataFrame(response.get("items"))
    df.to_excel("teste.xlsx", index=False)
    print(datetime.now() - agora)
    
        