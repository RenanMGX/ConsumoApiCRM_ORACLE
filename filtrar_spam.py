import pandas as pd
import os
import re
from getpass import getuser
from main import file_save_path_tickets
from typing import Literal
from Entities.apiXrm import ApiXrm
from Entities.crenciais import Credential
import multiprocessing
from datetime import datetime

class SpamFilter:
    @property
    def base_path(self) -> str:
        return self.__base_path
    
    @property
    def df(self) -> pd.DataFrame:
        df = self.__df[self.__df['Fila'] != 'Spam']
        return df
    
    @property
    def spam_keywords(self) -> list:
        spam_keywords = [
            r"através dele você pode consultar imediatamente o boleto de cota condominial",
            r"que o seu boleto de cota condominial",
            r"Clique aqui para sair da lista de endereçamento",
            r"clicar no botão abaixo ou apontar a câmera do seu celular para o QR Code",
            r"se preferir copie o código de barras a seguir e realize a quitação direto no aplicativo do seu banco",
            r"débito ou crédito",
            r"É simples e rápido fazer a portabilidade do seu número",
            r"para clientes pessoa física no plano indicado",
            r"Faça a contratação do plano e deixe o resto com a gente",
            r"tudo o que sua construtora pode precisar",
            r"caso não queira receber mais e-mails",
            r"quitação direto no aplicativo do seu banco",
            r"Além das opções acima, você ainda pode efetuar o pagamento",
            r"em nosso site",
            r"Intranet do Grupo Patrimar",
            r"curriculo"  
        ]
        return spam_keywords
    
    def __init__(self, path:str) -> None:
        self.__base_path:str = path
        self.__df:pd.DataFrame = pd.read_json(self.base_path, dtype=str) #type: ignore
        
    def __is_spam(self, email_content):
        try:
            # Compilar uma expressão regular que procura qualquer uma das palavras-chave de spam
            spam_pattern = re.compile('|'.join(self.spam_keywords), re.IGNORECASE)
            
            # Verificar se há alguma correspondência no conteúdo do email
            if spam_pattern.search(email_content):
                return True
            else:
                return False
        except:
            return False
        
    def __tratar_spam(self, df:pd.DataFrame) -> pd.Series:
        df = df[['Descrição do Problema', 'Nome do Contato']]
        result = {}
        for row,value in df.iterrows():
            if self.__is_spam(value['Descrição do Problema']):
                result[row] = True
            else:
                result[row] = False
        return pd.Series(result)
    
    def spam(self, retorno:Literal['DataFrame', 'List_ids']) -> pd.DataFrame|list:
        df_spam = self.df[self.__tratar_spam(self.df)]
        if retorno == 'DataFrame':
            return df_spam
        elif retorno == 'List_ids':
            return df_spam['Número de Referência'].tolist()
        
class AlterTickets(ApiXrm):
    def __init__(self) -> None:
        crd = Credential('XRM_API_PRD').load()
        super().__init__(username=crd['user'], password=crd['password'])
        
    def alter(self, *, endpoint: str):
        new_values = {
            "QueueName": "Spam",
            # "StatusCdMeaning": "Cancelado",
            "StatusCd": "PER_SVC_CANCELADO"
            # "StatusTypeCdMeaning": "Resolvido",
            # "StatusTypeCd": "ORA_SVC_RESOLVED"
            }
        return super().alter(endpoint=endpoint, new_values=new_values)     
        
        
if __name__ == "__main__":
    agora = datetime.now()
    multiprocessing.freeze_support()
    filter = SpamFilter(file_save_path_tickets)
    api = AlterTickets()

    for id in filter.spam('List_ids'):
        endpoint = f"https://fa-etyz-saasfaprod1.fa.ocs.oraclecloud.com//crmRestApi/resources/11.13.18.05/serviceRequests/{id}"
        api.alter(endpoint=endpoint)

    print(f"{datetime.now() - agora}")
    #print(api.alter(endpoint=))
