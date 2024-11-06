import pandas as pd
import os
import sys
import re
from getpass import getuser
#from main import file_save_path_tickets
from typing import Literal
from Entities.apiXrm import ApiXrm
from Entities.dependencies.credenciais import Credential
from Entities.dependencies.logs import Logs, traceback
from Entities.dependencies.config import Config
import multiprocessing
from datetime import datetime
from copy import deepcopy

class SpamFilter:
    @property
    def base_path(self) -> str:
        return self.__base_path
    
    @property
    def df(self) -> pd.DataFrame:
        df = self.__df
        df = df[df['Status'] != "Fechado"]
        df = df[df['Fila'] != 'Spam']
        return df
    
    @property
    def historico(self) -> pd.DataFrame:
        try:
            return self.__historico
        except AttributeError:
            raise AttributeError("é preciso executar o metodo SpamFilter.spam() primeiro para que possa gerar um historico")
    
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
            r"curriculo",
            r"Clique aqui para mais detalhes",
            r"Oferta exclusiva para você",
            r"Promoção imperdível",
            r"Ganhe um desconto especial",
            r"Última chance",
            r"Não perca essa oportunidade",
            r"Confirme sua inscrição",
            r"Aproveite agora",
            r"Grátis por tempo limitado"
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
        df_spam = deepcopy(self.df[self.__tratar_spam(self.df)])
        self.__historico:pd.DataFrame = deepcopy(df_spam[['Número de Referência', 'Fila', 'Status']])
        if retorno == 'DataFrame':
            return df_spam
        elif retorno == 'List_ids':
            return df_spam['Número de Referência'].tolist()
        
        
class AlterTickets(ApiXrm):
    def __init__(self) -> None:
        crd = Credential(Config()['credential']['crd']).load()
        super().__init__(username=crd['user'], password=crd['password'], url=crd["url"])
        
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
    try:
        crd = Credential(Config()['credential']['crd']).load()
        agora = datetime.now()
        multiprocessing.freeze_support()
        filter = SpamFilter(Config()['paths']['file_save_path_tickets'])
        api = AlterTickets()

        lista_ids = filter.spam('List_ids')
        historico:pd.DataFrame = filter.historico
        historico["processou"] = False
        for id in lista_ids:
            endpoint = f"{crd["url"]}crmRestApi/resources/11.13.18.05/serviceRequests/{id}"
            row = historico[historico['Número de Referência'] == id].index[0]
            try:
                result = api.alter(endpoint=endpoint)
                print(result)
                historico.loc[row, 'processou'] = True
            except Exception as error:
                print(id,error)

        path_registros:str = os.path.join(os.getcwd(), 'Registros')
        if not os.path.exists(path_registros):
            os.makedirs(path_registros)
        
        name_file_registro:str = os.path.join(path_registros, datetime.now().strftime('Registro_alteração_filas-%d%m%Y-%H%M%S.xlsx'))
        historico_for_save = historico[historico['processou'] == True]
        if not historico_for_save.empty:
            historico_for_save.to_excel(name_file_registro, index=False)
            lista_movidos:list = historico_for_save['Número de Referência'].tolist()
            Logs().register(status='Concluido', description=f"lista dos chamados que foram movidos para Spam {str(lista_movidos)}")
            sys.exit()
        Logs().register(status='Concluido', description=f"Automação Concluida mas sem chamados movidos para o Spam")
        print(f"{datetime.now() - agora}")
        
    except Exception as err:
        Logs().register(status='Error', description=str(err), exception=traceback.format_exc())
