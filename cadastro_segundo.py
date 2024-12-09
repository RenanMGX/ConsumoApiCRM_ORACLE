from Entities.apiXrm import ApiXrm
from Entities.dependencies.config import Config
from Entities.dependencies.credenciais import Credential
import pandas as pd
from Entities.contatos import Contatos
from Entities.apartamento import Apartamento
from Entities.dependencies.arguments import Arguments
from datetime import datetime
from dateutil.relativedelta import relativedelta
from Entities.dependencies.logs import Logs, traceback
from Entities.dependencies.functions import P
import os
import json
from typing import List, Dict
import sys

class RegistroCPFCadastrados:
    file_path = os.path.join(os.getcwd(), 'lista_cpf_cadastrados.json')
    if not os.path.exists(file_path):
        with open(file_path, 'w')as _file:
            json.dump([], _file)

    @staticmethod
    def load() -> list:
        with open(RegistroCPFCadastrados.file_path, 'r')as _file:
            return list(set(json.load(_file)))

    @staticmethod
    def add(value) -> None:
        reg = RegistroCPFCadastrados.load()
        reg.append(value)
        with open(RegistroCPFCadastrados.file_path, 'w')as _file:
            json.dump(list(set(reg)), _file)

class RangeDataExecute:
    @property
    def rules(self) -> Dict[datetime,Dict[str,datetime]]:
        return {

            datetime(2024,11,13): {"min": datetime(2023,7,1), "max": datetime(2023,12,31)},
            datetime(2024,11,14): {"min": datetime(2023,1,1), "max": datetime(2023,6,30)},
            datetime(2024,11,15): {"min": datetime(2022,7,1), "max": datetime(2022,12,31)},
            datetime(2024,11,16): {"min": datetime(2022,1,1), "max": datetime(2022,6,30)},
            datetime(2024,11,17): {"min": datetime(2021,7,1), "max": datetime(2021,12,31)},
            datetime(2024,11,18): {"min": datetime(2021,1,1), "max": datetime(2021,6,30)},
            datetime(2024,11,19): {"min": datetime(2020,7,1), "max": datetime(2020,12,31)},
            datetime(2024,11,20): {"min": datetime(2020,1,1), "max": datetime(2020,6,30)},
            datetime(2024,11,21): {"min": datetime(2019,7,1), "max": datetime(2019,12,31)},
            datetime(2024,11,22): {"min": datetime(2019,1,1), "max": datetime(2019,6,30)}
        }

    @property
    def max(self) -> datetime|None:
        # if (_max:=self.rules.get(self.__date)):
        #     return _max.get('max')
        return datetime.now()

    @property
    def min(self) -> datetime|None:
        # if (_min:=self.rules.get(self.__date)):
        #     return _min.get('min')
        return (datetime.now() - relativedelta(weeks=1))

    def __init__(self) -> None:
        now = datetime.now()
        self.__date:datetime = datetime(now.year, now.month, now.day)



class Execute:
    @staticmethod
    def start():
        rg_date = RangeDataExecute()
        if not rg_date.max:
            print("Data indisponivel para execução")
            sys.exit()

        crd:dict = Credential(Config()['credential']['crd']).load()
        api = ApiXrm(username=crd["user"], password=crd["password"], url=crd["url"])
        apartamento = Apartamento(
            empreendimento_file_path=Config()['paths']['file_save_path_empreendimentos_imobme'],
            unidades_oracle_file_path=Config()['paths']['file_save_path_unidades']
        )

        df = pd.read_json(Config()['paths']['file_save_clientes'])
        df['Data Criação'] = pd.to_datetime(df['Data Criação'], errors='coerce', format='%Y-%m-%dT%H:%M:%S.%f')
        df = df[
            (df['Data Criação'] >= rg_date.min) &
            (df['Data Criação'] <= rg_date.max) &
            (df['Principal (Sim ou Não)'] == "Não") &
            (~df['CPF/ CNPJ'].isin(RegistroCPFCadastrados.load()))
        ]
        #import pdb;pdb.set_trace()
        num_cadastros:int = 0
        for row, value in df.iterrows():
            try:
                contato = Contatos(value)

                apartamento_id = apartamento.find(
                    empreendimento=contato.Empreendimento,
                    bloco=contato.Bloco,
                    unidade=contato.Unidade
                )

                result = api.cadastrar_contatos(contato.payload())
                if not result.status_code == 201:
                    if "Já existe um contato com o CPF inserido".lower() in result.text.lower():
                        #print(P(f"{contato} já esta cadastrado", color='red'))
                        RegistroCPFCadastrados.add(str(value['CPF/ CNPJ']))
                        continue
                    raise Exception(f"erro ao cadastrar contato: {result.status_code=}; {result.reason}; {result.text}")
                else:
                    #print(f"cadastrado:{result.json().get("PartyNumber")}")
                    print(P(f"{contato};{result.json().get("PartyNumber")} -> cadastrado!", color='green'))

                data:dict = result.json()
                contato.PartyNumber = data['PartyNumber']

                if apartamento_id != -1:
                    result = api.registrar_unidade(
                        PartyNumber=contato.PartyNumber,
                        apartamento_id=apartamento_id,
                        principal_comprador=contato.Principal_Comprador
                    )

                    if not result.status_code == 201:
                        raise Exception(f"erro ao registrar unidade para o {contato=}: {result.status_code=}; {result.reason}; {result.text}")

                RegistroCPFCadastrados.add(str(value['CPF/ CNPJ']))

                num_cadastros += 1

            except Exception as err:
                print(P((type(err), str(err))))
                Logs(name="Cadastro Segundo Preponente").register(status='Error', description=f"cpf:{df['CPF/ CNPJ']} - {str(err)}", exception=traceback.format_exc())
                continue

        Logs(name="Cadastro Segundo Preponente").register(status='Concluido', description=f"Automação Finalizada! '{num_cadastros}' contados registrados!")




if __name__ == "__main__":
    Arguments({
        'start' : Execute.start
    })