from pandas import Series
import numpy as nb
import json
import re
from datetime import datetime

class Phones:
    @property
    def CountryCode(self) -> str:
        if not self.__phone_number:
            return ""
        if (code:=re.search(r'(?<=[+])[\d]{1,3}', self.__phone_number)):
            return code.group()
        return "55"
    
    @property
    def AreaCode(self) -> str:
        if not self.__phone_number:
            return ""
        if (code:=re.search(r'(?<=[(])[\d]+(?=[)])', self.__phone_number)):
            return code.group()
        return ""
    
    @property
    def Number(self) -> str:
        if not self.__phone_number:
            return ""
        if (code:=re.search(r'[\d-]{8,10}', self.__phone_number.replace(' ', ''))):
            return code.group().replace('-', '')
        return ""
    
    def __init__(self, phone_number:str) -> None:
        if phone_number is nb.nan:
            phone_number = ""
        self.__phone_number:str = phone_number
    def __str__(self) -> str:
        return str(self.__phone_number)
    
    def isValid(self) -> bool:
        if (self.CountryCode) and (self.AreaCode) and (self.Number):
            return True
        return False

class Contatos:
    @property
    def FirstName(self) -> str:
        return self.__value['Nome'].split(" ")[0].title()
    
    @property
    def LastName(self) -> str:
        name = self.__value['Nome'].split(" ")
        if len(name) > 1:
            return " ".join(name[1:]).title()
        return ""
    
    @property
    def PersonDEO_Nacionalidade_c(self) -> str:
        nacionalidade = self.__value['Nacionalidade']
        if nacionalidade:
            return nacionalidade
        return "Brasileiro(a)"
    
    @property
    def Gender(self) -> str:
        sexo = self.__value['Sexo']
        if sexo == 'Masculino':
            return "MALE"
        elif sexo == 'Feminino':
            return "FEMALE"
        return ""
    
    @property
    def PersonDEO_EstadoCivil_c(self) -> str:
        estado_civil = self.__value['Estado Civil']
        if estado_civil == 'Solteiro':
            return "PER_ESTADOCIVIL_SOLTEIRO"
        elif estado_civil == 'Casado':
            return "PER_ESTADOCIVIL_CASADO"
        elif estado_civil == 'Divorciado':
            return "PER_ESTADOCIVIL_DIVORCIADO"
        elif estado_civil == 'Viúvo':
            return "PER_ESTADOCIVIL_VIUVO"
        elif estado_civil == 'União Estável':
            return "PER_ESTADOCIVIL_UNIAOESTAVEL"
        elif estado_civil == 'Desquitado':
            return "PER_ESTADOCIVIL_DESQUITADO"
        elif estado_civil == 'Separação Judicial':
            return "PER_ESTADOCIVIL_SEPJUDICIAL"
        return ""
    
    @property
    def PersonDEO_CPF_c(self) -> str:
        cpf_cnpj = self.__value['CPF/ CNPJ']
        return re.sub(r'[\D]+', '', cpf_cnpj)
    
    @property
    def PersonDEO_Identidade_c(self) -> str:
        return self.__value['Identidade']
    
    @property
    def EmailAddress(self) -> str:
        return self.__value['E-mail']
    
    @property
    def DateOfBirth(self) -> str:
        try:
            date = datetime.strptime(self.__value['Data Nascimento'], '%Y-%m-%d %H:%M:%S')
            return date.strftime('%Y-%m-%d')
        except:
            date = datetime.strptime(self.__value['Data Nascimento'], '%Y-%m-%dT%H:%M:%S.%f')
            return date.strftime('%Y-%m-%d')
    
    @property
    def HomePhone(self) -> Phones:
        return Phones(self.__value['Tel. Residêncial'])
    
    @property
    def Mobile(self) -> Phones:
        return Phones(self.__value['Tel. Celular'])
    
    @property
    def WorkPhone(self) -> Phones:
        return Phones(self.__value['Tel. Comercial'])
    
    @property
    def PersonDEO_Tipo_c(self) -> str:
        return "PER_TIPOCONTATO_CLIENTE"
    
    @property
    def PersonDEO_TipoDeEnderecoContato_c(self) -> str:
        tipo_endere:str = self.__value['Tipo do Endereço']
        if tipo_endere == 'Residencial':
            return "PER_TIPODEENDERECO_RES"
        elif tipo_endere == 'Comercial':
            return "PER_TIPODEENDERECO_COM"
        return ""
    
    @property
    def PersonDEO_CEP_c(self) -> str:
        cep:str = self.__value['CEP']
        if not '-' in cep:
            if len(cep) == 8:
                cep = cep[0:5] + "-" + cep[-3:]
        return cep
    
    @property
    def PersonDEO_Logradouro_c(self) -> str:
        return self.__value['Endereço Principal']
    
    @property
    def PersonDEO_Bairro_c(self) -> str:
        return self.__value['Bairro']
    
    @property
    def PersonDEO_Cidade_c(self) -> str:
        return self.__value['Cidade']
    
    @property
    def PersonDEO_Estado_c(self) -> str:
        estado:str = self.__value['Estado']
        if estado == 'Acre':
            return "AC"
        if estado == 'Alagoas':
            return "AL"
        if estado == 'Amapá':
            return "AP"
        if estado == 'Amazonas':
            return "AM"
        if estado == 'Bahia':
            return "BA"
        if estado == 'Ceará':
            return "CE"
        if estado == 'Distrito Federal':
            return "DF"
        if estado == 'Espírito Santo':
            return "ES"
        if estado == 'Goiás':
            return "GO"
        if estado == 'Maranhão':
            return "MA"
        if estado == 'Mato Grosso':
            return "MT"
        if estado == 'Mato Grosso do Sul':
            return "MS"
        if estado == 'Minas Gerais':
            return "MG"
        if estado == 'Pará':
            return "PA"
        if estado == 'Paraíba':
            return "PB"
        if estado == 'Paraná':
            return "PR"
        if estado == 'Pernambuco':
            return "PE"
        if estado == 'Piauí':
            return "PI"
        if estado == 'Rio de Janeiro':
            return "RJ"
        if estado == 'Rio Grande do Norte':
            return "RN"
        if estado == 'Rio Grande do Sul':
            return "RS"
        if estado == 'Rondônia':
            return "RO"
        if estado == 'Roraima':
            return "RR"
        if estado == 'Santa Catarina':
            return "SC"
        if estado == 'São Paulo':
            return "SP"
        if estado == 'Sergipe':
            return "SE"
        if estado == 'Tocantins':
            return "TO"
        return estado.upper()[0:2]
    
    @property
    def PersonDEO_Complemento_c(self) -> str:
        return "C"
    
    @property
    def PersonDEO_Numero_c(self) -> str:
        return self.__value['Número']
    
    @property
    def PersonDEO_Pais_c(self) -> str:
        return "Brasil"
    
    @property
    def PersonDEO_Pais_Id_c(self) -> str:
        return "BR"
    
    @property
    def Empreendimento(self) -> str:
        return self.__value['Empreendimento']
    
    @property
    def Bloco(self) -> str:
        return self.__value['Bloco']

    @property
    def Unidade(self) -> str:
        return self.__value['Unidade']
    
    @property
    def Principal_Comprador(self) -> bool:
        principal = self.__value['Principal (Sim ou Não)']
        if principal == 'Sim':
            return True
        return False

    @property
    def PartyNumber(self) -> str:
        return self.__PartyNumber
    @PartyNumber.setter
    def PartyNumber(self, value:str) -> None:
        self.__PartyNumber = value
    
    def __init__(self, value:Series) -> None:
        self.__value:Series = value
        self.__PartyNumber = ""
    
    def __repr__(self) -> str:
        return F"Contato: {self.__value['Nome']}, CPF/ CNPJ: {self.__value['CPF/ CNPJ']}"
    
    def payload(self) -> dict:
        result = {}
        
        result['FirstName'] = self.FirstName if self.FirstName else None
        result['LastName'] = self.LastName if self.LastName else None
        result['PersonDEO_Nacionalidade_c'] = self.PersonDEO_Nacionalidade_c if self.PersonDEO_Nacionalidade_c else None
        result['Gender'] = self.Gender if self.Gender else None
        result['PersonDEO_EstadoCivil_c'] = self.PersonDEO_EstadoCivil_c if self.PersonDEO_EstadoCivil_c else None
        result['PersonDEO_CPF_c'] = self.PersonDEO_CPF_c if self.FirstName else None
        result['PersonDEO_Identidade_c'] = self.PersonDEO_Identidade_c if self.PersonDEO_Identidade_c else None
        result['EmailAddress'] = self.EmailAddress if self.EmailAddress else None
        result['DateOfBirth'] = self.DateOfBirth if self.DateOfBirth else None
        result['PersonDEO_Tipo_c'] = self.PersonDEO_Tipo_c if self.PersonDEO_Tipo_c else None
        result['PersonDEO_TipoDeEnderecoContato_c'] = self.PersonDEO_TipoDeEnderecoContato_c if self.PersonDEO_TipoDeEnderecoContato_c else None
        result['PersonDEO_CEP_c'] = self.PersonDEO_CEP_c if self.PersonDEO_CEP_c else None
        result['PersonDEO_Logradouro_c'] = self.PersonDEO_Logradouro_c if self.PersonDEO_Logradouro_c else None
        result['PersonDEO_Bairro_c'] = self.PersonDEO_Bairro_c if self.PersonDEO_Bairro_c else None
        result['PersonDEO_Cidade_c'] = self.PersonDEO_Cidade_c if self.PersonDEO_Cidade_c else None
        result['PersonDEO_Estado_c'] = self.PersonDEO_Estado_c if self.PersonDEO_Estado_c else None
        result['PersonDEO_Complemento_c'] = self.PersonDEO_Complemento_c if self.PersonDEO_Complemento_c else None
        result['PersonDEO_Numero_c'] = self.PersonDEO_Numero_c if self.PersonDEO_Numero_c else None
        result['PersonDEO_Pais_c'] = self.PersonDEO_Pais_c if self.PersonDEO_Pais_c else None
        result['PersonDEO_Pais_Id_c'] = self.PersonDEO_Pais_Id_c if self.PersonDEO_Pais_Id_c else None

        if self.HomePhone.isValid():
            result['HomePhoneCountryCode'] = self.HomePhone.CountryCode
            result['HomePhoneAreaCode'] = self.HomePhone.AreaCode
            result['HomePhoneNumber'] = self.HomePhone.Number
        if self.Mobile.isValid():
            result['MobileCountryCode'] = self.Mobile.CountryCode
            result['MobileAreaCode'] = self.Mobile.AreaCode
            result['MobileNumber'] = self.Mobile.Number
        if self.WorkPhone.isValid():
            result['WorkPhoneCountryCode'] = self.WorkPhone.CountryCode
            result['WorkPhoneAreaCode'] = self.WorkPhone.AreaCode
            result['WorkPhoneNumber'] = self.WorkPhone.Number

        return result