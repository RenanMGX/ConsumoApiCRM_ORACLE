#import os
import pandas as pd
#from datetime import datetime
from time import sleep
#import multiprocessing
from copy import deepcopy

class RelatRelacionementoCliente:
    @staticmethod
    def tratar(df:pd.DataFrame) -> pd.DataFrame:
        columns:list = ["SrNumber" , "QueueName" , "Categoria_c" , "CategoriaDeAssunto_c" , "Title" , "ProblemDescription" , "StatusCdMeaning" , "NomeDoEmpreendimento_c" , "BusinessUnitName" , "ChannelTypeCdMeaning" , "EnviarPesquisa_c" , "PesquisaEnviada_c" , "Avaliacao_c",  "CreationDate" , "LastReopenDate" , "LastResolvedDate" , "DataDaUltimaAlteracaoDeStatus_c" , "LastUpdateDate" , "ResolvedBy" , "PrimaryContactPartyUniqueName" , "PrimaryContactEmailAddress" , "PrimaryContactFormattedPhoneNumber" , "TipoDeFormulario_c" , "DataDaVisita_c" , "CompareceuAVisita_c" , "AdquiriuPersonalizacao_c" , "AdquiriuModificacao_c" , "DataDaVistoria_c" , "StatusDaVistoria_c" , "DataDaRevistoria_c" , "StatusDaRevistoria_c" , "DataDeEntregaDasChaves_c" , "ChavesEntregues_c" , "HorarioDoAgendamento_c" , "Patologia_c" , "DataDaSolucao_c" , "Procedencia_c" , "TipoDeEntrada_c" , "MotivoDoPendente_c" , "Transbordo_c" , "Unidade_c", "Causa_c", "SubCausaN1_c", "SubCausaN2_c", "Prazo_c"]
        
        df = df[columns]
        
        df = df.rename(columns={
            "SrNumber" : "Número de Referência",
            "QueueName" : "Fila",
            "Categoria_c" : "Categoria",
            "CategoriaDeAssunto_c" : "Assunto",
            "Title" : "Título",
            "ProblemDescription" : "Descrição do Problema",
            "StatusCdMeaning" : "Status",
            "NomeDoEmpreendimento_c" : "Nome do Empreendimento",
            "BusinessUnitName" : "Marca do Empreendimento",
            "ChannelTypeCdMeaning" : "Canal",
            "EnviarPesquisa_c" : "Enviar Pesquisa?",
            "PesquisaEnviada_c" : "Pesquisa Enviada?",
            "Avaliacao_c" : "Avaliação",
            "CreationDate" : "Data de Criação",
            "LastReopenDate" : "Data de Reabertura",
            "LastResolvedDate" : "Data de Resolução",
            "DataDaUltimaAlteracaoDeStatus_c" : "Data da Última Alteração de Status",
            "LastUpdateDate" : "Data da Última Atualização",
            "ResolvedBy" : "Resolvido Por",
            "PrimaryContactPartyUniqueName" : "Nome do Contato",
            "PrimaryContactEmailAddress" : "Email",
            "PrimaryContactFormattedPhoneNumber" : "Telefone do Contato",
            "TipoDeFormulario_c" : "Tipo de Formulário",
            "DataDaVisita_c" : "Data da Visita",
            "CompareceuAVisita_c" : "Compareceu à Visita",
            "AdquiriuPersonalizacao_c" : "Adquiriu Personalização",
            "AdquiriuModificacao_c" : "Adquiriu Modificação",
            "DataDaVistoria_c" : "Data da Vistoria",
            "StatusDaVistoria_c" : "Status da Vistoria",
            "DataDaRevistoria_c" : "Data da Revistoria",
            "StatusDaRevistoria_c" : "Status da Revistoria",
            "DataDeEntregaDasChaves_c" : "Data de Entrega das Chaves",
            "ChavesEntregues_c" : "Chaves Entregues",
            "HorarioDoAgendamento_c" : "Data do Agendamento",
            "Patologia_c" : "Patologia",
            "DataDaSolucao_c" : "Data da Solução",
            "Procedencia_c" : "Procedência",
            "TipoDeEntrada_c" : "Tipo de Entrada",
            "MotivoDoPendente_c" : "Motivo do Pendente",
            "Transbordo_c" : "Transbordo",
            "Unidade_c" : "Unidade_c",
            "Causa_c" : "Causa", 
            "SubCausaN1_c" : "SubCausaN1", 
            "SubCausaN2_c" : "SubCausaN2",
            "Prazo_c" : "Prazo"
            })
        
        for row,value in df.iterrows():
            try:
                dados_endereco = value["Unidade_c"].split(", ")
                df.loc[row, "Bloco da Unidade"] = dados_endereco[1]# type: ignore
                df.loc[row, "Número da Unidade"] = dados_endereco[2] # type: ignore

            except:
                continue
        
        df = RelatRelacionementoCliente.tratarLinhas(df)
   
        return df
    
    @staticmethod
    def tratarLinhas(df:pd.DataFrame) -> pd.DataFrame:
        assunto:dict = {
            'PER_SVR_CATEGORIA_PROJ' : "Projeto",
            'PER_SVR_CATEGORIA_REVINT' : "Revestimento Interno",
            'PER_SVR_CATEGORIA_INSTHIDR' : "Instalação Hidráulica",
            'PER_SVR_CATEGORIA_FIDELIZAC' : "Fidelização",
            'PER_SVR_CATEGORIA_ESTRUTURAL' : "Estrutural",
            'PER_SVR_CATEGORIA_ESQMAD' : "Esquadria de Madeira",
            'PER_SVR_CATEGORIA_OUTROSASSUNT' : "Outros assuntos",
            'PER_SVR_CATEGORIA_CONTESTVAL' : "Contestação de Valores",
            'PER_SVR_CATEGORIA_VISTCLIENTE' : "Vistoria Cliente",
            'PER_SVR_CATEGORIA_SEGVIABOLETO' : "Segunda via de boleto",
            'PER_SVR_CATEGORIA_ASSTECSS' : "Assistência Técnica",
            'PER_SVR_CATEGORIA_INSTELETR' : "Instalação Elétrica",
            'PER_SVR_CATEGORIA_ENTRCHAVES' : "Entrega de chaves",
            'PER_SVR_CATEGORIA_ACABINT' : "Acabamento Interno",
            'PER_SVR_CATEGORIA_ACESSORIOS' : "Acessórios",
            'PER_SVR_CATEGORIA_DADOSCADAST' : "Dados cadastrais",
            'PER_SVR_CATEGORIA_VISITA' : "Visita",
            'PER_SVR_CATEGORIA_FICHAFINAN' : "Ficha Financeira",
            'PER_SVR_CATEGORIA_PINTURAINT' : "Pintura Interna",
            'PER_SVR_CATEGORIA_NEGOCPARC' : "Negociação de parcelas",
            'PER_SVR_CATEGORIA_MODIF' : "Modificação",
            'PER_SVR_CATEGORIA_CESSDIROBRIG' : "Cessão de Direitos",
            'PER_SVR_CATEGORIA_INSTTELECOM' : "Instalação Telecomunicação",
            'PER_SVR_CATEGORIA_PERSONALIZ' : "Personalização",
            'PER_SVR_CATEGORIA_QUITIMOVEL' : "Quitação Imovel",
            'PER_SVR_CATEGORIA_ACABEXT' : "Acabamento Externo",
            'PER_SVR_CATEGORIA_ACABINT' : "Acabamento Interno",
            'PER_SVR_CATEGORIA_ACAOJUDIC' : "Ação Juridica",
            'PER_SVR_CATEGORIA_ACESSORIOS' : "Acessórios",
            'PER_SVR_CATEGORIA_AGUA' : "Agua",
            'PER_SVR_CATEGORIA_ALTERVENCIM' : "Alteração de Vencimento",
            'PER_SVR_CATEGORIA_AMORTPARC' : "Amortização de parcelas",
            'PER_SVR_CATEGORIA_ANTECPARC' : "Antecipação de parcelas" ,
            'PER_SVR_CATEGORIA_APLICATIVO' : "Aplicativo" ,
            'PER_SVR_CATEGORIA_AVULSO' : "Avulso" ,
            'PER_SVR_CATEGORIA_BOLETINDISP' : "Portal do Cliente" ,
            #'PER_SVR_CATEGORIA_BONIF' : "PER_SVR_CATEGORIA_BONIF" ,
            'PER_SVR_CATEGORIA_CADEMPREEND' : "Cadastro de empreendimento" ,
            #'PER_SVR_CATEGORIA_CND' : "Antecipação" ,
            'PER_SVR_CATEGORIA_COMPRPAGAM' : "Comprovante de pagamento" ,
            'PER_SVR_CATEGORIA_COMUNICADOS' : "Comunicados" ,
            'PER_SVR_CATEGORIA_CONDOM' : "Condomínio" ,                          
            'PER_SVR_CATEGORIA_CONTATOCBI' : "Contato do CBI" ,
            'PER_SVR_CATEGORIA_CONVCOND' : "Convenção condomínio" ,
            'PER_SVR_CATEGORIA_COPIACHAVES' : "Cópia de chaves" ,
            'PER_SVR_CATEGORIA_CORREIOS' : "PER_SVR_CATEGORIA_CORREIOS" ,
            'PER_SVR_CATEGORIA_CORRINCC' : "Correção INCC" ,
            'PER_SVR_CATEGORIA_CUSTOMIZACAO' : "Customização" ,
            'PER_SVR_CATEGORIA_DECORADO' : "Decorado" ,
            'PER_SVR_CATEGORIA_DISTBILAT' : "Distrato Bilateral " ,
            'PER_SVR_CATEGORIA_DISTRATO' : "Distrato" ,
            'PER_SVR_CATEGORIA_DISTUNILAT' : "Distrato Unilateral" ,
            'PER_SVR_CATEGORIA_DUVFICHAFIN' : "Dúvida de posição financeira" ,
            'PER_SVR_CATEGORIA_ENERGIA' : "Energia" ,
            'PER_SVR_CATEGORIA_ENTREMPREEND' : "Empreendimento" ,
            'PER_SVR_CATEGORIA_ENXNOTFISC' : "Enxoval" ,
            'PER_SVR_CATEGORIA_ERRODEACESS' : "Erro de acesso",
            'PER_SVR_CATEGORIA_BONIF' : "BONIF",
            'PER_SVR_CATEGORIA_CND' : "CND",
            'PER_SVR_CATEGORIA_CORREIOS' : "CORREIOS",
            'PER_SVR_CATEGORIA_ESPECIF' : "ESPECIF",
            'PER_SVR_CATEGORIA_ESQALUM' : "ESQALUM",
            'PER_SVR_CATEGORIA_ESTOQUE' : "ESTOQUE",
            'PER_SVR_CATEGORIA_FORNECEDOR' : "FORNECEDOR",
            'PER_SVR_CATEGORIA_HABITESE' : "HABITESE",
            'PER_SVR_CATEGORIA_INADIMPL' : "INADIMPL",
            'PER_SVR_CATEGORIA_INCORP' : "INCORP",
            'PER_SVR_CATEGORIA_INDCADAST' : "INDCADAST",
            'PER_SVR_CATEGORIA_INDICESCORR' : "INDICESCORR",
            'PER_SVR_CATEGORIA_INFORMEREND' : "INFORMEREND",
            'PER_SVR_CATEGORIA_INSTGAS' : "INSTGAS",
            'PER_SVR_CATEGORIA_IPTU' : "IPTU",
            'PER_SVR_CATEGORIA_ITBIREG' : "ITBIREG",
            'PER_SVR_CATEGORIA_KIT' : "KIT",
            'PER_SVR_CATEGORIA_LIMPEZA' : "LIMPEZA",
            'PER_SVR_CATEGORIA_LOUCAS' : "LOUCAS",
            'PER_SVR_CATEGORIA_MANUALPROP' : "MANUALPROP",
            'PER_SVR_CATEGORIA_MANUTENÇÂO' : "MANUTENÇÂO",
            'PER_SVR_CATEGORIA_MARCEARMARIO' : "MARCEARMARIO",
            'PER_SVR_CATEGORIA_MEMDESCRIT' : "MEMDESCRIT",
            'PER_SVR_CATEGORIA_METAIS' : "METAIS",
            'PER_SVR_CATEGORIA_MIDSOCIAIS' : "MIDSOCIAIS",
            'PER_SVR_CATEGORIA_MINUTACOMERC' : "MINUTACOMERC",
            'PER_SVR_CATEGORIA_NEGOCDIF' : "NEGOCDIF",
            'PER_SVR_CATEGORIA_NOTIFEXTJUD' : "NOTIFEXTJUD",
            'PER_SVR_CATEGORIA_OUTORGAESCR' : "OUTORGAESCR",
            'PER_SVR_CATEGORIA_PAGVMD' : "PAGVMD",
            'PER_SVR_CATEGORIA_PERMUTA' : "PERMUTA",
            'PER_SVR_CATEGORIA_PINTURAEXT' : "PINTURAEXT",
            'PER_SVR_CATEGORIA_PREMIACOES' : "PREMIACOES",
            'PER_SVR_CATEGORIA_PROMOCOES' : "PROMOCOES",
            'PER_SVR_CATEGORIA_PRORROGRESE' : "PRORROGRESE",
            'PER_SVR_CATEGORIA_RECLAMEAQUI' : "RECLAMEAQUI",
            'PER_SVR_CATEGORIA_REFAVUL' : "REFAVUL",
            'PER_SVR_CATEGORIA_REGBAIXHIPO' : "REGBAIXHIPO",
            'PER_SVR_CATEGORIA_REGISTROATA' : "REGISTROATA",
            'PER_SVR_CATEGORIA_RELVALPAGOS' : "RELVALPAGOS",
            'PER_SVR_CATEGORIA_REPAROSAPT' : "REPAROSAPT",
            'PER_SVR_CATEGORIA_REVEXT' : "REVEXT",
            'PER_SVR_CATEGORIA_STAND' : "STAND",
            'PER_SVR_CATEGORIA_STATREPASSE' : "STATREPASSE",
            'PER_SVR_CATEGORIA_TAXAINCENDIO' : "TAXAINCENDIO",
            'PER_SVR_CATEGORIA_TERMOQUIT' : "TERMOQUIT",
            'PER_SVR_CATEGORIA_VALORFASEOBR' : "VALORFASEOBR",
            'PER_SVR_CATEGORIA_VIDROS' : "VIDROS",
            'PER_SVR_CATEGORIA_VISTOBRA' : "VISTOBRA",
            'PER_SVR_CATEGORIA_VISTSAT' : "VISTSAT",
            'PER_SVR_CATEGORIA_VISTTEC' : "VISTTEC",
            'PER_SVR_CATEGORIA_VIZINHO' : "VIZINHO",
            'PER_SVR_PESQUISANPS' : "PESQUISANPS"
        }   
        categoria:dict = {
            "LISTA_CATEGORIA_ADMIN" : "Administrativo",
            "LISTA_CATEGORIA_ASSISTEC" : "Assistência Técnica",
            "LISTA_CATEGORIA_COBRRECEB" : "COBRRECEB",
            "LISTA_CATEGORIA_CONTRATOS" : "CONTRATOS",
            "LISTA_CATEGORIA_EMPREENDIM" : "Empreendimento",
            "LISTA_CATEGORIA_JURIDICO" : "JURIDICO",
            "LISTA_CATEGORIA_RELACCLIENTE" : "Relacionamento com Cliente",
        }
        
        sim_nao:dict = {
            "SIM_NAO_N" : "Não",
            "SIM_NAO_S" : "Sim"
        }
        tipo_formulario:dict = {
            "PER_SVR_FORM_ASSIS" : "Assistência Técnica",
            "PER_SVR_FORM_CHAVE" : "Entrega de Chaves",
            "PER_SVR_FORM_MODIF" : "Entrega de Chaves",
            "PER_SVR_FORM_PADRA" : "Padrão",
            "PER_SVR_FORM_PERSO" : "Personalização",
            "PER_SVR_FORM_VISIT" : "Visita",
            "PER_SVR_FORM_VISTO" : "Vistoria"            
        }
        
        status_vistoria:dict = {
            "PER_SVR_STVISTORIA_APROV" : "Aprovado",
            "PER_SVR_STVISTORIA_REPROV" : "Reprovado", 
            "PER_SVR_STREVISTORIA_APROV" : "Aprovado",
            "PER_SVR_STREVISTORIA_REPROV" : "Reprovado",      
        }
        
        tipo_entrada:dict = {
            "PER_TIPOENTR_SIST" : "Cadastro via sistema",
            "PER_TIPOENTR_WAPP" : "Integração via Whatsapp",
            "PER_TIPOENTR_CHAT" : "CHAT",
            "PER_TIPOENTR_FORMONL" : "FORMONL",
            "PER_TIPOENTR_IMPORT" : "IMPORT",
            "PER_TIPOENTR_STANDPDV" : "STANDPDV",
            "PER_TIPOENTR_TELEFONE" : "TELEFONE",            
        }
        
        procedencia:dict = {
            "PER_SRV_PROCED_IMPROCED" : "Improcedente",
            "PER_SRV_PROCED_PROCED" : "Procedente",
            "PER_SRV_PROCED_SACSAT" : "SAC/SAT"
        }
        
        avaliacao:dict = {
            "PER_PESQSATISFACAO_INDIF" : "Indiferente",
            "PER_PESQSATISFACAO_INSATISF" : "Insastisfeito(a)",
            "PER_PESQSATISFACAO_MINSATISF" : "Satisfação Minima",
            "PER_PESQSATISFACAO_MSATISF" : "Satisfação Maxima",
            "PER_PESQSATISFACAO_SATISF" : "Satisfeito(a)"
        }
        
        patologia:dict = {
            "PER_SRV_PATOLOGIA_ACESSORIOS" : "Acessórios",
            "PER_SRV_PATOLOGIA_ELETRICA" : "Instalação Elétrica",
            "PER_SRV_PATOLOGIA_ESQUADMADEIR" : "Esquadria de Madeira",
            "PER_SRV_PATOLOGIA_ESQUADRIA" : "Esquadria de Alumínio ou Metálica",
            "PER_SRV_PATOLOGIA_ESTRUTURAL" : "Estrutural",
            "PER_SRV_PATOLOGIA_FACHADA" : "Fachada",
            "PER_SRV_PATOLOGIA_HIDRO_A" : "Instalação Hidráulica",
            "PER_SRV_PATOLOGIA_INSTELETMODU" : "INSTELETMODU",
            "PER_SRV_PATOLOGIA_INSTHIDTORN" : "Instalação Hidráulica - Torneira / Ducha / Acabamento",
            "PER_SRV_PATOLOGIA_INSTHIDFORRO" : "Instalação Hidráulica - Forros / Paredes / Lajes manchados",
            "PER_SRV_PATOLOGIA_INSTHIDENT" : "Instalação Hidráulica – Entupimento",
            "PER_SRV_PATOLOGIA_INSTELETQUAD" : "Instalação Elétrica – Quadro",
            "PER_SRV_PATOLOGIA_INSTHIDLOUCA" : "Instalação Hidráulica - Louça / Bojo / Tanque / Caixa acoplada / Vaso sanitário",
            "PER_SRV_PATOLOGIA_INSTTELECOM" : "INSTTELECOM",
            "PER_SRV_PATOLOGIA_PISO" : "Piso de Madeira / Laminado / Vinílico",
            "PER_SRV_PATOLOGIA_REFORMAAVUL" : "REFORMAAVUL",    
            "PER_SRV_PATOLOGIA_REVEST" : "Revestimento Interno",
            "PER_SRV_PATOLOGIA_REVESTEXT" : "Revestimento Externo - Exceto fachada",               
        }
        
        df["Assunto"] = df["Assunto"].map(assunto)
        df["Categoria"] = df["Categoria"].map(categoria)
        df["Enviar Pesquisa?"] = df["Enviar Pesquisa?"].map(sim_nao)
        df["Chaves Entregues"] = df["Chaves Entregues"].map(sim_nao)
        df["Adquiriu Modificação"] = df["Adquiriu Modificação"].map(sim_nao)
        df["Adquiriu Personalização"] = df["Adquiriu Personalização"].map(sim_nao)
        df["Compareceu à Visita"] = df["Compareceu à Visita"].map(sim_nao)
        df["Transbordo"] = df["Transbordo"].map(sim_nao)
        df["Tipo de Formulário"] = df["Tipo de Formulário"].map(tipo_formulario)
        df["Status da Vistoria"] = df["Status da Vistoria"].map(status_vistoria)
        df["Status da Revistoria"] = df["Status da Revistoria"].map(status_vistoria)
        df["Tipo de Entrada"] = df["Tipo de Entrada"].map(tipo_entrada)
        df["Avaliação"] = df["Avaliação"].map(avaliacao)
        df["Procedência"] = df["Procedência"].map(procedencia)
        df["Patologia"] = df["Patologia"].map(patologia)
        
        return df

if __name__ == "__main__":
    print("executado pelo tratarDados.py")
    exit()
    df = pd.read_excel("#material/11.13.18.05_tickets.xlsx")
    
    print(RelatRelacionementoCliente.tratar(df))
    

    