#import os
import pandas as pd
#from datetime import datetime
from time import sleep
#import multiprocessing
from copy import deepcopy
import numpy as nb

class RelatRelacionementoCliente:
    @staticmethod
    def tratar(df:pd.DataFrame) -> pd.DataFrame:
        columns:list = ["SrNumber" , "QueueName" , "Categoria_c" , "CategoriaDeAssunto_c" , "Title" , "ProblemDescription" , "StatusCdMeaning" , "NomeDoEmpreendimento_c" , "BusinessUnitName" , "ChannelTypeCdMeaning" , "EnviarPesquisa_c" , "PesquisaEnviada_c" , "Avaliacao_c",  "CreationDate" , "LastReopenDate" , "LastResolvedDate" , "DataDaUltimaAlteracaoDeStatus_c" , "LastUpdateDate" , "ResolvedBy" , "PrimaryContactPartyUniqueName" , "PrimaryContactEmailAddress" , "PrimaryContactFormattedPhoneNumber" , "TipoDeFormulario_c" , "DataDaVisita_c" , "CompareceuAVisita_c" , "AdquiriuPersonalizacao_c" , "AdquiriuModificacao_c" , "DataDaVistoria_c" , "StatusDaVistoria_c" , "DataDaRevistoria_c" , "StatusDaRevistoria_c" , "DataDeEntregaDasChaves_c" , "ChavesEntregues_c" , "HorarioDoAgendamento_c" , "Patologia_c" , "DataDaSolucao_c" , "Procedencia_c" , "TipoDeEntrada_c" , "MotivoDoPendente_c" , "Transbordo_c" , "Unidade_c", "Causa_c", "SubCausaN1_c", "SubCausaN2_c", "Prazo_c", "TipoDeRegistro_c"]
        
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
            "Prazo_c" : "Prazo",
            "TipoDeRegistro_c": "Tipo De Registro"
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
            'PER_SVR_CATEGORIA_FIDELIZAC' : "Boas Vindas",
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
            'PER_SVR_CATEGORIA_ESQALUM' : "Esquadria de Alumínio",
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
            'PER_SVR_CATEGORIA_NOTIFEXTJUD' : "Notificação extrajudicial",
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
        
        status:dict = {
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
            'PER_SRV_PATOLOGIA_APARTAVULSO' : "Apartamento Avulso",
            'PER_SRV_PATOLOGIA_ARMARIOS' : "Armarios",
            "PER_SRV_PATOLOGIA_ELETRICA" : "Instalação Elétrica",
            "PER_SRV_PATOLOGIA_ESQUADMADEIR" : "Esquadria de Madeira",
            "PER_SRV_PATOLOGIA_ESQUADRIA" : "Esquadria de Alumínio ou Metálica",
            "PER_SRV_PATOLOGIA_ESTRUTURAL" : "Estrutural",
            "PER_SRV_PATOLOGIA_FACHADA" : "Fachada",
            "PER_SRV_PATOLOGIA_HIDRO_A" : "Instalação Hidráulica",
            "PER_SRV_PATOLOGIA_INSTELETMODU" : "Instalação Elétrica - Módulo",
            "PER_SRV_PATOLOGIA_INSTHIDTORN" : "Instalação Hidráulica - Torneira / Ducha / Acabamento",
            "PER_SRV_PATOLOGIA_INSTHIDFORRO" : "Instalação Hidráulica - Forros / Paredes / Lajes manchados",
            "PER_SRV_PATOLOGIA_INSTHIDENT" : "Instalação Hidráulica – Entupimento",
            "PER_SRV_PATOLOGIA_INSTELETQUAD" : "Instalação Elétrica – Quadro",
            "PER_SRV_PATOLOGIA_INSTHIDLOUCA" : "Instalação Hidráulica - Louça / Bojo / Tanque / Caixa acoplada / Vaso sanitário",
            "PER_SRV_PATOLOGIA_INSTTELECOM" : "Instalação Telecomunicações",
            "PER_SRV_PATOLOGIA_PISO" : "Piso de Madeira / Laminado / Vinílico",
            "PER_SRV_PATOLOGIA_REFORMAAVUL" : "REFORMAAVUL",    
            "PER_SRV_PATOLOGIA_REVEST" : "Revestimento Interno",
            "PER_SRV_PATOLOGIA_REVESTEXT" : "Revestimento Externo - Exceto fachada",   
            "PER_SRV_PATOLOGIA_TELHADO" : "Telhado"            
        }
        
        
        
        causa = {
        'TELHADO' : 'Telhado',
        'ESQUADRIAS_MADEIRA' : 'Esquadrias Madeira',
        'ESQUADRIA_ALUMINIO' : 'Esquadria Aluminio',
        'ESQUADRIAS_METÁLICAS' : 'Esquadrias Metálicas',
        'INST_TELEF_INTEF' : 'Instalações Telefônicas / Interfonia',
        'INSTALACAO_GAS' : 'Instalação Gás',
        'INSTALACAO_HIDRAULICA' : 'Instalação Hidráulicas',
        'IMPROCEDENTE' : 'Improcedente',
        'PINTURAS' : 'Pinturas',
        'INSTALACAO_AGUA_FRIA' : 'Instalação de Água Fria',
        'AR_CONDICIONADO' : 'Ar Condicionado',
        'ESTRUTURA_CONCRETO' : 'Estruturas de Concreto',
        'INSTALACAO_ELETRICA' : 'Instalação Elétrica',
        'REJUNTES' : 'Rejuntes',
        'IMCOMPATIBILIDADE_AGENDAMENTO' : 'Incompatibilidade de Agendamento',
        'PISO_LAMINADO' : 'Piso Laminado',
        'VIDROS' : 'Vidros',
        'ESTRUTURAS_GESSO' : 'Estruturas de Gesso',
        'ARMARIOS' : 'Armários',
        'REV_CER_PORC' : 'Revestimentos Cerâmicos / Porcelanatos',
        'INSTALACOES_ELETRICAS' : 'Instalação Elétrica',
        'PISO_VINILICO' : 'Pisos Vinilicos',
        'VER_CONCRET_APARENTE' : 'Revestimento em concreto aparente',
        'IMPERMEABILIZACAO' : 'Impermeabilização',
        'INSTALACAO_AGUA_PLUVIAL' : 'Instalação de Água Pluvial',
        'REVER_PEDR_NATURAIS' : 'Revestimentos em pedras naturais',
        'INSTALACAO_AGUA_QUENTE' : 'Instalação de Água Quente',
        'ALVENARIA_N_ESTRUTURAL' : 'Alvenaria não estrutural',
        'EQUIP_COMBAT_INCEND' : 'Equipamentos de Combate a Incêndio',
        'PAISAGISMO' : 'Paisagismo',
        'INSTALACAO_TV_A_CABO' : 'Instalação de Tv a Cabo',
        'ALVENARIA_ESTRUTURAL' : 'Alvenaria estrutural',
        'ACUSTICA' : 'Acústica',
        'FACHADA' : 'Fachada',
        'BANHEIRA_HIDROMASSAGEM_SPA' : 'Banheira de hidromassagem / SPA',
        'CAIXA_FIBRA' : 'Caixa de Fibra',
        'QUADRA_POLIESPORTIVA' : 'Quadra Poliesportiva',
        'CHURRASQUEIRA' : 'Churrasqueira',
        'APARTAMENTO_AVULSO' : 'Apartamento avulso',
        'UNIDADE_ESTOQUE' : 'Unidade Estoque'
        }
        
        sub_causa_n1 = {
        'AGUA_DENTRO_CONDUITE' : 'Água dentro do conduite',
        'AGUA_TURBULACAO_AR' : 'Água na turbulação/Ar',
        'ALISAR' : 'Alisar',
        'AQUECEDORES' : 'Aquecedores',
        'AQUECIMENTO_CABOS_FIOS' : 'Aquecimento de cabos/Fios',
        'AUSENCIA_PONTO_DRENO' : 'Ausência do ponto de dreno',
        'BANCADAS' : 'Bancadas',
        'BARULHOS_EQUIP_MOTORES' : 'Barulhos provocados equipamentos/motores',
        'BARULHOS_HIDRAULICOS' : 'Barulhos Hidráulicos',
        'BICHEIRAS' : 'Bicheiras',
        'BOIAS' : 'Bóias',
        'CABOS_DAN' : 'Cabos danificados',
        'CALHA_COM_PROBLMEA_VAZEMENTO' : 'Calha com problema de impermeabilização',
        'CHAPINS' : 'Chapins',
        'CONDUITE_DANIFICADO' : 'Conduite danificado',
        'CONDUITE_ENTUPIDO' : 'Conduite entupido',
        'CONEXÕES_MAL_ENCAIXADAS' : 'Conexões mal encaixadas',
        'CORTES_FUROS_ILEGAIS' : 'Cortes e furos ilegais',
        'DANIFICADO_MAU_USO' : 'Danificado por mau uso',
        'DANO_ESTRU_ESTS_ME_ETC' : 'DANO_ESTRU_ESTS_ME_ETC', ##########################
        'DEFEITOS_NA_ESTRUTURA' : 'Defeitos na estrutura',
        'DEFEITOS_PECAS' : 'Defeito nas peças',
        'DEFEITO_CENTRAL_INTERFONE' : 'Defeito/Falta de interfone',
        'DEFEITO_DEFEITO_ACIONADOR' : 'Defeito no acionador',
        'DEFEITO_FALTA_INTERFONE' : 'Defeito/Falta de interfone',
        'DEFEITO_FALTA_SENSOR_PRESENCA' : 'Defeito/Falta do sensor de presença',
        'DEFEITO_NO_EXAUSTOR' : 'Defeito no exaustor',
        'DEFEITO_PARTES_MOVEIS' : 'Defeito nas partes móveis (feixos, roldanas, fechaduras, etc.)',
        'DEMARCACAO_NUMERO_TAM_VAGA' : 'Demarcação do número ou tamanho da vaga',
        'DEMOLIÇÃO_ILEGAL' : 'Demolição ilegal',
        'DESAGREGAMENTO_PROTEÇÃO_MECÂNI' : 'Desagregamento da proteção mecânica',
        'DESCOLIL' : 'Descolamento',
        'DESCONFORMIDADE_NOM_TEC' : 'DESCONFORMIDADE_NOM_TEC',####################
        'DESREGULAGEM_REDUTORAS_PRESSAO' : 'Desregulagem de redutoras de pressão/ajustes',
        'DESREGULAGEM_REDUTORES_PRESSAO' : 'Desregulagem de redutores de pressão/Ajuste',
        'DISJUNTOR_CONTATORA_DESARMADA' : 'Disjuntor/Contatora desarmada',
        'DISJUNTOR_DR_DESARMADO' : 'Disjuntor DR Desarmado',
        'DRENAGEM' : 'Drenagem',
        'DRENO_OBSTRUÍDO' : 'Dreno obstruído',
        'DRY_WALL' : 'Dry Wall',
        'EMPENO_DESALINHAMENTO' : 'EMPENO_DESALINHAMENTO', #####################
        'ENC_SOL_CONT_CONSTRU_QUAND_CON' : 'Encerrado e solicitado contato com a construtora quando conveniente',
        'ENTUPIMENTO_DE_ELETRODUTOS' : 'Entupimento de eletrodutos',
        'ESPACO_INSU_MEDIDOR_AQUECEDOR' : 'Espaço insuficiente para medidor/Aquecedor',
        'ESPECIES_MORTAS_NAO_DESENV' : 'Espécies mortas/que não desenvolveram',
        'ESPECIFICAÇÃO_REJUNTE_INADEQUA' : 'Especificação de rejunte inadequado',
        'ESQUADRIA_DAN_OU_AMASSADA' : 'Esquadria danificada ou amassada',
        'ESQUADRIA_DESAJUSTADA' : 'Esquadria desajustada',
        'EXECUCAO_DESACORDO_PROJETO' : 'Esquadria desajustada',
        'EXECUCAO_INAD_ACAB_JUNTA_DILAT' : 'Execução indequada/Acabamento de junta de dilatação',
        'EXEC_SOBR_REV_UMIDO_PARED_CHAP' : 'Executada sobre revestimento ainda úmido em paredes ou chapas de gesso',
        'FACHADA_PINTURA_TEXTURIZADA' : 'Fachada em Pintura texturizada',
        'FACHADA_REVESTIMENTO_CERÂMICO' : 'Fachada em revestimento cerâmico',
        'FALAHA_FALTA_INSTAL_MANTA_ACUS' : 'Falha ou falta de instalação da manta acústica',
        'FALHASA_DOS_MATERIAIS' : 'Falha dos materiais',
        'FALHAS_ASSENTAMENTO' : 'Falhas no assentamento',
        'FALHAS_CONCRETAGEM' : 'Falhas de concretagem',
        'FALHAS_DE_FUNCIONAMENTO' : 'Falhas de funcionamento',
        'FALHAS_DE_PREENCHIMENTO' : 'Falhas de preenchimento',
        'FALHAS_DE_RUFO' : 'Falhas/Falta de rufo',
        'FALHAS_ENCONTRO_ESTRUTURA' : 'Falha no encontro com a estrutura',
        'FALHAS_FALT_FIX_TUBOS_CONEXOES' : 'Falhas/Falta de fixação de tubos e conexões',
        'FALHAS_NA_ADERÊNCIA' : 'Falhas na aderência',
        'FALHAS_NA_VEDAÇÃO' : 'Falhas na vedação',
        'FALHAS_REJUNTE_SILICONADO' : 'Falhas no rejunte siliconado',
        'FALHAS_VEDACAO' : 'Falhas na vedação',
        'FALHA_ACAB_MASSA_CORR_MASSA_OL' : 'Falha de acabamanto em massa corrida/Massa óleo/textura',
        'FALHA_ARREMATE_RALO_PASSAGEM_L' : 'Falha no arremate do ralo/passagem da laje',
        'FALHA_COMPO_VIDRO_LAMINADO' : 'Falha na composição do vidro laminado',
        'FALHA_DILTAÇÃO_BARULHO_PISO_ES' : 'Falha na diltação (barulho ao pisar, piso estufando, etc.)',
        'FALHA_ESTUTURA_APOIO' : 'FALHA_ESTUTURA_APOIO',############################
        'FALHA_FIXAÇÃO_PECAS' : 'Falha de fixação de peças',
        'FALHA_JUNTAS_T_ENCONTRO_PAREDE' : 'Falha nas juntas T e encontros com paredes',
        'FALHA_MOLAS_FECHADURA' : 'Falha nas molas e fechaduras',
        'FALHA_NA_FIXACAO' : 'FALHA_NA_FIXACAO', ####################
        'FALHA_PINTURA_EXPÓXI' : 'Falha em pintura expóxi',
        'FALTAS_COMPONENTES' : 'Falta de componentes',
        'FALTA_CALAFETACAO_FRESTAS_MET' : 'Falta de calafetação em frestas metálicas',
        'FALTA_COMPONENTES_QUADROS_ELET' : 'Falta de componentes de quadros elétricos',
        'FALTA_DEMAO_PINTURA' : 'Falta demão de pintura',
        'FALTA_DE_ATERRAMENTO' : 'Falta de aterramento',
        'FALTA_DE_CABEAMENTO' : 'Falta de cabeamento',
        'FALTA_DE_FIAÇÃO' : 'Falta de fiação',
        'FALTA_ESQUADRO_VAOS' : 'Falta de esquadro nos vãos (portas, janelas, etc.)',
        'FALTA_FIXACAO_TUBOS_CONEXOES' : 'Falta fixação de tubos e conexões',
        'FALTA_IDENTIFICAÇÃO_DG' : 'Falta de identificação no DG',
        'FALTA_IDENTIFIC_ADEQUA_CIRCUIT' : 'Falta de identificação adequada do circuito/Painéis/Tomadas',
        'FALTA_OU_EXCESSO_PRESSAO_GAS' : 'FALTA_OU_EXCESSO_PRESSAO_GAS', ####################
        'FALTA_PINTURA_ANT_MOFO' : 'Falta de pintura anti mofo',
        'FALTA_PLACAS_INTERFONE' : 'Defeito/Falta de interfone',
        'FALTA_PONTO_IRRIGACAO' : 'Falta ponto para irrigação',
        'FALTA_VEDANTE_JUNTAS_DILATACAO' : 'Falta de vedante nas juntas de dilatação',
        'FERRAGENSS_EXPOSTAS' : 'Ferragens expostas',
        'FIACAO_DANIFICADA' : 'Fiação danificada',
        'FIACAO_EM_CURTO' : 'Fiação em curto',
        'FIACAO_LIGACOES_INVET_INTERFON' : 'Fiação/Ligações invertidas interfone ou telefone',
        'FIACAO_SOLTA_CORTADA' : 'Fiação solta/Cortada',
        'FILTRO_DE_AGUA' : 'Filtro de água',
        'FISSURASS_TRINCAS' : 'Fissuras/Trincas',
        'FIXACAO_INADEQUADA' : 'Fixação Inadequada',
        'FLEXIVEL_COM_VAZAMENTO' : 'Flexível com vazamento',
        'FORROS_GESSO' : 'Forro de Gesso',
        'GUARDA_CORPO_FALHAS_FIXACAO' : 'Guarda Corpo - falhas na fixação',
        'IDENTIFICAÇÃO_REGISTROS_COMPON' : 'Identificação de registros/Componentes',
        'INEXISTENCIA_DE_IMPERMEABILIZA' : 'Inexistência de impermeabilização',
        'INFILTRACAO_NA_ESQUADRIA' : 'Infiltração na esquadria',
        'INFILTRACAO_PAREDE_DIVISA' : 'Infiltração na parede de divisa',
        'INSTACAO_EXECUT_DESACORDO_PROJ' : 'Instalação executada em desacordo com o projeto',
        'INSTALACAO_DESAC_PROJETO' : 'Instalação em desacordo com o projeto',
        'INSTALACAO_DE_ESGOTO' : 'Instalação de Esgoto',
        'INSTALACAO_ELETRICA_INEFICAZ' : 'Instalação elétrica ineficaz',
        'INSTALACAO_INCORRETA' : 'Instalação incorreta',
        'INSTALACOES_EXPOSTAS_AO_TEMPO' : 'Instalações expostas ao tempo',
        'INTERFERENCIA_RUIDO_NA_LINHA' : 'Interferência/Ruído na linha',
        'INVERSAO_DA_FIACAO' : 'Inversão da fiação',
        'ISOLAMENTO_DE_EMENDAS' : 'Isolamento de emendas',
        'ITEM_FORA_DE_GARANTIA' : 'Item fora de garantia',
        'LOUCAS_SANITARIAS' : 'Louças Sanitárias',
        'MANTA_DAN_CORT_MAL_EXEC_DESC' : 'Manta danificada/Cortada/Mal executada/Descolada',
        'MARCO' : 'Marco',
        'MARCO_ALISARES_PEITORIS_BORD_C' : 'Marcos / alisares / Peitoris / Bordas / Chapins',
        'METAIS_SANITARIOS' : 'Metáis Sanitários',
        'MISTURA_AGUA_QUENTE_NA_FRIA' : 'Mistura de água quente na fria',
        'MODULOS_PLACAS_ACAB_ELETRICOS' : 'Módulos / placas / acabamentos elétricos',
        'NAO_CONSTADO_EM_VIS_TECT' : 'Não constatado em visita técnica',
        'NAO_RESPONSABILIDADE_CONSTRUTO' : 'Não é responsabilidade da construtora',
        'PAREDES' : 'Paredes',
        'PERFURACOES_TRINCAS_EM_TUB_CON' : 'Perfurações/trincas em tubos e conexões',
        'PERFURACOES_TRINCAS_TUBOS_CONE' : 'Perfurações/Trincas em tubos ou conexões',
        'PINTURA' : 'Pintura',
        'PINTURA_COM_BOLHAS' : 'Pintura com bolhas',
        'PINTURA_SUJA' : 'Pintura suja',
        'PINTURA_TONALIDADE_DIFERENTE' : 'Pintura com tonalidade diferente',
        'PISOS' : 'Pisos',
        'PISOS_ELEVADOS' : 'Piso elevado',
        'PISO_DE_GARAGEM' : 'Piso de garagem',
        'PISO_ELEVADO' : 'Piso elevado',
        'PISO_PRE_MOD_INTER_LADRI_HIDR_' : 'Piso pré-moldado (intertravado, ladrilho hidráulico, etc.)',
        'PLANTIO_MAL_EXECUTADO' : 'Plantio mal executado',
        'PONTOS_DE_FERRUGEM' : 'Pontos de ferrugem',
        'PONTOS_ENCOB_DESLOCADO_PROFUND' : 'Ponto encoberto/deslocado/profundo',
        'PONTO_AGUA_ENCOB_DESCOLADO_PRO' : 'Ponto de água encoberto/Descolado/Profundo',
        'PORTA' : 'Porta',
        'PORTAS_CORTA_FOGO' : 'Portas corta fogo',
        'PORTOES' : 'Portões',
        'POS_TUB_DIF_CONST_MANUAL' : 'Posição da tubulação diferente do que consta no manual',
        'PROBLEMAS_AJUSTES_ANTENAS_CONE' : 'Problemas de ajuste de antenas/conectores',
        'PROBLEMAS_CENTRAL_AQUECIMENTO' : 'Problemas com a central de aquecimento',
        'PROBLEMAS_LUMINARIAS' : 'Problemas com as luminárias',
        'PROBLEMAS_PERSIANAS' : 'Problemas nas persianas',
        'PROBLEMAS_ROLDANAS' : 'Problemas nas roldanas',
        'PROBLEMAS_SIST_BOMB' : 'Problemas no sistema de bombeamento',
        'PROBLEMA_ELETRICOS_AUTOMACAO_P' : 'Problema elétricos e de automações de portões',
        'RALO_TUBULACAO_OBSTRUIDA' : 'Ralo/Tubulação obstruída',
        'REALIZADO_CORTESIA_PARA_CLIENT' : 'Realizado em cortesia para o cliente.',
        'REALIZADO_PELO_CLIENTE' : 'Realizado pelo cliente',
        'REFORMA_APARTAMENTO_AVULSO' : 'Reforma de apartamento avulso',
        'REFORMA_UNIDADE_INTERFONE' : 'REFORMA_UNIDADE_INTERFONE', ##################
        'REGISTROS' : 'Registros',
        'REGULAGEM_FALTA_AJUSTES' : 'Regulagem/Falta de ajustes',
        'REJUNTE_ESCURECIDO_AMAR_MOFADO' : 'Rejunte escurecido/Amarelado/Mofado',
        'REJUNTE_ESFARELADO' : 'Rejunte esfarelado',
        'REJUNTE_TRINCADO_DEV_MOV' : 'Rejunte trincado devido a movimentação',
        'RETORNOPAGUA_RALO' : 'Retorno de água pelo ralo',
        'RISCADO_MANCHADO' : 'Riscado/Manchado',
        'RISCADO_MANCHADOS_TRINCADOS_LA' : 'Riscados/Manchados/Trincados/Lascados',
        'RISCOS_AMASSADOS_MANCHADOS_SUJ' : 'Riscos/Amassados/Manchados/Sujos',
        'RUIDO_DE_IMPACTO' : 'Ruído de impacto',
        'SEM_CONTATO_COM_CLIENTE' : 'Sem contato com o cliente',
        'SERVICO_REALIZADO_DESC_PROJ' : 'Serviço realizado em desacordo com o projeto',
        'TAMPA_SOLTA_NAOINSTALADA_DANIF' : 'Tampa do shaft solta/Não instalada/Danificada',
        'TELHAS_MAL_COLOCADAS_FIXADAS' : 'Telhas mal colocadas/Fixadas',
        'TICKET_EM_DUPLICIDADE' : 'Ticket aberto em duplicidade',
        'TOMADAS_E_INTERRUPTORES' : 'Tomadas e Interruptores',
        'TRICA_PROBLEMAS_DILATACAO' : 'Trica por problemas na dilatação',
        'TRINCAS_ESTRUTURAIS' : 'Trincas estruturais',
        'TRINCA_NO_REVESTIMENTO' : 'Trinca no revestimento',
        'TROCA_PROVENIENTE_INFILTRACAO_' : 'Troca proveniente de infiltração/vazamento',
        'TUBULACAO_OBSTRUIDA' : 'Tubulação Obstruída',
        'VAZAMENTO_FALHA_AUSENCIA_SOLDA' : 'Vazamento: Falhas ou ausência de solda/cola',
        'VAZAMENTO_FUNDO_CAIXA' : 'Vazamento no fundo da caixa',
        'VAZAMENTO_NAS_TUBULAÇÕES' : 'Vazamento nas tubulações',
        'VAZAMENTO_PARAFUSO_FIXACAO_TEL' : 'Vazamento pelo parafuso de fixação da telha',
        'VAZAMENTO_PAREDES' : 'Vazamento nas paredes',
        'VAZAMENTO_TUB_TRINCADA_FURADA' : 'Vazamento/tubulação trincada/furada',
        'VAZEMENTO' : 'Vazamento'
        }       
        
        sub_causa_n2 = {
        'ACABAMENTO_INADEQUADO' : 'Acabamento inadequado',
        'AJUSTE_REGULAGEM' : 'Ajustes e Regulagem',
        'CAIMENTO_FALHAS' : 'Caimento com falhas',
        'CAIMENTO_INADEQUADO_CONTRARIO_' : 'Caimento inadequado/ao contrário do ralo',
        'CAIXAS_DANIFICADOS_EXEC_DESCOR' : 'Caixas (gordura, passagem, espuma, etc) danificadas ou executdas em desacordo com o uso.',
        'DEFEITO_FABRICACAO' : 'Defeito de fabricação',
        'DEFEITO_FUNCIONAMENTO_CAIXA_AC' : 'Defeito no funcionamento da caixa acoplada',
        'DESCOLAMENTO_PLACA_FALTA_ADERE' : 'Descolamento de placa/Falta de aderência a laje',
        'DESCONFORMIDADE_COM_O_PROJETO' : 'Desconformidade com o projeto (falha vedação acústica, composição para aumento do tempo de resistencia ao fogo, etc.)',
        'DESNIVELADO' : 'Desnivelado',
        'DESNIVEL_ENTRE_PECAS_DENTES' : 'Desnível entre peças (dentes)',
        'DESNIVEL_ENTRE_PECAS_DENTES_DE' : 'Desnível entre peças (dentes) ou desalinhamento',
        'DESREGULADOS_SOLTOS' : 'Desregulados/Soltos',
        'DETERIORADO_DEV_ABSORCAO_VAPOR' : 'Deteriorado devido a absorção de vapor de agua/Infiltração',
        'DIFERENCA_TONALIDADE' : 'Diferença de tonalidade',
        'DOBRADICAS' : 'Dobradiças',
        'EFLORESCENCIA' : 'Eflorescência',
        'ENTUPIMENTO' : 'Entupimento',
        'ERROS_DE_INSTACAO' : 'Erros de Instalação',
        'ESPACAMENTO_REAJUNTAMENTO_INSU' : 'Espaçamento para rejuntamento insuficiente',
        'ESTUFAMENTO' : 'Estufamento',
        'FALHAS_AUSENCIA_VENTILACAO' : 'Falhas ou ausencia de ventilação.',
        'FALHAS_DE_CAIMENTO' : 'Falhas de Caimento',
        'FALHAS_FIXACAO' : 'Falhas de fixação',
        'FALHAS_INSTALACAO_ESQUAV_REG_E' : 'Falhas de instalação (esquadro no vão, regulagem, etc.)',
        'FALHAS_PINTURA' : 'Falhas de pintura',
        'FALHAS_SIFAO_VALVULAS' : 'Falhas no sifão ou válvulas',
        'FALHA_ASSENTAMENTO_VEDACAO' : 'Falha de assentamento/vedação',
        'FALHA_DRENAGEM_CONTENCOES' : 'Falha na drenagem em contenções',
        'FALHA_FIXACAO_PLACAS' : 'Falha na fixação das placas',
        'FALHA_FUNCIONAMENTO_EQUIPAMENT' : 'Falha de funcionamento dos equipamentos',
        'FALHA_MATERIAL' : 'Falha no material',
        'FALHA_NAS_EMENDAS_DAS_PLACAS' : 'Fissura na fita das emendas das placas',
        'FALHA_NO_REJUNTAMENTO' : 'Falhas no  rejuntamento',
        'FALHA_SISTEMA_SUPORTE' : 'Falha no sistema de suporte',
        'FALH_FALT_FIX_TUBOS_CONEXAO_AB' : 'Falhas/Falta de fixação de tubos e conexões (abraçadeiras, tirantes, perfilados, etc.)',
        'FALTANDO_COMPONENTES' : 'Faltando componentes',
        'FALTA_ANEL_VEDACAO' : 'Falta de anel de vedação',
        'FALTA_DRENAGEM_INEF_PISOS' : 'Falta de drenagem/Ineficiente em pisos',
        'FALTA_DRENAGEM_INEF_QUADRAS' : 'Falta de drenagem/Ineficiente em quadras',
        'FALTA_DRENAGE_INEF_CAIXA_PASSA' : 'Falta de drenagem/Ineficiente em caixa de passagem',
        'FALTA_DRENA_INEFI_JARDINS' : 'Falta de drenagem/Ineficiente em jardins',
        'FALTA_FALHA_VED_ANEL_+BORRACHA' : 'Falta/Falha de vedação (anel de borracha, anel de vedação, etc.)',
        'FAL_INST_PRUMO_NIVEL_ESQUADRO_' : 'Falhas de instalação (prumo, nível, esquadro, fixação, etc.)',
        'FECHADURAS_MAQU_MAC_CHAV' : 'Fechaduras (máquina, maçaneta e chaves)',
        'FISSURAMENTO_TRINCAS_QUEBRADOS' : 'Fissuramento/trincas / quebrados',
        'INFILTRACAO_PROV_FACHADA' : 'Infiltração proveniente da fachada',
        'INSTACAO_EXEC_DESACORDO_PROJET' : 'Instalações executadas em desacordo com projeto',
        'JUNTA_DILTACAO' : 'Junta de dilatação',
        'LASCADOS_RISCADOS_TRINCADOS' : 'Lascados/Riscados/Trincados',
        'MANCHADOS' : 'Manchados',
        'MAU_CHEIRO' : 'Mau cheiro',
        'NÃO_INSTALADOS' : 'Não instalados',
        'ONDULACOES_NA_SUPERFICIE' : 'Ondulações na superfície',
        'ONDULACOES_PLACAS_FORA_PRUMO' : 'Ondulações nas placas/Fora de prumo',
        'PIA_COM_VAZAMENTO' : 'Pia com vazamento',
        'PISO_INTERTRAVADO_AFUNDADO' : 'Piso intertravado afundado',
        'QUEBRADOS' : 'Quebrados',
        'REVESTIMENTO_DESPLACANDO' : 'Revestimento desplacando',
        'REVESTIMENTO_DESPLACANDO_ESFAR' : 'Revestimento desplacando ou esfarelando',
        'RISCADOS_MANCHADOS_FALTANDO_CO' : 'Riscados/Manchados/faltando componentes',
        'SOLTOS_MAL_ADERIDOS' : 'Soltos/mal aderidos',
        'TRINCAS_FISSURAS' : 'Trincas/Fissuras',
        'TRINCA_NO_REVESTIMENTO' : 'Trinca no revestimento',
        'TRINC_MANCHADAS_LASCADAS_TONAL' : 'Trincadas/Manchadas/Lascadas/Tonalidade diferente',
        'TUBOS_CONEXOES_TRINC_FURADOS_R' : 'Tubos/Conexões trincados/Furados/Rachados',
        'VASO_SANIT_PROBLEMA_SINF_DEF_F' : 'Vaso sanitário com problema de sifonagem - defeito de fabricação',
        'VAZAMANTO_INTUPIMENTO_VAV_REG_' : 'Vazamanto/Entupimento em válvula/Registros/torneiras'
        }      
        
        tipoRegistro:dict = {
            "PER_SVR_REGI_SOLICITA" : "Solicitação",
            "PER_SVR_REGI_RECLAMA" : "Reclamação",
            "PER_SVR_REGI_DUVIDA" : "Duvida",
            "PER_SVR_REGI_OFERTA" : "Oferta",
            "PER_SVR_REGI_DEMANDA" : "Demanda Interna",
            "PER_SVR_REGI_INFO" : "Informação",
            "PER_SVR_REGI_SUGEST" : "Sugestão",
            "PER_SVR_REGI_ELOGIO" : "Elogio"            
        }
        
        df["Assunto"] = df["Assunto"].map(assunto, na_action='ignore')
        df["Categoria"] = df["Categoria"].map(categoria, na_action='ignore')
        df["Enviar Pesquisa?"] = df["Enviar Pesquisa?"].map(sim_nao, na_action='ignore')
        df["Chaves Entregues"] = df["Chaves Entregues"].map(sim_nao, na_action='ignore')
        df["Adquiriu Modificação"] = df["Adquiriu Modificação"].map(sim_nao, na_action='ignore')
        df["Adquiriu Personalização"] = df["Adquiriu Personalização"].map(sim_nao, na_action='ignore')
        df["Compareceu à Visita"] = df["Compareceu à Visita"].map(sim_nao, na_action='ignore')
        df["Transbordo"] = df["Transbordo"].map(sim_nao, na_action='ignore')
        df["Tipo de Formulário"] = df["Tipo de Formulário"].map(tipo_formulario, na_action='ignore')
        df["Status da Vistoria"] = df["Status da Vistoria"].map(status, na_action='ignore')
        df["Status da Revistoria"] = df["Status da Revistoria"].map(status, na_action='ignore')
        df["Tipo de Entrada"] = df["Tipo de Entrada"].map(tipo_entrada, na_action='ignore')
        df["Avaliação"] = df["Avaliação"].map(avaliacao, na_action='ignore')
        df["Procedência"] = df["Procedência"].map(procedencia, na_action='ignore')
        df["Patologia"] = df["Patologia"].map(patologia, na_action='ignore')
        df["Causa"] = df["Causa"].map(causa, na_action='ignore')
        df["SubCausaN1"] = df["SubCausaN1"].map(sub_causa_n1, na_action='ignore')
        df["SubCausaN2"] = df["SubCausaN2"].map(sub_causa_n2, na_action='ignore')
        df["Tipo De Registro"] = df["Tipo De Registro"].map(tipoRegistro, na_action='ignore').replace(nb.nan, "Não Preenchido")
        
        
        return df

if __name__ == "__main__":
    print("executado pelo tratarDados.py")
    exit()
    df = pd.read_excel("#material/11.13.18.05_tickets.xlsx")
    
    print(RelatRelacionementoCliente.tratar(df))
    

    