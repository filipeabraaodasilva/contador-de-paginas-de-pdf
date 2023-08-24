# Importando bibliotecas de terceiros
import os
import fitz
import pyodbc
from tqdm import tqdm
import datetime as dt
from azure.storage.blob import BlobServiceClient



# Função para buscar dados da unidade e do cliente
def buscar_dados(id_unidade_cliente):
    conn = pyodbc.connect(
        'Driver={SQL Server};'
        'Server=nome_servidor.database.windows.net;'
        'Database=banco_de_dados;'
        'Uid=usuario;'
        'Pwd=senha;'
    )
    
    cursor = conn.cursor()

    cursor.execute("""
        SELECT      DISTINCT
                    u.Id AS 'IdUnidade', 
                    u.NomeFantasia AS 'Unidade',
                    c.Id AS 'IdCliente',
                    c.NomeFantasia AS 'Cliente',
                    u.Azure_NomeConta AS 'Azure_Conta', 
                    u.Azure_ChaveAcessoPrimario AS 'Azure_Chave'
        FROM        Schema.UnidadeCliente uc 
        INNER JOIN  Schema.Unidade u ON u.Id = uc.IdUnidade AND uc.Id = ? 
        INNER JOIN  Schema.Cliente c ON c.Id = uc.IdCliente""", (id_unidade_cliente)
        )
        
    resultado = cursor.fetchone()

    cursor.close()
    conn.close()

    dados = {
        "id_unidade": "{}".format(resultado[0]),
        "nome_unidade": "{}".format(resultado[1]),
        "id_cliente": "{}".format(resultado[2]),
        "nome_cliente": "{}".format(resultado[3]),
        "azure_conta": "{}".format(resultado[4]),
        "azure_chave": "{}".format(resultado[5])
    }
    
    return dados
    
def data():
    data = dt.datetime.now()
    data = str(data).split(".")
    data = data[0]
    data = data.replace("-","").replace(" ","").replace(":","")
    return data

# Especifica o diretório que serão salvos os arquivos
diretorio: str = "C:\\"

data_name = data()

with open(diretorio + "\\txt\\lista-img.txt", "r") as arquivos:

    total_linhas: int = sum(1 for linha in arquivos)
    
    arquivos.seek(0)

    for linha in tqdm(arquivos, total = total_linhas, desc = "PROGRESSO: "):
        
        id_unidade_cliente, imagem_azure = linha.strip().split("|")
        
        caminho_local: str = "{}\\tmp".format(diretorio)
        
        caminho_download: str = "{}\\{}".format(caminho_local, imagem_azure)
        
        try:        
        
            dados = buscar_dados(id_unidade_cliente)
        
            # Conectar ao serviço de blob do Azure
            connection_string = "DefaultEndpointsProtocol=https;AccountName={};AccountKey={};EndpointSuffix=core.windows.net".format(dados["azure_conta"], dados["azure_chave"])
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            
            # Nome do contêiner e nome do blob
            container_name = dados["id_cliente"].lower()
            blob_name = imagem_azure
            
            num_paginas = 0
            
            id_imagem = imagem_azure.split(".") 
            id_imagem = id_imagem[0].upper()
            
            if not os.path.exists(caminho_local):
                os.makedirs(caminho_download)
            
            with open(caminho_download, "wb") as file:
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                file.write(blob_client.download_blob().readall())
                
            try:
                documento = fitz.open(caminho_download)
                num_paginas = documento.page_count
                documento.close()
                
            except Exception as erro_contar_paginas:
                
                erro_contar_paginas = str(erro_contar_paginas)
                erro_contar_paginas = erro_contar_paginas.replace("\n", " ")
            
                with open(diretorio + "\\log\\{}__Erro_Contar_Paginas.txt".format(data_name), "a", encoding="utf-8") as log_erro_contar_pagina:
                    log_erro_contar_pagina.write("Erro ao contar página do IdImagem: '{}'. | {}".format(id_imagem, erro_contar_paginas))   
                    
                continue
                
            with open(diretorio + "\\comandosql\\{}__UPDATE__{}__{}.sql".format(data_name, dados["nome_unidade"].replace(" ", "_"), dados["nome_cliente"].replace(" ", "_")), "a", encoding="utf-8") as comando_sql:
                comando_sql.write("UPDATE Schema.Imagem SET QtdePagina = {} WHERE Id = '{}';\n".format(num_paginas, id_imagem))
            
            os.remove(caminho_download)
            
            with open(diretorio + "\\log\\{}__LOG_SUCESSO.txt".format(data_name, id_unidade_cliente), "a", encoding="utf-8") as log_sucesso:
                log_sucesso.write("Páginas contadas com sucesso, IdImagem: '{}'.\n".format(id_imagem))
                
        except Exception as e:
        
            e = str(e)
            e = e.replace("\n", " ")
            
            with open(diretorio + "\\log\\{}__ERROS_NAO_TRATADOS.txt".format(data_name), "a", encoding = "utf-8") as erros_nao_tratados:
                erros_nao_tratados.write("Imagem: '{}' | Erro: '{}' \n".format(imagem_azure, e))