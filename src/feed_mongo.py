from pathlib import Path
import subprocess
from multiprocessing import Pool, cpu_count
import logging
import tempfile

# Definição dos caminhos e variáveis
origem = Path("/home/davi/Documentos/GitHub/cve_list_pipeline/dataset")
anos = [2020, 2021, 2022, 2023, 2024, 2025]
string_conexao = "mongodb://localhost:27017"
nome_banco = "cve"

# Configuração do logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def import_json(json_files, ano):
    """Função para importar vários arquivos JSON para o MongoDB."""
    try:
        # Criar um arquivo temporário para agrupar os JSONs
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
            for json_file in json_files:
                with json_file.open('r') as f:  
                    temp_file.write(f.read() + '\n') 
        
        temp_file_path = Path(temp_file.name)  # Converter o caminho para um objeto Path

        comando = [
            "mongoimport",
            "--uri", f"{string_conexao}/{nome_banco}",
            "--collection", f"cve_{ano}",
            "--file", str(temp_file_path)  # Converter Path para string
        ]
        
        resultado = subprocess.run(comando, capture_output=True, text=True)
        
        if resultado.returncode != 0:
            logger.error(f"Erro ao importar arquivos para a coleção 'cve_{ano}': {resultado.stderr}")
    
    finally:
        # Remover o arquivo temporário após a importação
        if temp_file_path.exists():  # Verificar se o arquivo existe
            temp_file_path.unlink()  # Remover o arquivo

def process_ano(ano):
    """Processa todos os arquivos JSON de um ano."""
    arquivos_json = list(origem.rglob(f'{ano}/*/*.json'))  # Usar rglob para encontrar arquivos
    if not arquivos_json:
        logger.warning(f"Nenhum arquivo encontrado para o ano {ano}.")
        return
    
    # Dividir os arquivos em lotes para evitar sobrecarga
    tamanho_lote = 100  # Número de arquivos por lote
    lotes = [arquivos_json[i:i + tamanho_lote] for i in range(0, len(arquivos_json), tamanho_lote)]
    
    # Processar cada lote
    for lote in lotes:
        import_json(lote, ano)

try:
    logger.info("Iniciando processamento...")
    
    # Usar multiprocessamento para processar anos em paralelo
    num_processos = min(cpu_count(), len(anos))  # Limitar o número de processos
    with Pool(num_processos) as pool:
        pool.map(process_ano, anos)
    
    logger.info("Processamento concluído!")
except Exception as e:
    logger.error(f"Erro ao processar arquivos: {e}")