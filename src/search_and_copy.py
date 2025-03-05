import git
import os
import shutil
import logging
from pythonjsonlogger import jsonlogger

logger = logging.getLogger(__name__)
file_handler = logging.FileHandler("./logs/script_json.log")
formatter = jsonlogger.JsonFormatter(
    fmt="%(asctime)s %(levelname)s %(message)s %(filename)s %(lineno)s"
)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)


repo_path = '/home/davi/Documentos/GitHub/cvelistV5'
output_dir = "/home/davi/Documentos/GitHub/cve_list_pipeline/dataset_updated"
LAST_COMMIT_FILE = "./assets/last_commit.txt"

def get_last_commit():
    if os.path.exists(LAST_COMMIT_FILE):
        with open(LAST_COMMIT_FILE, "r") as f:
            return f.read().strip()
    return None

def save_last_commit(commit_hash):
    with open(LAST_COMMIT_FILE, "w") as f:
        f.write(commit_hash)

def update_local_repo():
    logger.info("Acessando o repositorio local...")
    repo = git.Repo(repo_path)

    logger.info("Atualizando repositorio local...")
    origin = repo.remotes.origin
    origin.pull()

    return repo

def check_new_commits(last_commit_hash, latest_commit, repo):

    if not last_commit_hash:
        error_msg = "Nao ha ultimo commit verificado. Crie um documento 'last_commit.txt' com o hash do ultimo commit verificado."
        return False, error_msg
    if latest_commit.hexsha == last_commit_hash:
        error_msg = "Nao ha novos commits."
        return False, error_msg
    return repo.commit(last_commit_hash), None    

def checking_new_or_modified_files(commits):

    files_to_copy = []

    for commit in reversed(commits):  
        logger.info("Verificando commit", extra={
        "commit_hash": commit.hexsha,
        "commit_message": commit.message.strip()
    })
        
        if commit.parents:
            previous_commit = commit.parents[0]
            diff = previous_commit.diff(commit)

            # Verificar arquivos adicionados ou modificados
            for file_diff in diff:
                if file_diff.change_type in ["A", "M"]:  # Arquivo adicionado ou modificado
                    files_to_copy.append(file_diff.a_path)
                    logger.debug("Arquivo detectado", extra={
                        "file_path": file_diff.a_path,
                        "change_type": file_diff.change_type
                    })

    return files_to_copy
        

try:
    repo = update_local_repo()
    latest_commit = repo.head.commit
    last_commit_hash = get_last_commit()

    logger.info("Checando se exitem novos commits...")

    last_commit, error_msg = check_new_commits(last_commit_hash, latest_commit, repo)

    if not last_commit:
        raise Exception(error_msg)

    commits = list(repo.iter_commits(f"{last_commit.hexsha}..{latest_commit.hexsha}"))
    
    files_to_copy = checking_new_or_modified_files(commits)
    
    for file_path in files_to_copy:
        src_path = os.path.join(repo_path, file_path)
        dest_path = os.path.join(output_dir, file_path)
        
        # Copiar o arquivo
        shutil.copy2(src_path, output_dir)
        
    logger.info("Arquivos copiados", extra={
            "status": "success"
        })
    
    save_last_commit(latest_commit.hexsha)
    logger.info("Copia concluida")

except git.GitCommandError as e:
    logger.error("Erro ao executar comando Git", extra={"error": str(e)})
except FileNotFoundError as e:
    logger.error("Arquivo ou diretorio não encontrado", extra={"error": str(e)})
except Exception as e:
    logger.critical("Erro inesperado", extra={"error": str(e)})