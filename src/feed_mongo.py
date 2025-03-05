from pathlib import Path
import subprocess

origem = Path("/home/davi/Documentos/GitHub/cve_list_pipeline/dataset")
anos = [2020,2021,2022,2023,2024,2025]
string_conexao = "mongodb://localhost:27017"
nome_banco = "cve"

for ano in anos:
    for json_file in origem.rglob(f'{ano}/*/*.json'):
        comando = [
        "mongoimport",
        "--uri", f"{string_conexao}/{nome_banco}",
        "--collection", f"teste_{ano}",
        "--file", str(json_file)
]

        resultado = subprocess.run(comando, capture_output=True, text=True)

        if resultado.returncode == 0:
            print("Arquivo importado com sucesso!")
        else:
            print(f"Erro ao importar o arquivo: {resultado.stderr}")
        

