import kagglehub
import os

# ID do dataset no Kaggle
dataset_id = "yashdevladdha/uber-ride-analytics-dashboard"

print("Baixando o dataset...")

# Baixa a versão mais recente do dataset para o diretório atual
path = kagglehub.dataset_download(dataset_id)

print(f"Dataset baixado com sucesso para: {path}")

# Lista os arquivos baixados
print("Arquivos baixados:")
for root, dirs, files in os.walk(path):
    for file in files:
        print(os.path.join(root, file))