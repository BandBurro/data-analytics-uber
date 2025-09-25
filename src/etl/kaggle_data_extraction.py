import kagglehub
import os

dataset_id = "yashdevladdha/uber-ride-analytics-dashboard"

print("Baixando o dataset...")

path = kagglehub.dataset_download(dataset_id)

print(f"Dataset baixado com sucesso para: {path}")

print("Arquivos baixados:")
for root, dirs, files in os.walk(path):
    for file in files:
        print(os.path.join(root, file))