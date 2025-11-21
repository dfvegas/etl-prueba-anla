import kagglehub

# Descarga la última versión del dataset superstore de kagglehub
path = kagglehub.dataset_download("vivek468/superstore-dataset-final")

print("Path to dataset files:", path)
