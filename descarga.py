import kagglehub
import shutil

# Download latest version
path = kagglehub.dataset_download("mkechinov/direct-messaging")


print("Path to dataset files:", path)

origen = path
destino = "./data/bronze_data"

shutil.move(origen, destino)