
import os
import sys
import requests
import xml.etree.ElementTree as ET
from urllib.parse import unquote
from warnings import filterwarnings

filterwarnings("ignore")

#=============================================================================================================
#===================================
# env loader - connection setup
#===================================
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pipelines.commons.env_loader import (
    MINIO_ACCESS_KEY, MINIO_SECRET_KEY, validate_env,
)
from pipelines.commons.logger import get_logger

logger = get_logger("rfb_download")

validate_env({
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
})
#=============================================================================================================


def download_rfb_files(share_token: str, ano_mes: str, base_dir: str):
    webdav_url = f"https://arquivos.receitafederal.gov.br/public.php/webdav/{ano_mes}"
    target_dir = os.path.join(base_dir, f"ref{ano_mes.replace('-', '')}")
    os.makedirs(target_dir, exist_ok=True)
    
    auth = (share_token, "")
    
    logger.info(f"Querying remote directory via WebDAV: {ano_mes}")
    try:
        response = requests.request("PROPFIND", webdav_url, auth=auth, headers={"Depth": "1"})
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to access Receita Federal WebDAV URL: {e}")
        return

    namespaces = {'d': 'DAV:'}
    root = ET.fromstring(response.content)

    # namespaces = {'d': 'DAV:'}
    
    # try:
    #     root = ET.fromstring(response.content)
    # except ET.ParseError as e:
    #     logger.error("Falha ao fazer o parse do XML. O servidor não retornou um WebDAV válido.")
    #     logger.error(f"Conteúdo recebido : {response.text[:1000]}")
    #     raise RuntimeError(f"ParseError: {e}")
    
    files_to_download = []
    for element in root.findall('d:response', namespaces):
        href = element.find('d:href', namespaces).text
        is_dir = element.find('.//d:collection', namespaces) is not None
        
        if not is_dir and href.endswith('.zip'):
            files_to_download.append(href)

    logger.info(f"Found {len(files_to_download)} ZIP files to download into {target_dir}")

    for href in files_to_download:
        file_name = unquote(href.split('/')[-1])
        download_url = f"https://arquivos.receitafederal.gov.br{href}"
        local_path = os.path.join(target_dir, file_name)
        
        if os.path.exists(local_path):
            logger.info(f"Skipping existing local file: {file_name}")
            continue
            
        logger.info(f"Downloading file: {file_name}")
        
        with requests.get(download_url, auth=auth, stream=True) as r:
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
        logger.info(f"Successfully downloaded: {file_name}")

if __name__ == "__main__":
    TOKEN = os.getenv("RFB_SHARE_TOKEN")
    REFERENCIA = "2026-08"
    LANDING_ZONE = "/mnt/datasource/rfb"
    
    download_rfb_files(TOKEN, REFERENCIA, LANDING_ZONE)