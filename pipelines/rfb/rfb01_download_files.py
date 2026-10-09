"""RFB (CNPJ open data) -> local landing zone, via the public WebDAV share."""
import os
import sys
import xml.etree.ElementTree as ET
from urllib.parse import unquote
from warnings import filterwarnings

import requests

filterwarnings("ignore")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pipelines.commons.env_loader import DATASOURCE_DIR, rfb_reference, validate_env
from pipelines.commons.logger import get_logger, log_event
from pipelines.observability import current_step

logger = get_logger("rfb_download")

WEBDAV_BASE = "https://arquivos.receitafederal.gov.br/public.php/webdav"
DOWNLOAD_BASE = "https://arquivos.receitafederal.gov.br"


def list_remote_zips(share_token: str, ano_mes: str) -> list[str]:
    url = f"{WEBDAV_BASE}/{ano_mes}"
    logger.info(f"querying remote directory via WebDAV: {ano_mes}")
    response = requests.request("PROPFIND", url, auth=(share_token, ""), headers={"Depth": "1"}, timeout=60)
    response.raise_for_status()
    try:
        root = ET.fromstring(response.content)
    except ET.ParseError as exc:
        raise RuntimeError(f"WebDAV did not return valid XML: {response.text[:500]}") from exc

    ns = {"d": "DAV:"}
    hrefs = []
    for element in root.findall("d:response", ns):
        href = element.find("d:href", ns).text
        is_dir = element.find(".//d:collection", ns) is not None
        if not is_dir and href.endswith(".zip"):
            hrefs.append(href)
    return hrefs


def download_rfb_files(share_token: str, ano_mes: str, base_dir: str) -> int:
    step = current_step()
    target_dir = os.path.join(base_dir, f"ref{ano_mes.replace('-', '')}")
    os.makedirs(target_dir, exist_ok=True)

    try:
        hrefs = list_remote_zips(share_token, ano_mes)
    except Exception as exc:
        log_event(logger, "source_unavailable", f"Receita Federal WebDAV failed: {exc}", level=40, ref_month=ano_mes)
        return 1  # was a silent `return` (task green with nothing downloaded)

    if not hrefs:
        log_event(logger, "source_empty", f"no ZIP files published for {ano_mes}", level=40, ref_month=ano_mes)
        return 1

    step.set(files_total=len(hrefs), ref_month=ano_mes)
    logger.info(f"found {len(hrefs)} ZIP files to download into {target_dir}")

    for href in hrefs:
        file_name = unquote(href.split("/")[-1])
        local_path = os.path.join(target_dir, file_name)
        if os.path.exists(local_path):
            logger.info(f"skipping existing local file: {file_name}")
            step.add(files_ok=1, files_skipped=1)
            continue

        tmp_path = local_path + ".part"  # never leave a truncated .zip behind
        try:
            logger.info(f"downloading file: {file_name}")
            with requests.get(f"{DOWNLOAD_BASE}{href}", auth=(share_token, ""), stream=True, timeout=(30, 300)) as r:
                r.raise_for_status()
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
            os.replace(tmp_path, local_path)
            step.add(files_ok=1, bytes_downloaded=os.path.getsize(local_path))
            logger.info(f"successfully downloaded: {file_name}")
        except Exception as exc:
            step.add(files_failed=1)
            log_event(logger, "file_failed", f"{file_name}: {exc}", level=40, file=file_name)
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    # Bronze needs the full set of files of a reference month: any missing file fails the task.
    return 1 if step.counters.get("files_failed") else 0


if __name__ == "__main__":
    token = os.getenv("RFB_SHARE_TOKEN")
    validate_env({"RFB_SHARE_TOKEN": token})
    sys.exit(download_rfb_files(token, rfb_reference(), os.path.join(DATASOURCE_DIR, "rfb")))
