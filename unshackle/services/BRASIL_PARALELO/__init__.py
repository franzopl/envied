import re
import requests
from typing import List, Union
from unshackle.core.services import Service
from unshackle.core.search_result import SearchResult
from unshackle.core.titles.movie import Movie, Movies
from unshackle.core.titles.episode import Series
from unshackle.core.tracks.track import Track
from unshackle.core.drm import Widevine
import click
import logging
import json
import base64
import xml.etree.ElementTree as ET

class BRASIL_PARALELO(Service):
    """
    Brasil Paralelo Service
    Version: 1.0
    Auth: Cookies
    Security: Widevine DRM (L3)
    Usage: unshackle dl BRASIL_PARALELO <title_id> [--movie]
    """
    
    TITLE_RE = re.compile(r"^(?:https?://plataforma\.brasilparalelo\.com\.br/playlists/[^/]+/media/)?(?P<title_id>[0-9a-f]{24})$")
    
    GEOFENCE = ("BR",)
    
    def __init__(self, ctx, **kwargs):
        self.title_id = kwargs.pop("title", None)
        self.is_movie = kwargs.pop("movie", False)
        self.device = kwargs.pop("device", "web")
        kwargs.pop("debug", None)  # Remove debug to avoid TypeError
        super().__init__(ctx, **kwargs)
        self.base_url = "https://plataforma.brasilparalelo.com.br"
        self.stream_base = "https://stream.brasilparalelo.com.br"
        self.session.headers.update(self.config["client"][self.device]["headers"])
    
    def authenticate(self):
        if self.cookies:
            self.session.cookies.update(self.load_cookies("BRASIL_PARALELO"))
        else:
            raise Exception("Cookies necessários. Exporte do navegador para Cookies/BRASIL_PARALELO/default.txt")
    
    def search(self, query: str) -> List[SearchResult]:
        resp = self.session.get(f"{self.base_url}/api/search", params={"q": query})
        results = resp.json().get("results", [])
        return [SearchResult(title=r["name"], id=r["id"], type=r["type"]) for r in results]
    
    def get_titles(self, title_id: str = None) -> Union[Movies, Series]:
        title_id = title_id or self.title_id
        if not title_id:
            raise ValueError("Nenhum title_id fornecido.")
        
        query = """
        query ($media_id: String!, $slug: String!) {
          media(media_id: $media_id, playlistslug: $slug) {
            id
            name
            duration
            playlist {
              type { name }
              name
              slug
            }
          }
        }
        """
        variables = {
            "media_id": title_id,
            "slug": "entre-lobos"
        }
        payload = {"query": query, "variables": variables}
        
        url = self.config["endpoints"]["metadata"]
        try:
            resp = self.session.post(url, json=payload)
            resp.raise_for_status()
        except requests.RequestException as e:
            logging.error(f"Falha ao acessar o endpoint GraphQL {url}: {e}")
            raise Exception(f"Falha ao obter metadados para title_id {title_id}: {e}")
        
        data = resp.json()
        logging.debug(f"Resposta completa da API GraphQL para metadados: {json.dumps(data, indent=2)}")
        
        if "errors" in data:
            logging.error(f"Erros retornados pela API GraphQL: {data['errors']}")
            raise ValueError(f"Erros na resposta da API: {data['errors']}")
        
        media = data.get("data", {}).get("media")
        if not media:
            raise ValueError(f"Nenhum dado de mídia retornado pela API: {data}")
        
        name = media.get("name")
        if not name:
            raise KeyError(f"Chave 'name' não encontrada na resposta da API: {data}")
        
        content_type = media.get("playlist", {}).get("type", {}).get("name")
        duration = media.get("duration")
        slug = media.get("playlist", {}).get("slug")
        if not slug:
            raise ValueError(f"Chave 'playlist.slug' não encontrada na resposta da API: {data}")
        
        self.slug = slug  # Armazenar o slug para uso em get_tracks
        
        if self.is_movie or content_type == "movie":
            movie = Movie(
                id_=title_id,
                service=self.__class__,
                name=name,
                year=None,
                data={"duration": duration}
            )
            return Movies([movie])
        else:
            return Series(id=title_id, title=name, seasons=[{"name": media.get("playlist", {}).get("name")}])
    
    def get_tracks(self, title_id: str = None) -> List[Track]:
        title_id = title_id or self.title_id
        if not title_id:
            raise ValueError("Nenhum title_id fornecido.")
        
        query = """
        query ($media_id: String!, $slug: String!) {
          media(media_id: $media_id, playlistslug: $slug) {
            source { id }
          }
        }
        """
        variables = {
            "media_id": title_id,
            "slug": getattr(self, "slug", "entre-lobos")
        }
        payload = {"query": query, "variables": variables}
        
        url = self.config["endpoints"]["metadata"]
        try:
            resp = self.session.post(url, json=payload)
            resp.raise_for_status()
        except requests.RequestException as e:
            logging.error(f"Falha ao acessar o endpoint GraphQL para tracks {url}: {e}")
            raise Exception(f"Falha ao obter tracks para title_id {title_id}: {e}")
        
        data = resp.json()
        logging.debug(f"Resposta completa da API GraphQL para tracks: {json.dumps(data, indent=2)}")
        
        if "errors" in data:
            logging.error(f"Erros retornados pela API GraphQL para tracks: {data['errors']}")
            raise ValueError(f"Erros na resposta da API para tracks: {data['errors']}")
        
        media = data.get("data", {}).get("media")
        if not media:
            raise ValueError(f"Nenhum dado de mídia retornado pela API para tracks: {data}")
        
        content_id = media.get("source", {}).get("id")
        if not content_id:
            raise ValueError(f"content_id não encontrado na resposta da API: {data}")
        
        # Construct manifest URL using content_id and stream_id from HAR
        stream_id = "d7137531-aab7-43f7-a15b-a64b0453b4ca"  # Episódio 2
        if title_id == "62b0d862d12d4c0029f05602":
            stream_id = "79f73236-4c3a-4998-8f5d-c14d33a91c9f"  # Episódio 1
        manifest_url = f"https://stream.brasilparalelo.com.br/{content_id}/{stream_id}/mpd/stream.mpd"
        
        # Baixar e parsear o manifest MPD
        try:
            manifest_resp = self.session.get(manifest_url)
            manifest_resp.raise_for_status()
            manifest_xml = manifest_resp.text
            logging.debug(f"Manifest MPD response status: {manifest_resp.status_code}")
            logging.debug(f"Manifest MPD content: {manifest_xml[:1000]}...")  # Log first 1000 chars
            logging.debug(f"Full Manifest MPD content: {manifest_xml}")  # Log full content
        except requests.RequestException as e:
            logging.error(f"Falha ao baixar o manifest MPD {manifest_url}: {e}")
            raise Exception(f"Falha ao obter manifest MPD: {e}")
        
        # Parsear o XML do manifest
        try:
            root = ET.fromstring(manifest_xml)
        except ET.ParseError as e:
            logging.error(f"Falha ao parsear o manifest MPD XML: {e}")
            raise ValueError(f"Falha ao parsear o manifest MPD XML: {e}")
        
        namespaces = {
            "cenc": "urn:mpeg:cenc:2013",
            "dash": "urn:mpeg:dash:schema:mpd:2011"
        }
        kid = None
        pssh = None
        # Extract cenc:default_KID from mp4protection
        for cp in root.findall(".//dash:ContentProtection[@schemeIdUri='urn:mpeg:dash:mp4protection:2011']", namespaces):
            logging.debug(f"mp4protection ContentProtection attributes: {cp.attrib}")
            kid = cp.get("cenc:default_KID")
            if kid:
                logging.debug(f"Found KID in mp4protection: {kid}")
                break
        # Extract cenc:pssh from Widevine
        if kid:
            for cp in root.findall(".//dash:ContentProtection[@schemeIdUri='urn:uuid:edef8ba9-79d6-4ace-a3c8-27dcd51d21ed']", namespaces):
                logging.debug(f"Widevine ContentProtection attributes: {cp.attrib}")
                pssh_elem = cp.find("cenc:pssh", namespaces)
                pssh = pssh_elem.text if pssh_elem is not None else None
                if pssh:
                    logging.debug(f"Found PSSH in Widevine: {pssh}")
                    break
        # Fallback to any ContentProtection with cenc:pssh if Widevine fails
        if kid and not pssh:
            for cp in root.findall(".//dash:ContentProtection", namespaces):
                logging.debug(f"Fallback ContentProtection attributes: {cp.attrib}")
                pssh_elem = cp.find("cenc:pssh", namespaces)
                pssh = pssh_elem.text if pssh_elem is not None else None
                if pssh:
                    logging.debug(f"Found PSSH in fallback: {pssh}")
                    break
        
        if not kid or not pssh:
            raise ValueError(f"Não foi possível extrair kid ou pssh do manifest MPD: {manifest_url}")
        
        logging.debug(f"Extracted KID: {kid}")
        logging.debug(f"Extracted PSSH: {pssh}")
        
        tracks = self.parse_manifest(manifest_url)
        tracks[0].drm = Widevine(
            key=None,  # key será obtido via licença Widevine
            license_url=self.config["endpoints"]["metadata"],
            pssh=pssh,
            content_id=content_id
        )
        tracks[0].drm.kid = kid
        return tracks
    
    def get_widevine_license(self, challenge: bytes, track: Track) -> bytes:
        query = """
        query ($drm_type: String!, $license_challenge: String!, $media_id: String!) {
          drm_license_v_2(drm_type: $drm_type, license_challenge: $license_challenge, media_id: $media_id) {
            ... on license {
              license
            }
            ... on error {
              error
              code
              message
            }
          }
        }
        """
        variables = {
            "drm_type": "widevine",
            "license_challenge": base64.b64encode(challenge).decode("utf-8"),
            "media_id": self.title_id
        }
        payload = {"query": query, "variables": variables}
        
        url = self.config["endpoints"]["metadata"]
        try:
            resp = self.session.post(url, json=payload)
            resp.raise_for_status()
        except requests.RequestException as e:
            logging.error(f"Falha ao obter licença Widevine via GraphQL {url}: {e}")
            raise Exception(f"Falha ao obter licença Widevine: {e}")
        
        data = resp.json()
        logging.debug(f"Resposta completa da API GraphQL para licença Widevine: {json.dumps(data, indent=2)}")
        
        if "errors" in data:
            logging.error(f"Erros retornados pela API GraphQL para licença Widevine: {data['errors']}")
            raise ValueError(f"Erros na resposta da API para licença Widevine: {data['errors']}")
        
        license_data = data.get("data", {}).get("drm_license_v_2")
        if not license_data or "license" not in license_data:
            error = license_data.get("error", {}) if license_data else {}
            raise Exception(f"Falha ao obter licença Widevine: {error.get('message', 'Erro desconhecido')}")
        
        return base64.b64decode(license_data["license"])
    
    def get_chapters(self, title_id: str = None) -> List:
        """
        Retrieve chapters for the given title_id.
        Returns an empty list as chapters are not provided in the JSON data.
        TODO: Implement API call to fetch chapters if available.
        """
        return []

@click.command(name="BRASIL_PARALELO", short_help="https://plataforma.brasilparalelo.com.br")
@click.argument("title", type=str)
@click.option("-m", "--movie", is_flag=True, default=False, help="Especifica se é um filme")
@click.option("-d", "--device", type=str, default="web", help="Selecione dispositivo do config")
@click.option("--debug", is_flag=True, help="Enable debug logging")
@click.pass_context
def cli(ctx, **kwargs):
    if kwargs.get("debug"):
        logging.getLogger().setLevel(logging.DEBUG)
    service = BRASIL_PARALELO(ctx, **kwargs)
    title = service.get_titles()
    tracks = service.get_tracks()
    return service

# Exportar explicitamente o método cli
BRASIL_PARALELO.cli = cli