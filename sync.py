#!/usr/bin/env python3
"""
Justimmo → Webflow CMS Synchronisation v3
==========================================
Liest alle aktiven Immobilien von der Justimmo API (OpenImmo-Format)
und importiert/aktualisiert sie vollständig in die Webflow CMS Collections.

Wichtige Besonderheiten:
  - Bilder werden als Webflow Assets hochgeladen (S3) → fileId wird verwendet
  - TITELBILD-Gruppe = property-cover-image (echtes Titelbild)
  - BILD-Gruppe[1:] = small-image-1..4 (BILD[0] = Agenten-PNG, wird übersprungen)
  - Slug wird NUR beim Erstellen neuer Items gesetzt, nie beim Update
  - property-overview wird nur gesetzt wenn Beschreibung vorhanden (Pflichtfeld)
  - Ansprechpartner aus Justimmo kontaktperson/id → Webflow Agent-ID

XML-Struktur (OpenImmo-Format):
  <immobilie>
    <objektkategorie>  → Typ, Vermarktungsart
    <geo>              → PLZ, Ort, Koordinaten
    <preise>           → Preise, Miete
    <flaechen>         → Flächen, Zimmer
    <freitexte>        → Titel, Beschreibung
    <anhaenge>         → Bilder (TITELBILD + BILD)
    <kontaktperson>    → Ansprechpartner (id)
    <verwaltung_techn> → objektnr_intern (JM-ID), objektnr_extern (Objektnummer)
    <verwaltung_objekt>→ Status, verfuegbar_ab
"""

import os
import sys
import time
import logging
import argparse
import unicodedata
import re
import hashlib
import xml.etree.ElementTree as ET
from typing import Optional

import requests

# ─────────────────────────────────────────────
# Konfiguration
# ─────────────────────────────────────────────
JUSTIMMO_USER  = os.getenv("JUSTIMMO_USER", "api-97120")
JUSTIMMO_PASS  = os.getenv("JUSTIMMO_PASS", "cScKIP9TW2")
JUSTIMMO_BASE  = "https://api.justimmo.at/rest/v1"

WEBFLOW_TOKEN  = os.getenv("WEBFLOW_TOKEN", "")
WEBFLOW_BASE   = "https://api.webflow.com/v2"
WEBFLOW_SITE_ID = "699f29df3ecf1945550ca280"

# Collection IDs
COL_PROPERTIES = "699f29e03ecf1945550ca36c"
COL_AGENTS     = "699f29e03ecf1945550ca38b"
COL_LOCATIONS  = "699f29e03ecf1945550ca3c6"
COL_CATEGORIES = "699f29e03ecf1945550ca3da"
COL_TYPES      = "699f29e03ecf1945550ca3e1"

# Justimmo Kontaktperson-ID → Webflow Agent-ID Mapping
AGENT_MAP = {
    "2009026":  "69a6ee9c6ee9509b8be4d895",  # Harald Grassler
    "16385723": "69a6eea0f083679bed8045a9",  # Mario Schmid
    "22260941": "69a6eea3f9234d30c8dc0c9d",  # Nataliya Schweda
    "26293524": "69a6ee9e982c3f7f3c614e9a",  # Sascha Nevoral
}

# Rate-Limiting
WEBFLOW_RATE_DELAY = 1.2  # Sekunden zwischen Webflow-Requests

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("sync.log", encoding="utf-8"),
    ]
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Hilfsfunktionen
# ─────────────────────────────────────────────
def slugify(text: str) -> str:
    text = str(text).lower().strip()
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_-]+", "-", text)
    text = re.sub(r"^-+|-+$", "", text)
    return text or "objekt"


def xml_text(element, path: str, default: str = "") -> str:
    node = element.find(path)
    if node is not None and node.text:
        return node.text.strip()
    return default


def xml_float(element, path: str, default: float = 0.0) -> float:
    val = xml_text(element, path)
    try:
        return float(val.replace(",", "."))
    except (ValueError, AttributeError):
        return default


def strip_html(text: str) -> str:
    """Einfaches HTML-Strippen (Legacy-Fallback)."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def html_to_structured_text(html: str) -> str:
    """
    Konvertiert HTML-Beschreibung aus Justimmo in strukturierten Plaintext.
    Erhält Absätze, Aufzählungen, Überschriften und Fettschrift-Markierungen.
    Verwendet \n\n als Absatztrenner und • für Listenelemente.
    Das Frontend (JS) rendert daraus schönes HTML.
    """
    import re as _re
    import html as _html

    # HTML-Entities dekodieren
    text = _html.unescape(html)

    # Block-Elemente: Absatz-Trenner einfügen
    # <p>, <div>, <br>, <h1>-<h6> → Zeilenumbrüche
    text = _re.sub(r'<h[1-6][^>]*>', '\n\n## ', text, flags=_re.IGNORECASE)
    text = _re.sub(r'</h[1-6]>', '\n\n', text, flags=_re.IGNORECASE)
    text = _re.sub(r'<p[^>]*>', '\n\n', text, flags=_re.IGNORECASE)
    text = _re.sub(r'</p>', '', text, flags=_re.IGNORECASE)
    text = _re.sub(r'<br\s*/?>', '\n', text, flags=_re.IGNORECASE)
    text = _re.sub(r'<div[^>]*>', '\n\n', text, flags=_re.IGNORECASE)
    text = _re.sub(r'</div>', '', text, flags=_re.IGNORECASE)

    # Listen: <li> → Bullet-Punkt
    text = _re.sub(r'<li[^>]*>', '\n• ', text, flags=_re.IGNORECASE)
    text = _re.sub(r'</li>', '', text, flags=_re.IGNORECASE)
    text = _re.sub(r'<[uo]l[^>]*>', '\n', text, flags=_re.IGNORECASE)
    text = _re.sub(r'</[uo]l>', '\n', text, flags=_re.IGNORECASE)

    # Fettschrift: <strong>, <b> → **text** Marker
    text = _re.sub(r'<(?:strong|b)[^>]*>', '**', text, flags=_re.IGNORECASE)
    text = _re.sub(r'</(?:strong|b)>', '**', text, flags=_re.IGNORECASE)

    # Unterstreichung: <u> → Marker für Abschnittstitel
    text = _re.sub(r'<u[^>]*>', '§§', text, flags=_re.IGNORECASE)
    text = _re.sub(r'</u>', '§§', text, flags=_re.IGNORECASE)

    # Alle verbleibenden HTML-Tags entfernen
    text = _re.sub(r'<[^>]+>', '', text)

    # Mehrfache Leerzeilen auf max. 2 reduzieren
    text = _re.sub(r'\n{3,}', '\n\n', text)

    # Zeilen trimmen
    lines = [l.rstrip() for l in text.split('\n')]
    text = '\n'.join(lines)

    # Führende/nachfolgende Leerzeilen entfernen
    text = text.strip()

    return text[:8000]


# ─────────────────────────────────────────────
# Webflow Asset Upload
# ─────────────────────────────────────────────
def upload_image_to_webflow(img_url: str, filename: str, wf_headers: dict) -> Optional[dict]:
    """
    Lädt ein Bild von img_url herunter und lädt es als Webflow Asset hoch.
    Gibt {"fileId": ..., "url": ...} zurück oder None bei Fehler.
    
    Webflow Asset Upload Ablauf:
    1. POST /sites/{site_id}/assets → Upload-Slot anfordern (gibt uploadUrl + uploadDetails)
    2. POST uploadUrl mit multipart/form-data → Bild zu S3 hochladen
    3. fileId aus Schritt 1 verwenden
    """
    try:
        # Bild herunterladen
        r = requests.get(img_url, timeout=30)
        if r.status_code != 200:
            log.warning(f"    Bild nicht erreichbar (HTTP {r.status_code}): {img_url[-60:]}")
            return None
        img_data = r.content
        if len(img_data) < 500:
            log.warning(f"    Bild zu klein ({len(img_data)} bytes) – übersprungen")
            return None

        # Content-Type und Dateiname
        ct_header = r.headers.get("content-type", "image/jpeg").split(";")[0].strip()
        if "png" in ct_header:
            content_type = "image/png"
            if not filename.endswith(".png"):
                filename = filename.rsplit(".", 1)[0] + ".png"
        elif "webp" in ct_header:
            content_type = "image/webp"
            if not filename.endswith(".webp"):
                filename = filename.rsplit(".", 1)[0] + ".webp"
        else:
            content_type = "image/jpeg"
            if not filename.endswith(".jpg"):
                filename = filename.rsplit(".", 1)[0] + ".jpg"

        file_hash = hashlib.md5(img_data).hexdigest()

        # Upload-Slot anfordern
        slot_resp = requests.post(
            f"{WEBFLOW_BASE}/sites/{WEBFLOW_SITE_ID}/assets",
            headers=wf_headers,
            json={"fileName": filename, "fileHash": file_hash},
            timeout=30,
        )
        if slot_resp.status_code not in (200, 202):
            log.warning(f"    Asset-Slot Fehler {slot_resp.status_code}: {slot_resp.text[:150]}")
            return None

        asset_data    = slot_resp.json()
        upload_url    = asset_data["uploadUrl"]
        upload_fields = asset_data["uploadDetails"]
        asset_id      = asset_data["id"]
        hosted_url    = asset_data.get("hostedUrl", "")

        # S3 Upload
        form_fields = dict(upload_fields)
        if "content-type" in form_fields:
            form_fields["content-type"] = content_type
        s3_resp = requests.post(
            upload_url,
            data=form_fields,
            files={"file": (filename, img_data, content_type)},
            timeout=60,
        )
        if s3_resp.status_code not in (200, 201, 204):
            log.warning(f"    S3 Upload Fehler {s3_resp.status_code}")
            return None

        # CDN-URL konstruieren (Webflow CDN-Muster: cdn.prod.website-files.com/{siteId}/{assetId}_{filename})
        cdn_url = f"https://cdn.prod.website-files.com/{WEBFLOW_SITE_ID}/{asset_id}_{filename}"
        
        log.info(f"    \u2713 Bild hochgeladen: {filename} ({len(img_data)//1024}KB) \u2192 {asset_id[:12]}...")
        # Webflow benötigt fileId UND url für Image-Felder
        return {"fileId": asset_id, "url": cdn_url}

    except Exception as e:
        log.warning(f"    Upload-Fehler für {img_url[-50:]}: {e}")
        return None


# ─────────────────────────────────────────────
# Justimmo API Client
# ─────────────────────────────────────────────
class JustimmoClient:
    def __init__(self, user: str, password: str):
        self.session = requests.Session()
        self.session.auth = (user, password)
        self.session.headers.update({"Accept": "application/xml"})

    def get_all_ids(self) -> list:
        url = f"{JUSTIMMO_BASE}/objekt/ids"
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        try:
            ids = resp.json()
            return [str(i) for i in ids]
        except Exception:
            root = ET.fromstring(resp.content)
            return [node.text for node in root.findall(".//id") if node.text]

    def get_realty_detail(self, objekt_id: str) -> Optional[ET.Element]:
        url = f"{JUSTIMMO_BASE}/objekt/detail"
        resp = self.session.get(url, params={
            "objekt_id": objekt_id,
            "picturesize": "big",
            "culture": "de",
        }, timeout=30)
        if not resp.ok:
            log.warning(f"  Detail-API Fehler für {objekt_id}: {resp.status_code}")
            return None
        root = ET.fromstring(resp.content)
        realties = root.findall(".//immobilie")
        return realties[0] if realties else None


# ─────────────────────────────────────────────
# Webflow API Client
# ─────────────────────────────────────────────
class WebflowClient:
    def __init__(self, token: str):
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "accept-version": "1.0.0",
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self._last_request = 0.0

    def _throttle(self):
        elapsed = time.time() - self._last_request
        if elapsed < WEBFLOW_RATE_DELAY:
            time.sleep(WEBFLOW_RATE_DELAY - elapsed)
        self._last_request = time.time()

    def get_collection_items(self, collection_id: str) -> list:
        items = []
        offset = 0
        while True:
            self._throttle()
            resp = self.session.get(
                f"{WEBFLOW_BASE}/collections/{collection_id}/items",
                params={"limit": 100, "offset": offset}
            )
            resp.raise_for_status()
            data = resp.json()
            batch = data.get("items", [])
            items.extend(batch)
            if len(items) >= data.get("pagination", {}).get("total", 0):
                break
            if not batch:
                break
            offset += 100
        return items

    def create_item(self, collection_id: str, field_data: dict, dry_run: bool = False) -> Optional[dict]:
        if dry_run:
            log.info(f"  [DRY-RUN] Würde erstellen: {field_data.get('name', '?')}")
            return {"id": "dry-run"}
        self._throttle()
        resp = self.session.post(
            f"{WEBFLOW_BASE}/collections/{collection_id}/items",
            json={"isArchived": False, "isDraft": False, "fieldData": field_data}
        )
        if not resp.ok:
            log.error(f"  Fehler beim Erstellen: {resp.status_code} – {resp.text[:300]}")
            return None
        return resp.json()

    def update_item(self, collection_id: str, item_id: str, field_data: dict, dry_run: bool = False) -> Optional[dict]:
        if dry_run:
            log.info(f"  [DRY-RUN] Würde aktualisieren: {field_data.get('name', '?')}")
            return {"id": item_id}
        self._throttle()
        resp = self.session.patch(
            f"{WEBFLOW_BASE}/collections/{collection_id}/items/{item_id}",
            json={"isArchived": False, "isDraft": False, "fieldData": field_data}
        )
        if not resp.ok:
            log.error(f"  Fehler beim Aktualisieren: {resp.status_code} – {resp.text[:300]}")
            return None
        return resp.json()

    def publish_collection(self, collection_id: str, item_ids: list, dry_run: bool = False):
        if dry_run or not item_ids:
            return
        self._throttle()
        resp = self.session.post(
            f"{WEBFLOW_BASE}/collections/{collection_id}/items/publish",
            json={"itemIds": item_ids}
        )
        if not resp.ok:
            log.warning(f"  Veröffentlichung fehlgeschlagen: {resp.status_code} – {resp.text[:200]}")

    def delete_item(self, collection_id: str, item_id: str, dry_run: bool = False) -> bool:
        if dry_run:
            log.info(f"  [DRY-RUN] Würde löschen: {item_id}")
            return True
        self._throttle()
        resp = self.session.delete(
            f"{WEBFLOW_BASE}/collections/{collection_id}/items/{item_id}"
        )
        if resp.ok:
            return True
        log.error(f"  Fehler beim Löschen {item_id}: {resp.status_code} – {resp.text[:200]}")
        return False


# ─────────────────────────────────────────────
# Lookup-Maps
# ─────────────────────────────────────────────
def build_lookup_map(wf: WebflowClient, collection_id: str, key_field: str = "name") -> dict:
    items = wf.get_collection_items(collection_id)
    result = {}
    for item in items:
        fd = item.get("fieldData", {})
        key = fd.get(key_field, "")
        if key:
            result[key] = item["id"]
    return result


def build_justimmo_id_map(wf: WebflowClient) -> tuple:
    items = wf.get_collection_items(COL_PROPERTIES)
    jm_map = {}
    featured_map = {}
    for item in items:
        fd = item.get("fieldData", {})
        jm_id = fd.get("justimmo-id", "")
        if jm_id:
            jm_map[str(jm_id)] = item["id"]
        featured_map[item["id"]] = fd.get("feature-property", False)
    return jm_map, featured_map


def ensure_reference_item(wf: WebflowClient, collection_id: str,
                           name: str, lookup: dict, dry_run: bool) -> Optional[str]:
    if not name:
        return None
    if name in lookup:
        return lookup[name]
    result = wf.create_item(collection_id, {"name": name, "slug": slugify(name)}, dry_run)
    if result and result.get("id") and result["id"] != "dry-run":
        lookup[name] = result["id"]
        return result["id"]
    return None


# ─────────────────────────────────────────────
# Bild-Extraktion aus OpenImmo XML
# ─────────────────────────────────────────────
def extract_images(realty: ET.Element) -> tuple:
    """
    Extrahiert Bild-URLs aus dem OpenImmo XML.
    
    Rückgabe: (cover_url, galerie_urls)
      cover_url:    URL des TITELBILD (echtes Titelbild, fullhd)
      galerie_urls: Liste der BILD-URLs ab Index 1 (BILD[0] = Agenten-PNG, überspringen!)
    """
    titelbild_urls = []
    bild_urls = []

    for pic_node in realty.findall("anhaenge/anhang"):
        gruppe = (pic_node.get("gruppe") or "BILD").upper()
        if gruppe not in ("TITELBILD", "BILD"):
            continue
        # fullhd (1920×1080px) bevorzugen, dann big, dann pfad
        url = (pic_node.findtext("daten/fullhd")
               or pic_node.findtext("daten/big")
               or pic_node.findtext("daten/pfad"))
        if not url or not url.strip():
            continue
        url = url.strip()
        if gruppe == "TITELBILD":
            titelbild_urls.append(url)
        else:
            bild_urls.append(url)

    cover_url = titelbild_urls[0] if titelbild_urls else None
    # BILD[0] ist immer das Agenten-PNG → überspringen!
    galerie_urls = bild_urls[1:] if len(bild_urls) > 1 else bild_urls
    # Fallback: kein Titelbild → erstes verfügbares Galerie-Bild als Cover verwenden
    if not cover_url and galerie_urls:
        cover_url = galerie_urls[0]

    return cover_url, galerie_urls


# ─────────────────────────────────────────────
# Daten-Mapping: Justimmo OpenImmo XML → Webflow Fields
# ─────────────────────────────────────────────
def map_realty_to_webflow(realty: ET.Element,
                           type_map: dict,
                           category_map: dict,
                           location_map: dict,
                           wf_headers: dict,
                           dry_run: bool = False) -> dict:
    """
    Konvertiert ein Justimmo OpenImmo-XML-Element in ein Webflow fieldData-Dict.
    Lädt Bilder als Webflow Assets hoch und verwendet fileId.
    """
    # ── IDs ──────────────────────────────────────────────────────
    objekt_id = xml_text(realty, "verwaltung_techn/objektnr_intern")
    objektnr  = xml_text(realty, "verwaltung_techn/objektnr_extern")

    # ── Titel & Beschreibung ──────────────────────────────────────
    titel = xml_text(realty, "freitexte/objekttitel") or f"Objekt {objektnr or objekt_id}"
    beschreibung_raw = xml_text(realty, "freitexte/objektbeschreibung")
    ausstattung_raw  = xml_text(realty, "freitexte/ausstattung")
    lage_raw         = xml_text(realty, "freitexte/lage")

    # Alle Freitexte zusammenführen (plain text)
    beschreibung_parts = []
    if beschreibung_raw:
        beschreibung_parts.append(strip_html(beschreibung_raw))
    if ausstattung_raw:
        beschreibung_parts.append(strip_html(ausstattung_raw))
    if lage_raw:
        beschreibung_parts.append(strip_html(lage_raw))

    beschreibung = " ".join(beschreibung_parts).strip()[:8000] if beschreibung_parts else ""

    # ── Adresse ───────────────────────────────────────────────────
    plz  = xml_text(realty, "geo/plz")
    ort  = xml_text(realty, "geo/ort")
    location_str = ", ".join(filter(None, [plz, ort]))

    # ── Preise ────────────────────────────────────────────────────
    kaufpreis = xml_text(realty, "preise/kaufpreis")
    warmmiete = xml_text(realty, "preise/warmmiete")
    kaltmiete = xml_text(realty, "preise/kaltmiete")
    preis_str = ""
    try:
        if kaufpreis and float(kaufpreis) > 0:
            preis_str = f"€ {float(kaufpreis):,.0f}".replace(",", ".")
        elif warmmiete and float(warmmiete) > 0:
            preis_str = f"€ {float(warmmiete):,.0f}"
        elif kaltmiete and float(kaltmiete) > 0:
            preis_str = f"€ {float(kaltmiete):,.0f}"
    except ValueError:
        pass

    # ── Flächen & Zimmer ──────────────────────────────────────────
    wohnflaeche  = xml_float(realty, "flaechen/wohnflaeche")
    nutzflaeche  = xml_float(realty, "flaechen/nutzflaeche")
    grundflaeche = (xml_float(realty, "flaechen/grundstuecksflaeche")
                   or xml_float(realty, "flaechen/grundflaeche"))
    flaeche_val  = wohnflaeche or nutzflaeche or grundflaeche
    flaeche_str  = f"{flaeche_val:g} m²" if flaeche_val else ""

    zimmer  = xml_text(realty, "flaechen/anzahl_zimmer")
    baeder  = xml_text(realty, "flaechen/anzahl_badezimmer")
    parking = (xml_text(realty, "flaechen/anzahl_stellplaetze")
               or xml_text(realty, "flaechen/anzahl_garagen"))

    # ── Objektart / Typ ───────────────────────────────────────────
    objektart_name = xml_text(realty, "objektkategorie/user_defined_simplefield[@feldname='objektart_name']")
    vermarktung_kauf  = realty.find("objektkategorie/vermarktungsart[@KAUF='1']") is not None
    vermarktung_miete = realty.find("objektkategorie/vermarktungsart[@MIETE_PACHT='1']") is not None
    # WICHTIG: Bestehende Webflow-Kategorien heißen 'Kaufen', 'Mieten', 'Anlage' (nicht 'Kauf'/'Miete')
    kategorie_name = "Kaufen" if vermarktung_kauf else ("Mieten" if vermarktung_miete else "")

    # ── Agent ─────────────────────────────────────────────────────
    agent_id    = xml_text(realty, "kontaktperson/id")
    agent_wf_id = AGENT_MAP.get(agent_id)

    # ── Bundesland für Standort-Filter ───────────────────────────
    bundesland = xml_text(realty, "geo/bundesland") or ""

    # ── Referenz-IDs ─────────────────────────────────────────────
    type_wf_id     = type_map.get(objektart_name)
    category_wf_id = category_map.get(kategorie_name)
    # Standort = Bundesland (nicht Ort), damit der Filter auf der Website korrekt funktioniert
    location_wf_id = location_map.get(bundesland)

    # ── Bilder: Extrahieren und als Webflow Assets hochladen ──────
    cover_url, galerie_urls = extract_images(realty)

    cover_asset   = None
    galerie_assets = []

    if not dry_run:
        # Cover-Image hochladen
        if cover_url:
            fname = f"cover-{objektnr or objekt_id}.jpg"
            cover_asset = upload_image_to_webflow(cover_url, fname, wf_headers)
            time.sleep(0.5)  # kurze Pause nach Upload

        # Galerie-Bilder hochladen (max. 4)
        for idx, img_url in enumerate(galerie_urls[:4], 1):
            fname = f"gallery-{objektnr or objekt_id}-{idx}.jpg"
            asset = upload_image_to_webflow(img_url, fname, wf_headers)
            if asset:
                galerie_assets.append(asset)
            time.sleep(0.5)
    else:
        # Dry-Run: URL-Referenzen verwenden
        if cover_url:
            cover_asset = {"url": cover_url}
        galerie_assets = [{"url": u} for u in galerie_urls[:4]]

    # ── Weitere Felder ────────────────────────────────────────────
    etage     = xml_text(realty, "flaechen/etage")
    baujahr   = xml_text(realty, "zustand_angaben/baujahr")
    verfuegbar= xml_text(realty, "verwaltung_objekt/verfuegbar_ab")
    provision = xml_text(realty, "preise/aussen_courtage")
    bk_raw    = xml_text(realty, "preise/zusatzkosten/betriebskosten/brutto")

    # ── field_data zusammenstellen ────────────────────────────────
    field_data = {
        "name":               titel,
        "property-location":  location_str,
        "property-price":     preis_str,
        "property-area":      flaeche_str,
        "property-beds":      zimmer,
        "property-bathrooms": baeder,
        "property-parking":   parking,
        "feature-property":   False,
        "objektnummer":       objektnr,
    }

    # Beschreibung nur wenn vorhanden (Pflichtfeld!)
    if beschreibung:
        field_data["property-overview"] = beschreibung

    # Bilder (fileId für Webflow Asset, url als Fallback)
    if cover_asset:
        field_data["property-cover-image"] = cover_asset
    for i, asset in enumerate(galerie_assets, 1):
        field_data[f"small-image-{i}"] = asset

    # Referenz-Felder
    if type_wf_id:
        field_data["property-type"] = type_wf_id
    if category_wf_id:
        field_data["property-categories"] = category_wf_id
    if location_wf_id:
        field_data["property-locations"] = location_wf_id
    if agent_wf_id:
        field_data["agent-detail"] = agent_wf_id

    # Optionale Felder (nur Felder die in Webflow existieren)
    # Entfernt: etage, baujahr, verfuegbar-ab, betriebskosten (Felder in Webflow gelöscht am 24.03.2026)
    if provision:  field_data["provision"] = provision

    return field_data


# ─────────────────────────────────────────────
# filter-data.js auf GitHub pushen
# ─────────────────────────────────────────────
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPO  = "ImmoHub/justimmo-webflow-sync"
GITHUB_FILE  = "filter-data.js"


def push_filter_data(wf: "WebflowClient", jm_id_map: dict, category_map: dict, location_map: dict):
    """
    Erstellt filter-data.js mit Slug → {k: kategorie, l: standort} Mapping
    und pusht sie auf GitHub. Wird vom Frontend-Script geladen.
    """
    if not GITHUB_TOKEN:
        log.warning("  GITHUB_TOKEN nicht gesetzt – filter-data.js wird nicht aktualisiert")
        return

    log.info("\n[6/6] Aktualisiere filter-data.js auf GitHub...")

    try:
        # Alle Properties mit Slug, Kategorie und Standort laden
        items = wf.get_collection_items(COL_PROPERTIES)

        # Umgekehrte Maps: ID → Name
        cat_by_id = {v: k for k, v in category_map.items()}
        loc_by_id = {v: k for k, v in location_map.items()}

        mapping = {}
        for item in items:
            fd = item.get("fieldData", {})
            slug = fd.get("slug", "")
            cat_id = fd.get("property-categories", "")
            loc_id = fd.get("property-locations", "")
            if slug:
                mapping[slug] = {
                    "k": cat_by_id.get(cat_id, "").lower(),
                    "l": loc_by_id.get(loc_id, "")
                }

        import json as _json
        import base64 as _b64
        js_content = f"window.IR_FILTER_DATA = {_json.dumps(mapping, ensure_ascii=False)};\n"

        # SHA der bestehenden Datei holen
        gh_headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json"
        }
        r = requests.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}",
            headers=gh_headers, timeout=15
        )
        sha = r.json().get("sha") if r.status_code == 200 else None

        payload = {
            "message": f"filter-data.js: {len(mapping)} Immobilien",
            "content": _b64.b64encode(js_content.encode()).decode()
        }
        if sha:
            payload["sha"] = sha

        r = requests.put(
            f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}",
            headers=gh_headers, json=payload, timeout=15
        )
        if r.status_code in (200, 201):
            log.info(f"  ✓ filter-data.js aktualisiert ({len(mapping)} Einträge)")
        else:
            log.warning(f"  GitHub Push Fehler {r.status_code}: {r.text[:200]}")

    except Exception as e:
        log.warning(f"  filter-data.js Push fehlgeschlagen: {e}")


# ─────────────────────────────────────────────
# Hauptsynchronisation
# ─────────────────────────────────────────────
def sync(dry_run: bool = False, max_items: int = None):
    log.info("=" * 60)
    log.info("Justimmo → Webflow Synchronisation v3 gestartet")
    log.info(f"Modus: {'DRY-RUN (keine Änderungen)' if dry_run else 'LIVE'}")
    log.info("=" * 60)

    jm = JustimmoClient(JUSTIMMO_USER, JUSTIMMO_PASS)
    wf = WebflowClient(WEBFLOW_TOKEN)

    # ── Schritt 1: Webflow-Collections laden ──────────────────────
    log.info("\n[1/5] Lade bestehende Webflow-Collections...")
    type_map     = build_lookup_map(wf, COL_TYPES)
    category_map = build_lookup_map(wf, COL_CATEGORIES)
    location_map = build_lookup_map(wf, COL_LOCATIONS)
    jm_id_map, featured_map = build_justimmo_id_map(wf)

    log.info(f"  Typen: {len(type_map)}, Kategorien: {len(category_map)}, "
             f"Standorte: {len(location_map)}, Immobilien: {len(jm_id_map)}")

    # ── Schritt 2: Alle Justimmo-IDs holen ────────────────────────
    log.info("\n[2/5] Hole alle aktiven Justimmo-IDs...")
    all_ids = jm.get_all_ids()
    if max_items:
        all_ids = all_ids[:max_items]
    log.info(f"  {len(all_ids)} aktive Immobilien gefunden")

    # ── Schritt 3: Referenz-Collections ───────────────────────────
    log.info("\n[3/5] Synchronisiere Referenz-Collections...")
    created_ids = {COL_TYPES: [], COL_CATEGORIES: [], COL_LOCATIONS: [], COL_PROPERTIES: []}

    # ── Schritt 4: Immobilien synchronisieren ─────────────────────
    log.info("\n[4/5] Synchronisiere Immobilien...")
    stats = {"neu": 0, "aktualisiert": 0, "fehler": 0}

    for i, objekt_id in enumerate(all_ids, 1):
        log.info(f"  [{i}/{len(all_ids)}] Lade Detail für ID {objekt_id}...")

        try:
            realty = jm.get_realty_detail(objekt_id)
            if realty is None:
                log.warning(f"  Kein Detail für {objekt_id} – übersprungen")
                stats["fehler"] += 1
                continue

            titel      = xml_text(realty, "freitexte/objekttitel") or f"Objekt {objekt_id}"
            objektnr   = xml_text(realty, "verwaltung_techn/objektnr_extern")
            ort        = xml_text(realty, "geo/ort")
            bundesland = xml_text(realty, "geo/bundesland") or ""
            objektart  = xml_text(realty, "objektkategorie/user_defined_simplefield[@feldname='objektart_name']")
            kauf       = realty.find("objektkategorie/vermarktungsart[@KAUF='1']") is not None
            miete      = realty.find("objektkategorie/vermarktungsart[@MIETE_PACHT='1']") is not None
            # WICHTIG: Bestehende Webflow-Kategorien heißen 'Kaufen', 'Mieten', 'Anlage'
            kategorie  = "Kaufen" if kauf else ("Mieten" if miete else "")

            log.info(f"  [{i}/{len(all_ids)}] {titel} (Nr: {objektnr}, Ort: {ort}, Bundesland: {bundesland})")

            # Referenz-Items sicherstellen
            # Typen: NUR die 4 erlaubten Objekttypen verknüpfen, alle anderen ignorieren
            ALLOWED_TYPES = {"Zinshaus / Renditeobjekt", "Haus", "Wohnung", "Grundstück"}
            if objektart and objektart in ALLOWED_TYPES:
                wf_id = ensure_reference_item(wf, COL_TYPES, objektart, type_map, dry_run)
                if wf_id and wf_id not in created_ids[COL_TYPES]:
                    created_ids[COL_TYPES].append(wf_id)
            elif objektart:
                log.info(f"  Objekttyp '{objektart}' nicht in Whitelist – wird ignoriert")
            # Kategorien: NUR Kaufen und Mieten erlaubt, Anlage ignorieren
            ALLOWED_CATEGORIES = {"Kaufen", "Mieten"}
            if kategorie and kategorie in ALLOWED_CATEGORIES and kategorie in category_map:
                if category_map[kategorie] not in created_ids[COL_CATEGORIES]:
                    created_ids[COL_CATEGORIES].append(category_map[kategorie])
            elif kategorie and kategorie not in ALLOWED_CATEGORIES:
                log.info(f"  Kategorie '{kategorie}' nicht erlaubt – wird ignoriert")
            elif kategorie:
                log.warning(f"  Kategorie '{kategorie}' nicht in Webflow gefunden – übersprungen")
            # Standorte: NUR Wien und Niederösterreich erlaubt
            ALLOWED_LOCATIONS = {"Wien", "Niederösterreich"}
            if bundesland and bundesland in ALLOWED_LOCATIONS and bundesland in location_map:
                if location_map[bundesland] not in created_ids[COL_LOCATIONS]:
                    created_ids[COL_LOCATIONS].append(location_map[bundesland])
            elif bundesland and bundesland not in ALLOWED_LOCATIONS:
                log.info(f"  Bundesland '{bundesland}' nicht erlaubt – wird ignoriert")
            elif bundesland:
                log.warning(f"  Bundesland '{bundesland}' nicht in Webflow gefunden – übersprungen")

            # Field-Data aufbauen (inkl. Bild-Upload)
            field_data = map_realty_to_webflow(
                realty, type_map, category_map, location_map,
                wf.headers, dry_run
            )

            if objekt_id in jm_id_map:
                # Aktualisieren – Slug NICHT senden!
                item_id = jm_id_map[objekt_id]
                field_data["feature-property"] = featured_map.get(item_id, False)
                field_data.pop("slug", None)
                result = wf.update_item(COL_PROPERTIES, item_id, field_data, dry_run)
                if result:
                    stats["aktualisiert"] += 1
                    created_ids[COL_PROPERTIES].append(item_id)
                    log.info(f"  ✓ Aktualisiert: {titel}")
                else:
                    stats["fehler"] += 1
            else:
                # Neu erstellen – Slug nur beim Create
                slug = slugify(f"immobilie-{objektnr or objekt_id}")
                field_data["slug"] = slug
                result = wf.create_item(COL_PROPERTIES, field_data, dry_run)
                if result and result.get("id"):
                    stats["neu"] += 1
                    jm_id_map[objekt_id] = result["id"]
                    created_ids[COL_PROPERTIES].append(result["id"])
                    log.info(f"  ✓ Neu angelegt: {titel}")
                else:
                    stats["fehler"] += 1

        except Exception as e:
            log.error(f"  Fehler bei Objekt {objekt_id}: {e}", exc_info=True)
            stats["fehler"] += 1

        time.sleep(0.3)

    # ── Schritt 5: Deaktivierte Objekte löschen ──────────────────────
    log.info("\n[5/6] Lösche deaktivierte Objekte aus Webflow...")
    active_jm_ids = set(all_ids)  # alle aktiven Justimmo-IDs
    deleted_count = 0
    for jm_id, wf_item_id in list(jm_id_map.items()):
        if jm_id not in active_jm_ids:
            log.info(f"  Lösche deaktiviertes Objekt: Justimmo-ID {jm_id} (Webflow: {wf_item_id})")
            if wf.delete_item(COL_PROPERTIES, wf_item_id, dry_run):
                stats["geloescht"] = stats.get("geloescht", 0) + 1
                deleted_count += 1
                del jm_id_map[jm_id]
            else:
                stats["fehler"] += 1
    log.info(f"  {deleted_count} deaktivierte Objekte gelöscht")

    # ── Schritt 6: Veröffentlichen ─────────────────────────────
    log.info("\n[6/6] Veröffentliche geänderte Items...")
    for col_id, ids in created_ids.items():
        if ids:
            for chunk in [ids[j:j+100] for j in range(0, len(ids), 100)]:
                wf.publish_collection(col_id, chunk, dry_run)
            log.info(f"  Collection {col_id}: {len(ids)} Items veröffentlicht")

    # ── Schritt 7: filter-data.js auf GitHub aktualisieren ────
    if not dry_run:
        push_filter_data(wf, jm_id_map, category_map, location_map)

    # ── Zusammenfassung ────────────────────────────────────────────
    log.info("\n" + "=" * 60)
    log.info("SYNC ABGESCHLOSSEN")
    log.info(f"  Neu erstellt:    {stats['neu']}")
    log.info(f"  Aktualisiert:    {stats['aktualisiert']}")
    log.info(f"  Gelöscht:        {stats.get('geloescht', 0)}")
    log.info(f"  Fehler:          {stats['fehler']}")
    log.info("=" * 60)


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Justimmo → Webflow Synchronisation v3")
    parser.add_argument("--dry-run", action="store_true",
                        help="Nur simulieren, keine Änderungen vornehmen")
    parser.add_argument("--limit", type=int, default=None,
                        help="Maximale Anzahl zu synchronisierender Immobilien")
    args = parser.parse_args()

    sync(dry_run=args.dry_run, max_items=args.limit)
