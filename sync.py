#!/usr/bin/env python3
"""
Justimmo → Webflow CMS Synchronisation
=======================================
Liest alle aktiven Immobilien von der Justimmo API (OpenImmo-Format)
und importiert/aktualisiert sie vollständig in die Webflow CMS Collections.

XML-Struktur: OpenImmo-Format
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

Verwendung:
  python3 sync.py                  # Vollsynchronisation
  python3 sync.py --dry-run        # Nur anzeigen, nichts schreiben
  python3 sync.py --limit 10       # Nur 10 Objekte synchronisieren
"""

import os
import sys
import time
import logging
import argparse
import unicodedata
import re
import xml.etree.ElementTree as ET
from typing import Optional

import requests

# ─────────────────────────────────────────────
# Konfiguration
# ─────────────────────────────────────────────
JUSTIMMO_USER     = os.getenv("JUSTIMMO_USER", "api-97120")
JUSTIMMO_PASS     = os.getenv("JUSTIMMO_PASS", "cScKIP9TW2")
JUSTIMMO_BASE     = "https://api.justimmo.at/rest/v1"

WEBFLOW_TOKEN     = os.getenv("WEBFLOW_TOKEN", "")  # Wird aus Umgebungsvariable gelesen
WEBFLOW_BASE      = "https://api.webflow.com/v2"
WEBFLOW_SITE_ID   = "699f29df3ecf1945550ca280"

# Collection IDs
COL_PROPERTIES    = "699f29e03ecf1945550ca36c"
COL_AGENTS        = "699f29e03ecf1945550ca38b"
COL_LOCATIONS     = "699f29e03ecf1945550ca3c6"
COL_CATEGORIES    = "699f29e03ecf1945550ca3da"
COL_TYPES         = "699f29e03ecf1945550ca3e1"

# Justimmo Kontaktperson-ID → Webflow Agent-ID Mapping
# (Ermittelt aus bestehenden Webflow-Items)
AGENT_MAP = {
    "2009026":  "69a6ee9c6ee9509b8be4d895",  # Harald Grassler
    "16385723": "69a6eea0f083679bed8045a9",  # Mario Schmid
    "22260941": "69a6eea3f9234d30c8dc0c9d",  # Nataliya Schweda
    "26293524": "69a6ee9e982c3f7f3c614e9a",  # Sascha Nevoral
}

# Rate-Limiting: Webflow erlaubt 60 Requests/Minute
WEBFLOW_RATE_DELAY = 1.1  # Sekunden zwischen Requests

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
    """Erstellt einen URL-sicheren Slug aus einem beliebigen Text."""
    text = str(text).lower().strip()
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_-]+", "-", text)
    text = re.sub(r"^-+|-+$", "", text)
    return text or "objekt"


def xml_text(element, path: str, default: str = "") -> str:
    """Liest den Textinhalt eines XML-Elements sicher aus."""
    node = element.find(path)
    if node is not None and node.text:
        return node.text.strip()
    return default


def xml_float(element, path: str, default: float = 0.0) -> float:
    """Liest einen Float-Wert aus einem XML-Element."""
    val = xml_text(element, path)
    try:
        return float(val.replace(",", "."))
    except (ValueError, AttributeError):
        return default


def strip_html(text: str) -> str:
    """Entfernt HTML-Tags aus einem Text."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


# ─────────────────────────────────────────────
# Justimmo API Client
# ─────────────────────────────────────────────
class JustimmoClient:
    def __init__(self, user: str, password: str):
        self.session = requests.Session()
        self.session.auth = (user, password)
        self.session.headers.update({"Accept": "application/xml"})

    def get_all_ids(self) -> list[str]:
        """Ruft alle aktiven Immobilien-IDs ab."""
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
        """Ruft die Detailansicht einer einzelnen Immobilie ab (OpenImmo-Format)."""
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
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "accept-version": "1.0.0",
        })
        self._last_request = 0.0

    def _throttle(self):
        """Rate-Limiting: max. 60 Requests/Minute."""
        elapsed = time.time() - self._last_request
        if elapsed < WEBFLOW_RATE_DELAY:
            time.sleep(WEBFLOW_RATE_DELAY - elapsed)
        self._last_request = time.time()

    def get_collection_items(self, collection_id: str) -> list[dict]:
        """Liest alle vorhandenen Items einer Collection aus."""
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
        """Erstellt ein neues CMS-Item."""
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
        """Aktualisiert ein bestehendes CMS-Item."""
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

    def publish_collection(self, collection_id: str, item_ids: list[str], dry_run: bool = False):
        """Veröffentlicht Items einer Collection."""
        if dry_run or not item_ids:
            return
        self._throttle()
        resp = self.session.post(
            f"{WEBFLOW_BASE}/collections/{collection_id}/items/publish",
            json={"itemIds": item_ids}
        )
        if not resp.ok:
            log.warning(f"  Veröffentlichung fehlgeschlagen: {resp.status_code} – {resp.text[:200]}")


# ─────────────────────────────────────────────
# Daten-Mapping: Justimmo OpenImmo XML → Webflow Fields
# ─────────────────────────────────────────────
def map_realty_to_webflow(realty: ET.Element,
                           type_map: dict,
                           category_map: dict,
                           location_map: dict) -> dict:
    """
    Konvertiert ein Justimmo OpenImmo-XML-Element in ein Webflow fieldData-Dict.
    
    OpenImmo-Pfade:
      ID:          verwaltung_techn/objektnr_intern
      Objektnr:    verwaltung_techn/objektnr_extern
      Titel:       freitexte/objekttitel
      Beschreibung:freitexte/objektbeschreibung
      PLZ:         geo/plz
      Ort:         geo/ort
      Preis (Kauf):preise/kaufpreis
      Gesamtmiete: preise/warmmiete
      Wohnfläche:  flaechen/wohnflaeche
      Zimmer:      flaechen/anzahl_zimmer
      Bäder:       flaechen/anzahl_badezimmer
      Typ:         objektkategorie/user_defined_simplefield[@feldname='objektart_name']
      Vermarktung: objektkategorie/vermarktungsart[@KAUF] / [@MIETE_PACHT]
      Agent-ID:    kontaktperson/id
      Bilder:      anhaenge/anhang[@gruppe='TITELBILD'] → Cover
                   anhaenge/anhang (ohne gruppe, BILD[1:]) → Galerie
    """
    # ── IDs ──────────────────────────────────────────────────────
    objekt_id  = xml_text(realty, "verwaltung_techn/objektnr_intern")
    objektnr   = xml_text(realty, "verwaltung_techn/objektnr_extern")

    # ── Titel & Beschreibung ──────────────────────────────────────
    titel = xml_text(realty, "freitexte/objekttitel") or f"Objekt {objektnr or objekt_id}"
    beschreibung_raw = xml_text(realty, "freitexte/objektbeschreibung")
    beschreibung = strip_html(beschreibung_raw)[:5000] if beschreibung_raw else ""

    # ── Adresse ───────────────────────────────────────────────────
    plz  = xml_text(realty, "geo/plz")
    ort  = xml_text(realty, "geo/ort")
    land = xml_text(realty, "geo/land")
    location_str = ", ".join(filter(None, [plz, ort]))

    # ── Koordinaten ───────────────────────────────────────────────
    lat = xml_text(realty, "geo/user_defined_simplefield[@feldname='geokoordinaten_breitengrad']")
    lng = xml_text(realty, "geo/user_defined_simplefield[@feldname='geokoordinaten_laengengrad']")

    # ── Preise ────────────────────────────────────────────────────
    kaufpreis   = xml_text(realty, "preise/kaufpreis")
    warmmiete   = xml_text(realty, "preise/warmmiete")
    kaltmiete   = xml_text(realty, "preise/kaltmiete")
    preis_str   = ""
    if kaufpreis and float(kaufpreis or 0) > 0:
        preis_str = f"€ {float(kaufpreis):,.0f}".replace(",", ".")
    elif warmmiete and float(warmmiete or 0) > 0:
        preis_str = f"€ {float(warmmiete):,.0f}"
    elif kaltmiete and float(kaltmiete or 0) > 0:
        preis_str = f"€ {float(kaltmiete):,.0f}"

    # ── Flächen & Zimmer ──────────────────────────────────────────
    wohnflaeche  = xml_float(realty, "flaechen/wohnflaeche")
    nutzflaeche  = xml_float(realty, "flaechen/nutzflaeche")
    grundflaeche = xml_float(realty, "flaechen/grundflaeche")
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
    kategorie_name = "Kauf" if vermarktung_kauf else ("Miete" if vermarktung_miete else "")

    # ── Agent ─────────────────────────────────────────────────────
    agent_id    = xml_text(realty, "kontaktperson/id")
    agent_wf_id = AGENT_MAP.get(agent_id)

    # ── Referenz-IDs ─────────────────────────────────────────────
    type_wf_id     = type_map.get(objektart_name)
    category_wf_id = category_map.get(kategorie_name)
    location_wf_id = location_map.get(ort)

    # ── Bilder ────────────────────────────────────────────────────
    # TITELBILD-Gruppe → Cover-Image (echtes Titelbild)
    # BILD-Gruppe[1:]  → Galerie (BILD[0] = Agenten-PNG, überspringen!)
    titelbild_urls = []
    bild_urls = []
    for pic_node in realty.findall("anhaenge/anhang"):
        gruppe = (pic_node.get("gruppe") or "BILD").upper()
        if gruppe not in ("TITELBILD", "BILD"):
            continue
        # fullhd bevorzugen (1920×1080px), dann big, dann pfad
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

    cover_url    = titelbild_urls[0] if titelbild_urls else None
    # BILD[0] ist immer das Agenten-PNG → überspringen
    galerie_urls = bild_urls[1:] if len(bild_urls) > 1 else bild_urls

    # ── Weitere Felder ────────────────────────────────────────────
    etage     = xml_text(realty, "flaechen/etage")
    baujahr   = xml_text(realty, "zustand_angaben/baujahr")
    verfuegbar= xml_text(realty, "verwaltung_objekt/verfuegbar_ab")
    provision = xml_text(realty, "preise/aussen_courtage")
    bk_raw    = xml_text(realty, "preise/zusatzkosten/betriebskosten/brutto")

    # ── field_data zusammenstellen ────────────────────────────────
    field_data = {
        "name":              titel,
        # Slug wird NICHT beim Update gesetzt → nur beim Create (siehe sync())
        "property-location": location_str,
        "property-price":    preis_str,
        "property-area":     flaeche_str,
        "property-beds":     zimmer,
        "property-bathrooms":baeder,
        "property-parking":  parking,
        "feature-property":  False,
        "justimmo-id":       objekt_id,
        "objektnummer":      objektnr,
    }

    # Beschreibung nur wenn vorhanden (Pflichtfeld!)
    if beschreibung:
        field_data["property-overview"] = beschreibung

    # Bilder
    if cover_url:
        field_data["property-cover-image"] = {"url": cover_url}
    for i, img_url in enumerate(galerie_urls[:4], 1):
        field_data[f"small-image-{i}"] = {"url": img_url}

    # Referenz-Felder (nur wenn ID vorhanden)
    if type_wf_id:
        field_data["property-type"] = type_wf_id
    if category_wf_id:
        field_data["property-categories"] = category_wf_id
    if location_wf_id:
        field_data["property-locations"] = location_wf_id
    if agent_wf_id:
        field_data["agent-detail"] = agent_wf_id

    # Optionale Felder
    if etage:      field_data["etage"]          = etage
    if baujahr:    field_data["baujahr"]         = baujahr
    if verfuegbar: field_data["verfuegbar-ab"]   = verfuegbar
    if provision:  field_data["provision"]       = provision
    if bk_raw:
        try:
            field_data["betriebskosten"] = f"€ {float(bk_raw):,.2f}"
        except ValueError:
            field_data["betriebskosten"] = bk_raw

    return field_data


# ─────────────────────────────────────────────
# Lookup-Maps: Name → Webflow Item ID
# ─────────────────────────────────────────────
def build_lookup_map(wf: WebflowClient, collection_id: str, key_field: str = "name") -> dict:
    """Erstellt ein Dict {feldwert: webflow_item_id} für schnelle Suche."""
    items = wf.get_collection_items(collection_id)
    result = {}
    for item in items:
        fd = item.get("fieldData", {})
        key = fd.get(key_field, "")
        if key:
            result[key] = item["id"]
    return result


def build_justimmo_id_map(wf: WebflowClient) -> tuple[dict, dict]:
    """
    Erstellt ein Dict {justimmo_id: webflow_item_id} und {item_id: feature-property}.
    Verwendet justimmo-id Feld für zuverlässige Zuordnung.
    """
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
    """Stellt sicher, dass ein Referenz-Item existiert, erstellt es ggf. neu."""
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
# Hauptsynchronisation
# ─────────────────────────────────────────────
def sync(dry_run: bool = False, max_items: int = None):
    log.info("=" * 60)
    log.info("Justimmo → Webflow Synchronisation gestartet")
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

    # ── Schritt 3: Referenz-Collections befüllen ──────────────────
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

            titel    = xml_text(realty, "freitexte/objekttitel") or f"Objekt {objekt_id}"
            objektnr = xml_text(realty, "verwaltung_techn/objektnr_extern")
            ort      = xml_text(realty, "geo/ort")
            objektart= xml_text(realty, "objektkategorie/user_defined_simplefield[@feldname='objektart_name']")
            kauf     = realty.find("objektkategorie/vermarktungsart[@KAUF='1']") is not None
            miete    = realty.find("objektkategorie/vermarktungsart[@MIETE_PACHT='1']") is not None
            kategorie= "Kauf" if kauf else ("Miete" if miete else "")

            log.info(f"  [{i}/{len(all_ids)}] {titel} (Nr: {objektnr}, Ort: {ort})")

            # Referenz-Items sicherstellen
            if objektart:
                wf_id = ensure_reference_item(wf, COL_TYPES, objektart, type_map, dry_run)
                if wf_id and wf_id not in created_ids[COL_TYPES]:
                    created_ids[COL_TYPES].append(wf_id)
            if kategorie:
                wf_id = ensure_reference_item(wf, COL_CATEGORIES, kategorie, category_map, dry_run)
                if wf_id and wf_id not in created_ids[COL_CATEGORIES]:
                    created_ids[COL_CATEGORIES].append(wf_id)
            if ort:
                wf_id = ensure_reference_item(wf, COL_LOCATIONS, ort, location_map, dry_run)
                if wf_id and wf_id not in created_ids[COL_LOCATIONS]:
                    created_ids[COL_LOCATIONS].append(wf_id)

            # Field-Data aufbauen
            field_data = map_realty_to_webflow(realty, type_map, category_map, location_map)

            if objekt_id in jm_id_map:
                # Aktualisieren
                item_id = jm_id_map[objekt_id]
                field_data["feature-property"] = featured_map.get(item_id, False)
                # WICHTIG: Slug NICHT beim Update senden!
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
            log.error(f"  Fehler bei Objekt {objekt_id}: {e}")
            stats["fehler"] += 1

        # Kurze Pause zwischen Detail-API-Aufrufen
        time.sleep(0.3)

    # ── Schritt 5: Veröffentlichen ─────────────────────────────────
    log.info("\n[5/5] Veröffentliche geänderte Items...")
    for col_id, ids in created_ids.items():
        if ids:
            for chunk in [ids[j:j+100] for j in range(0, len(ids), 100)]:
                wf.publish_collection(col_id, chunk, dry_run)
            log.info(f"  Collection {col_id}: {len(ids)} Items veröffentlicht")

    # ── Zusammenfassung ────────────────────────────────────────────
    log.info("\n" + "=" * 60)
    log.info("SYNC ABGESCHLOSSEN")
    log.info(f"  Neu erstellt:    {stats['neu']}")
    log.info(f"  Aktualisiert:    {stats['aktualisiert']}")
    log.info(f"  Fehler:          {stats['fehler']}")
    log.info("=" * 60)


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Justimmo → Webflow Synchronisation")
    parser.add_argument("--dry-run", action="store_true",
                        help="Nur simulieren, keine Änderungen vornehmen")
    parser.add_argument("--limit", type=int, default=None,
                        help="Maximale Anzahl zu synchronisierender Immobilien")
    args = parser.parse_args()

    sync(dry_run=args.dry_run, max_items=args.limit)
