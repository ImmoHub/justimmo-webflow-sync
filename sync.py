#!/usr/bin/env python3
"""
Justimmo → Webflow CMS Synchronisation
=======================================
Liest alle aktiven Immobilien von der Justimmo API und importiert/aktualisiert
sie vollständig in die Webflow CMS Collections.

Unterstützte Collections:
  - Immobilien (Properties)        → Hauptobjekte
  - Immobilientypen (Propert Types)
  - Kategorien (Property Categories)
  - Standorte (Property Locations)
  - Makler (Agents)

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
JUSTIMMO_USER     = os.getenv("JUSTIMMO_USER", "api-871120")
JUSTIMMO_PASS     = os.getenv("JUSTIMMO_PASS", "")          # Aus .env oder Umgebungsvariable
JUSTIMMO_BASE     = "https://api.justimmo.at/rest/v1"
JUSTIMMO_PICSIZE  = "big"                                    # Bildgröße: small | medium | big

WEBFLOW_TOKEN     = os.getenv("WEBFLOW_TOKEN", "8424b93453944e22efb0d92da7e802f9011156a5d6109d4748eb7ec3097b8b7c")
WEBFLOW_BASE      = "https://api.webflow.com/v2"
WEBFLOW_SITE_ID   = "699f29df3ecf1945550ca280"

# Collection IDs
COL_PROPERTIES    = "699f29e03ecf1945550ca36c"
COL_AGENTS        = "699f29e03ecf1945550ca38b"
COL_LOCATIONS     = "699f29e03ecf1945550ca3c6"
COL_CATEGORIES    = "699f29e03ecf1945550ca3da"
COL_TYPES         = "699f29e03ecf1945550ca3e1"

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


# ─────────────────────────────────────────────
# Justimmo API Client
# ─────────────────────────────────────────────
class JustimmoClient:
    def __init__(self, user: str, password: str):
        self.session = requests.Session()
        self.session.auth = (user, password)
        self.session.headers.update({"Accept": "application/xml"})

    def get(self, endpoint: str, params: dict = None) -> ET.Element:
        url = f"{JUSTIMMO_BASE}/{endpoint}"
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return ET.fromstring(resp.content)

    def get_all_ids(self) -> list[str]:
        """Ruft alle Immobilien-IDs ab (ohne Limit)."""
        root = self.get("objekt/ids")
        # Antwort ist ein JSON-Array als Text, aber wir parsen XML-Wrapper
        # Alternativ direkt als JSON
        url = f"{JUSTIMMO_BASE}/objekt/ids"
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        try:
            ids = resp.json()
            return [str(i) for i in ids]
        except Exception:
            # Fallback: XML parsen
            root = ET.fromstring(resp.content)
            return [node.text for node in root.findall(".//id") if node.text]

    def get_realty_list(self, limit: int = 100, offset: int = 0) -> ET.Element:
        """Ruft eine paginierte Liste von Immobilien ab."""
        return self.get("objekt/list", {
            "limit": limit,
            "offset": offset,
            "showDetails": 1,
            "picturesize": JUSTIMMO_PICSIZE,
            "culture": "de",
        })

    def get_realty_detail(self, objekt_id: str) -> ET.Element:
        """Ruft die Detailansicht einer einzelnen Immobilie ab."""
        return self.get("objekt/detail", {
            "objekt_id": objekt_id,
            "picturesize": JUSTIMMO_PICSIZE,
            "culture": "de",
        })

    def get_all_realties(self, max_items: int = None) -> list[ET.Element]:
        """Ruft alle Immobilien paginiert ab."""
        all_realties = []
        offset = 0
        batch = 100

        log.info("Starte Abruf aller Immobilien von Justimmo...")
        while True:
            root = self.get_realty_list(limit=batch, offset=offset)
            count_node = root.find(".//count")
            total = int(count_node.text) if count_node is not None else 0

            realties = root.findall(".//immobilie")
            if not realties:
                break

            all_realties.extend(realties)
            log.info(f"  Abgerufen: {len(all_realties)}/{total}")

            if max_items and len(all_realties) >= max_items:
                all_realties = all_realties[:max_items]
                break

            if len(all_realties) >= total:
                break

            offset += batch
            time.sleep(0.5)

        log.info(f"Gesamt abgerufen: {len(all_realties)} Immobilien")
        return all_realties


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
# Daten-Mapping: Justimmo XML → Webflow Fields
# ─────────────────────────────────────────────
def map_realty_to_webflow(realty: ET.Element,
                           type_map: dict,
                           category_map: dict,
                           location_map: dict,
                           agent_map: dict) -> dict:
    """
    Konvertiert ein Justimmo XML-Immobilien-Element in ein Webflow fieldData-Dict.
    Alle Feldnamen entsprechen den bestehenden Webflow Collection-Slugs.
    """
    objekt_id = xml_text(realty, "id")
    titel     = xml_text(realty, "titel") or xml_text(realty, "objektnummer", f"Objekt {objekt_id}")

    # Preis
    preis       = xml_text(realty, "preis")
    gesamtmiete = xml_text(realty, "gesamtmiete")
    kaufpreis   = xml_text(realty, "kaufpreis")
    preis_str   = ""
    if kaufpreis and float(kaufpreis or 0) > 0:
        preis_str = f"€ {float(kaufpreis):,.0f}".replace(",", ".")
    elif gesamtmiete and float(gesamtmiete or 0) > 0:
        preis_str = f"€ {float(gesamtmiete):,.0f} / Monat"
    elif preis and float(preis or 0) > 0:
        preis_str = f"€ {float(preis):,.0f}"

    # Fläche
    wohnflaeche  = xml_float(realty, "wohnflaeche")
    nutzflaeche  = xml_float(realty, "nutzflaeche")
    grundflaeche = xml_float(realty, "grundflaeche")
    flaeche_val  = wohnflaeche or nutzflaeche or grundflaeche
    flaeche_str  = f"{flaeche_val:g} m²" if flaeche_val else ""

    # Zimmer & Bäder
    zimmer  = xml_text(realty, "anzahl_zimmer")
    baeder  = xml_text(realty, "anzahl_badezimmer")
    parking = xml_text(realty, "anzahl_stellplaetze") or xml_text(realty, "anzahl_garagen")

    # Adresse / Standort
    ort  = xml_text(realty, "ort")
    plz  = xml_text(realty, "plz")
    land = xml_text(realty, "land")
    location_str = ", ".join(filter(None, [plz, ort, land]))

    # Beschreibung
    beschreibung = xml_text(realty, "objektbeschreibung") or xml_text(realty, "dreizeiler")
    # HTML-Tags entfernen für PlainText
    beschreibung_plain = re.sub(r"<[^>]+>", " ", beschreibung).strip()
    beschreibung_plain = re.sub(r"\s+", " ", beschreibung_plain)

    # Bilder: TITELBILD-Gruppe als Cover, BILD-Gruppe[1:] als Galerie
    # WICHTIG: BILD[0] ist immer das Agenten-Foto (PNG) – wird NICHT verwendet!
    titelbild_urls = []
    bild_urls = []
    for pic_node in realty.findall(".//anhang"):
        gruppe = (pic_node.get("gruppe") or "BILD").upper()
        if gruppe not in ("TITELBILD", "BILD"):
            continue
        # fullhd bevorzugen (1920x1080), dann big, dann pfad
        url = (pic_node.findtext("daten/fullhd")
               or pic_node.findtext("daten/big")
               or pic_node.findtext("daten/pfad")
               or pic_node.findtext("pfad"))
        if not url or not url.strip():
            continue
        url = url.strip()
        if gruppe == "TITELBILD":
            titelbild_urls.append(url)
        else:
            bild_urls.append(url)

    # Cover = TITELBILD (1 pro Immobilie, echtes Titelbild)
    # Galerie = BILD[1:] (BILD[0] ist Agenten-PNG – überspringen)
    cover_url = titelbild_urls[0] if titelbild_urls else None
    galerie_urls = bild_urls[1:] if len(bild_urls) > 1 else bild_urls

    # Fallback: Kurzliste wenn gar nichts vorhanden
    if not cover_url and not galerie_urls:
        for tag in ["erstes_bild", "zweites_bild", "drittes_bild"]:
            url = xml_text(realty, tag)
            if url:
                galerie_urls.append(url)

    cover_image = {"url": cover_url} if cover_url else None

    # Objektart / Typ
    objektart_name = xml_text(realty, ".//user_defined_simplefield[@feldname='objektart_name']")
    sub_art_name   = xml_text(realty, ".//user_defined_simplefield[@feldname='sub_objektart_name']")
    vermarktung_kauf  = realty.find(".//vermarktungsart[@KAUF='1']") is not None
    vermarktung_miete = realty.find(".//vermarktungsart[@MIETE_PACHT='1']") is not None

    # Kategorie bestimmen (Kauf / Miete)
    kategorie_name = "Kauf" if vermarktung_kauf else ("Miete" if vermarktung_miete else "")

    # Ausstattungsmerkmale als Feature-Felder
    ausstattung = []
    for feat in realty.findall(".//ausstattung/*"):
        if feat.text and feat.text.strip() not in ("0", "false", ""):
            ausstattung.append(feat.tag.replace("_", " ").title())
    # Zusätzliche Merkmale aus user_defined_simplefield
    for f in realty.findall(".//user_defined_simplefield"):
        name = f.get("feldname", "")
        if name.startswith("ausstattung_") and f.text and f.text not in ("0",):
            ausstattung.append(f.text.strip())

    features = ausstattung[:5]  # Webflow hat 5 Feature-Felder

    # Makler / Agent
    # Die List-API liefert kontaktperson/id nicht zuverlässig.
    # Wir prüfen mehrere mögliche Pfade.
    makler_id = (xml_text(realty, ".//kontaktperson/id")
                 or xml_text(realty, "kontaktperson_id")
                 or xml_text(realty, "mitarbeiter_id")
                 or xml_text(realty, ".//user_defined_simplefield[@feldname='kontaktperson_id']"))
    agent_wf_id = agent_map.get(makler_id) if makler_id else None

    # Typ-Referenz
    type_wf_id     = type_map.get(objektart_name) or type_map.get(sub_art_name)
    category_wf_id = category_map.get(kategorie_name)
    location_wf_id = location_map.get(ort)

    # Slug generieren (eindeutig durch Objekt-ID)
    slug = slugify(f"{titel}-{objekt_id}")

    # Ausstattungs-Switches (Justimmo-spezifische Felder)
    def has_feature(tag: str) -> bool:
        node = realty.find(f".//{tag}")
        return node is not None and node.text not in (None, "0", "false", "")

    field_data = {
        "name":                 titel,
        # Slug wird NICHT beim Update gesetzt (Webflow-Validierungsfehler)
        # Wird separat beim Create hinzugefügt (siehe sync()-Funktion)
        "property-location":    location_str,
        "property-price":       preis_str,
        "property-area":        flaeche_str,
        "property-beds":        zimmer,
        "property-bathrooms":   baeder,
        "property-parking":     parking,
        "feature-property":     False,  # Standard: kein Featured
    }

    # property-overview nur setzen wenn Inhalt vorhanden (Pflichtfeld!)
    if beschreibung_plain:
        field_data["property-overview"] = beschreibung_plain[:5000]

    # Bilder hinzufügen
    if cover_image:
        field_data["property-cover-image"] = cover_image
    for i, img_url in enumerate(galerie_urls[:4], 1):
        field_data[f"small-image-{i}"] = {"url": img_url}

    # Feature-Texte
    for i, feat in enumerate(features, 1):
        field_data[f"feature-{i}"] = feat

    # Referenz-Felder (nur setzen wenn ID vorhanden)
    if type_wf_id:
        field_data["property-type"] = type_wf_id
    if category_wf_id:
        field_data["property-categories"] = category_wf_id
    if location_wf_id:
        field_data["property-locations"] = location_wf_id
    if agent_wf_id:
        field_data["agent-detail"] = agent_wf_id

    # Neue Justimmo-spezifische Felder
    etage    = xml_text(realty, "etage")
    baujahr  = xml_text(realty, "baujahr")
    zustand  = xml_text(realty, ".//user_defined_simplefield[@feldname='zustand_name']")
    heizung  = xml_text(realty, ".//user_defined_simplefield[@feldname='heizungsart_name']")
    hwb      = xml_text(realty, "hwb") or xml_text(realty, "energieausweis")
    bk       = xml_text(realty, "betriebskosten")
    provision= xml_text(realty, "provision")
    verfuegbar = xml_text(realty, "verfuegbar_ab") or xml_text(realty, "bezugsfrei_ab")

    if etage:     field_data["etage"]          = etage
    if baujahr:   field_data["baujahr"]        = baujahr
    if zustand:   field_data["zustand"]        = zustand
    if heizung:   field_data["heizung"]        = heizung
    if hwb:       field_data["energieausweis"] = hwb
    if bk:        field_data["betriebskosten"] = f"€ {float(bk):,.2f}" if bk.replace('.','').isdigit() else bk
    if provision: field_data["provision"]      = provision
    if verfuegbar:field_data["verfuegbar-ab"]  = verfuegbar

    # Justimmo-ID speichern
    field_data["justimmo-id"]   = objekt_id
    field_data["objektnummer"]  = xml_text(realty, "objektnummer")

    # Ausstattungs-Switches (Mapping Justimmo → Webflow)
    switch_map = {
        "tv":              ["tv", "fernseher"],
        "air-conditioner": ["klimaanlage", "klima"],
        "washing-machine": ["waschmaschine"],
        "internet":        ["internet", "wlan", "wifi"],
        "water-heater":    ["warmwasser", "boiler"],
        "refrigerator":    ["kuehlschrank", "kühlschrank"],
        "sofa":            ["sofa", "couch"],
        "wardrobe":        ["schrank", "kleiderschrank"],
        "gas":             ["gas", "gasheizung"],
        "kitchen":         ["kueche", "küche", "einbaukueche", "einbauküche"],
    }
    for wf_slug, keywords in switch_map.items():
        for kw in keywords:
            if has_feature(kw):
                field_data[wf_slug] = True
                break

    # Terrasse/Balkon, Keller, Aufzug
    terrassen = xml_float(realty, "anzahl_terrassen") + xml_float(realty, "anzahl_balkons") + xml_float(realty, "anzahl_loggias")
    if terrassen > 0:
        field_data["terrasse-balkon"] = True
    if xml_float(realty, "anzahl_keller") > 0:
        field_data["keller"] = True
    if xml_text(realty, "aufzug") not in ("", "0", "false"):
        field_data["aufzug"] = True
    if xml_text(realty, "barrierefrei") not in ("", "0", "false"):
        field_data["barrierefrei"] = True

    # Justimmo-ID als Referenz in Feature-1 speichern falls leer
    if not field_data.get("feature-1"):
        field_data["feature-1"] = f"Justimmo-ID: {objekt_id}"

    return field_data


def map_agent_to_webflow(kontakt: ET.Element) -> dict:
    """Konvertiert einen Justimmo-Kontakt/Makler in Webflow Agent-Felder."""
    vorname  = xml_text(kontakt, "vorname")
    nachname = xml_text(kontakt, "nachname")
    name     = f"{vorname} {nachname}".strip() or xml_text(kontakt, "name", "Unbekannt")
    tel      = xml_text(kontakt, "tel") or xml_text(kontakt, "mobile")
    email    = xml_text(kontakt, "email")
    titel    = xml_text(kontakt, "titel")
    firma    = xml_text(kontakt, "firma")

    return {
        "name":            name,
        "slug":            slugify(name),
        "agent-title":     titel or firma or "Makler",
        "agent-phone":     tel,
        "agent-email":     email,
        "about-agent":     xml_text(kontakt, "beschreibung"),
        "address-office":  xml_text(kontakt, "strasse"),
    }


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


def build_slug_map(wf: WebflowClient, collection_id: str) -> tuple[dict, dict]:
    """Erstellt ein Dict {slug: webflow_item_id} und {item_id: feature-property-wert}."""
    items = wf.get_collection_items(collection_id)
    slug_map = {item.get("fieldData", {}).get("slug", ""): item["id"] for item in items}
    featured_map = {item["id"]: item.get("fieldData", {}).get("feature-property", False) for item in items}
    return slug_map, featured_map


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

    # Clients initialisieren
    jm = JustimmoClient(JUSTIMMO_USER, JUSTIMMO_PASS)
    wf = WebflowClient(WEBFLOW_TOKEN)

    # ── Schritt 1: Bestehende Webflow-Items laden ──────────────
    log.info("\n[1/5] Lade bestehende Webflow-Collections...")
    type_map     = build_lookup_map(wf, COL_TYPES)
    category_map = build_lookup_map(wf, COL_CATEGORIES)
    location_map = build_lookup_map(wf, COL_LOCATIONS)
    agent_map    = build_lookup_map(wf, COL_AGENTS)
    prop_slugs, featured_map = build_slug_map(wf, COL_PROPERTIES)

    log.info(f"  Typen: {len(type_map)}, Kategorien: {len(category_map)}, "
             f"Standorte: {len(location_map)}, Makler: {len(agent_map)}, "
             f"Immobilien: {len(prop_slugs)}")

    # ── Schritt 2: Immobilien von Justimmo abrufen ─────────────
    log.info("\n[2/5] Rufe Immobilien von Justimmo ab...")
    realties = jm.get_all_realties(max_items=max_items)

    if not realties:
        log.warning("Keine Immobilien von Justimmo erhalten. Prüfe Zugangsdaten!")
        return

    # ── Schritt 3: Referenz-Collections befüllen ───────────────
    log.info("\n[3/5] Synchronisiere Referenz-Collections (Typen, Kategorien, Standorte)...")
    created_ids = {COL_TYPES: [], COL_CATEGORIES: [], COL_LOCATIONS: [], COL_PROPERTIES: []}

    for realty in realties:
        objektart = xml_text(realty, ".//user_defined_simplefield[@feldname='objektart_name']")
        ort       = xml_text(realty, "ort")
        kauf      = realty.find(".//vermarktungsart[@KAUF='1']") is not None
        miete     = realty.find(".//vermarktungsart[@MIETE_PACHT='1']") is not None
        kategorie = "Kauf" if kauf else ("Miete" if miete else "")

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

    log.info(f"  Neue Typen: {len(created_ids[COL_TYPES])}, "
             f"Kategorien: {len(created_ids[COL_CATEGORIES])}, "
             f"Standorte: {len(created_ids[COL_LOCATIONS])}")

    # ── Schritt 4: Immobilien synchronisieren ──────────────────
    log.info("\n[4/5] Synchronisiere Immobilien...")
    stats = {"neu": 0, "aktualisiert": 0, "fehler": 0}

    for i, realty in enumerate(realties, 1):
        objekt_id = xml_text(realty, "id")
        titel     = xml_text(realty, "titel") or f"Objekt {objekt_id}"
        slug      = slugify(f"{titel}-{objekt_id}")

        log.info(f"  [{i}/{len(realties)}] {titel} (ID: {objekt_id})")

        try:
            field_data = map_realty_to_webflow(
                realty, type_map, category_map, location_map, agent_map
            )

            if slug in prop_slugs:
                # Aktualisieren — feature-property ("Empfohlenes Objekt") beibehalten
                # WICHTIG: Slug NICHT beim Update senden (Webflow-Validierungsfehler!)
                item_id = prop_slugs[slug]
                field_data["feature-property"] = featured_map.get(item_id, False)
                # Sicherstellen dass kein Slug im Update-Dict ist
                field_data.pop("slug", None)
                result = wf.update_item(COL_PROPERTIES, item_id, field_data, dry_run)
                if result:
                    stats["aktualisiert"] += 1
                    created_ids[COL_PROPERTIES].append(item_id)
                else:
                    stats["fehler"] += 1
            else:
                # Neu erstellen – Slug nur beim Create setzen
                field_data["slug"] = slug
                result = wf.create_item(COL_PROPERTIES, field_data, dry_run)
                if result and result.get("id"):
                    stats["neu"] += 1
                    prop_slugs[slug] = result["id"]
                    created_ids[COL_PROPERTIES].append(result["id"])
                else:
                    stats["fehler"] += 1

        except Exception as e:
            log.error(f"  Fehler bei Objekt {objekt_id}: {e}")
            stats["fehler"] += 1

    # ── Schritt 5: Veröffentlichen ─────────────────────────────
    log.info("\n[5/5] Veröffentliche geänderte Items...")
    for col_id, ids in created_ids.items():
        if ids:
            # Max. 100 IDs pro Publish-Request
            for chunk in [ids[j:j+100] for j in range(0, len(ids), 100)]:
                wf.publish_collection(col_id, chunk, dry_run)
            log.info(f"  Collection {col_id}: {len(ids)} Items veröffentlicht")

    # ── Zusammenfassung ────────────────────────────────────────
    log.info("\n" + "=" * 60)
    log.info("Synchronisation abgeschlossen!")
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
