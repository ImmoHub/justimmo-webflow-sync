#!/usr/bin/env python3
"""
Justimmo → Webflow CMS Vollständiger Sync
==========================================
- Neue Immobilien von Justimmo in Webflow anlegen
- Bestehende Immobilien aktualisieren
- Nicht mehr aktive Immobilien in Webflow archivieren
- Koordinaten von Justimmo Detail-API holen
- Site nach dem Sync publizieren

Credentials werden über Umgebungsvariablen geladen (GitHub Secrets).
"""

import requests
import xml.etree.ElementTree as ET
from requests.auth import HTTPBasicAuth
import json
import time
import re
import os
import unicodedata
import logging
from datetime import datetime

# === KONFIGURATION (aus Umgebungsvariablen / GitHub Secrets) ===
JUSTIMMO_USER = os.environ.get("JUSTIMMO_USER", "api-97120")
JUSTIMMO_PASS = os.environ.get("JUSTIMMO_PASS", "cScKIP9TW2")
WEBFLOW_TOKEN = os.environ.get("WEBFLOW_TOKEN", "73a9946918ef8550ec737f53f55f350cc919a51c6c9031bf44e37c6956ea1026")
SITE_ID = "699f29df3ecf1945550ca280"
COLLECTION_ID = "699f29e03ecf1945550ca36c"

# Reference-Collection IDs
LOCATIONS_COLLECTION = "699f29e03ecf1945550ca3c6"
CATEGORIES_COLLECTION = "699f29e03ecf1945550ca3da"
TYPES_COLLECTION = "699f29e03ecf1945550ca3e1"

# Webflow Reference IDs
BUNDESLAND_IDS = {
    "Wien": "69a751b3ea56691dccd98f51",
    "Niederösterreich": "69a751b5aa6d0c9cd0d5fdc7",
    "Burgenland": "69a751b6a03eccfbf22f310f",
}
CATEGORY_IDS = {
    "Kaufen": "69a6ee19bfda0586db78646e",
    "Mieten": "69a6ee1b7beef16b60ce3d67",
    "Anlage": "69a6ee1d86f3814c85c3540c",
}
TYPE_IDS = {
    "Wohnung": "69a6ee204444a16a1df33299",
    "Haus": "69a6ee224e886d72629ca282",
    "Grundstück": "69a6ee24f9234d30c8dbb6f2",
    "Gewerbe": "69a6ee264e886d72629ca566",
    "Garage": "69a6ee2886f3814c85c35c8e",
    "Zinshaus": "69a6ee2a66a4105527ae01a0",
}
DEFAULT_AGENT_ID = "69a6ee9c6ee9509b8be4d895"  # Mag.(FH) Harald Grassler

# PLZ → Bundesland Mapping
PLZ_BUNDESLAND = {}
for plz in range(1010, 1240):
    PLZ_BUNDESLAND[str(plz)] = "Wien"
for plz in range(2000, 3999):
    PLZ_BUNDESLAND[str(plz)] = "Niederösterreich"
for plz in range(7000, 7999):
    PLZ_BUNDESLAND[str(plz)] = "Burgenland"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/tmp/justimmo_sync.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

WEBFLOW_HEADERS = {
    "Authorization": f"Bearer {WEBFLOW_TOKEN}",
    "accept-version": "1.0.0",
    "Content-Type": "application/json"
}


# ============================================================
# JUSTIMMO API
# ============================================================

def get_justimmo_all_ids():
    ids = []
    offset = 0
    limit = 100
    while True:
        url = f"https://api.justimmo.at/rest/v1/objekt/list?limit={limit}&offset={offset}"
        resp = requests.get(url, auth=HTTPBasicAuth(JUSTIMMO_USER, JUSTIMMO_PASS), timeout=30)
        if resp.status_code != 200:
            log.error(f"Justimmo List API Fehler: {resp.status_code}")
            break
        root = ET.fromstring(resp.content)
        batch = [el.text.strip() for el in root.findall('.//immobilie/id') if el.text]
        ids.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
        time.sleep(0.5)
    return ids


def get_justimmo_detail(justimmo_id):
    url = f"https://api.justimmo.at/rest/v1/objekt/detail?objekt_id={justimmo_id}"
    try:
        resp = requests.get(url, auth=HTTPBasicAuth(JUSTIMMO_USER, JUSTIMMO_PASS), timeout=30)
        if resp.status_code != 200:
            return None
        return ET.fromstring(resp.content)
    except Exception as e:
        log.error(f"Justimmo Detail Fehler für ID {justimmo_id}: {e}")
        return None


def get_field(root, feldname):
    for el in root.iter('user_defined_simplefield'):
        if el.get('feldname') == feldname and el.text:
            return el.text.strip()
    return ''


def parse_justimmo_item(root, justimmo_id):
    data = {}

    # Titel
    titel_el = root.find('.//freitexte/objekttitel')
    if titel_el is None or not titel_el.text:
        titel_el = root.find('.//titel')
    data['name'] = titel_el.text.strip() if titel_el is not None and titel_el.text else f"Immobilie {justimmo_id}"

    # Beschreibung
    beschr_el = root.find('.//objektbeschreibung')
    if beschr_el is not None and beschr_el.text:
        beschr = re.sub(r'<[^>]+>', ' ', beschr_el.text)
        beschr = re.sub(r'\s+', ' ', beschr).strip()
        data['property-overview'] = beschr[:5000]
    else:
        data['property-overview'] = ''

    data['justimmo-id'] = str(justimmo_id)
    objnr_el = root.find('.//verwaltung_techn/objektnr_extern')
    data['objektnummer'] = objnr_el.text.strip() if objnr_el is not None and objnr_el.text else ''

    # Adresse (PLZ + Ort)
    plz_el = root.find('.//geo/plz')
    ort_el = root.find('.//geo/ort')
    plz = plz_el.text.strip() if plz_el is not None and plz_el.text else ''
    ort = ort_el.text.strip() if ort_el is not None and ort_el.text else ''
    data['property-location'] = f"{plz} {ort}".strip()

    # Bundesland
    bundesland = PLZ_BUNDESLAND.get(plz[:4] if plz else '', '')
    if not bundesland:
        if ort.lower() == 'wien':
            bundesland = 'Wien'
        elif plz.startswith('2') or plz.startswith('3'):
            bundesland = 'Niederösterreich'
        elif plz.startswith('7'):
            bundesland = 'Burgenland'
    data['_bundesland'] = bundesland

    # Vermarktungsart
    kauf_el = root.find('.//objektkategorie/vermarktungsart')
    if kauf_el is not None:
        if kauf_el.get('KAUF') == '1':
            data['_kategorie'] = 'Kaufen'
        elif kauf_el.get('MIETE_PACHT') == '1':
            data['_kategorie'] = 'Mieten'
        elif kauf_el.get('ANLAGE') == '1':
            data['_kategorie'] = 'Anlage'
        else:
            data['_kategorie'] = 'Kaufen'
    else:
        data['_kategorie'] = 'Kaufen'

    # Objektart
    objektart_name = get_field(root, 'objektart_name')
    if 'Wohnung' in objektart_name:
        data['_typ'] = 'Wohnung'
    elif 'Haus' in objektart_name or 'Villa' in objektart_name:
        data['_typ'] = 'Haus'
    elif 'Grundstück' in objektart_name or 'Grund' in objektart_name:
        data['_typ'] = 'Grundstück'
    elif 'Gewerbe' in objektart_name or 'Büro' in objektart_name:
        data['_typ'] = 'Gewerbe'
    elif 'Garage' in objektart_name or 'Stellplatz' in objektart_name:
        data['_typ'] = 'Garage'
    elif 'Zinshaus' in objektart_name:
        data['_typ'] = 'Zinshaus'
    else:
        data['_typ'] = 'Wohnung'

    # Preise
    preise = root.find('.//preise')
    if preise is not None:
        def get_price(tag):
            el = preise.find(tag)
            return el.text.strip() if el is not None and el.text else ''

        kaufpreis = get_price('kaufpreis')
        gesamtmiete = get_price('gesamtmiete')
        nettomiete = get_price('nettokaltmiete')
        warmmiete = get_price('warmmiete')

        if kaufpreis:
            try:
                p = float(kaufpreis)
                data['property-price'] = f"€ {p:,.0f}".replace(',', '.')
            except:
                data['property-price'] = f"€ {kaufpreis}"
        elif gesamtmiete:
            try:
                p = float(gesamtmiete)
                data['property-price'] = f"€ {p:,.2f} / Monat".replace(',', '.')
            except:
                data['property-price'] = f"€ {gesamtmiete} / Monat"
        else:
            data['property-price'] = ''

        data['kaufpreis'] = kaufpreis
        data['kaufpreis-netto'] = get_price('kaufpreisnetto')
        data['warmmiete'] = warmmiete
        data['nettokaltmiete'] = nettomiete
        data['gesamtmiete'] = gesamtmiete
        data['nebenkosten'] = get_price('nebenkosten')
        data['heizkosten'] = get_price('heizkosten')
        prov_el = preise.find('aussen_courtage')
        data['provision'] = prov_el.text.strip() if prov_el is not None and prov_el.text else ''
    else:
        data['property-price'] = ''
        for k in ['kaufpreis', 'kaufpreis-netto', 'warmmiete', 'nettokaltmiete', 'gesamtmiete', 'nebenkosten', 'heizkosten', 'provision']:
            data[k] = ''

    # Flächen
    flaechen = root.find('.//flaechen')
    if flaechen is not None:
        def get_fl(tag):
            el = flaechen.find(tag)
            return el.text.strip() if el is not None and el.text else ''
        wfl = get_fl('wohnflaeche')
        nutzfl = get_fl('nutzflaeche')
        grundfl = get_fl('grundstuecksflaeche')
        data['wohnflache-m2'] = wfl
        data['nutzflache-m2'] = nutzfl
        data['grundstucksflache-m2'] = grundfl
        hauptfl = wfl or nutzfl or grundfl
        if hauptfl:
            try:
                data['property-area'] = f"{float(hauptfl):,.2f} m²".replace(',', '.')
            except:
                data['property-area'] = f"{hauptfl} m²"
        else:
            data['property-area'] = ''
    else:
        data['wohnflache-m2'] = data['nutzflache-m2'] = data['grundstucksflache-m2'] = data['property-area'] = ''

    # Zimmer & Bad
    zimmer_el = root.find('.//anzahl_zimmer')
    data['property-beds'] = zimmer_el.text.strip() if zimmer_el is not None and zimmer_el.text else ''
    bad_el = root.find('.//anzahl_badezimmer')
    data['property-bathrooms'] = bad_el.text.strip() if bad_el is not None and bad_el.text else ''

    # Stellplätze
    for tag in ['stellplatz_anzahl', 'anzahl_stellplaetze']:
        el = root.find(f'.//{tag}')
        if el is not None and el.text:
            data['property-parking'] = el.text.strip()
            break
    else:
        data['property-parking'] = ''

    # Weitere Felder
    etage_el = root.find('.//etage')
    data['etage'] = etage_el.text.strip() if etage_el is not None and etage_el.text else ''
    data['baujahr'] = get_field(root, 'baujahr')
    data['zustand'] = get_field(root, 'zustand_name') or ''
    data['heizung'] = get_field(root, 'heizungsart_name')
    hwb = get_field(root, 'hwb_wert')
    hwb_kl = get_field(root, 'hwb_klasse')
    data['energieausweis'] = (f"HWB: {hwb} kWh/m²a" + (f" ({hwb_kl})" if hwb_kl else '')) if hwb else ''
    data['verfuegbar-ab'] = get_field(root, 'verfuegbar_ab')

    # Ausstattung
    ausstattung = root.find('.//ausstattung')
    def has_feature(tag):
        if ausstattung is None:
            return False
        el = ausstattung.find(f'.//{tag}')
        return el is not None and el.text and el.text.strip() in ['1', 'true', 'True']

    data['air-conditioner'] = has_feature('klimaanlage')
    data['washing-machine'] = has_feature('waschmaschine')
    data['wardrobe'] = has_feature('einbauschrank')
    data['keller'] = has_feature('keller')
    data['aufzug'] = has_feature('personenaufzug')
    data['barrierefrei'] = has_feature('barrierefrei')

    terrasse_el = root.find('.//anzahl_terrassen')
    balkon_el = root.find('.//anzahl_balkone')
    data['terrasse-balkon'] = (
        (terrasse_el is not None and terrasse_el.text and int(terrasse_el.text.strip() or 0) > 0) or
        (balkon_el is not None and balkon_el.text and int(balkon_el.text.strip() or 0) > 0)
    )

    # Koordinaten
    lat = get_field(root, 'ungenaue_verortung_breitengrad') or get_field(root, 'geokoordinaten_breitengrad')
    lon = get_field(root, 'ungenaue_verortung_laengengrad') or get_field(root, 'geokoordinaten_laengengrad')
    data['latitude-4'] = lat
    data['longitude-3'] = lon

    return data


# ============================================================
# WEBFLOW API
# ============================================================

def get_all_webflow_items():
    items = []
    offset = 0
    limit = 100
    while True:
        url = f"https://api.webflow.com/v2/collections/{COLLECTION_ID}/items?limit={limit}&offset={offset}"
        resp = requests.get(url, headers=WEBFLOW_HEADERS)
        data = resp.json()
        batch = data.get('items', [])
        items.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    return items


def make_slug(name):
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
    name = re.sub(r'[^a-z0-9\s-]', '', name.lower())
    return re.sub(r'[\s-]+', '-', name).strip('-')[:80]


def build_webflow_payload(data):
    bundesland_id = BUNDESLAND_IDS.get(data.get('_bundesland', ''), '')
    kategorie_id = CATEGORY_IDS.get(data.get('_kategorie', 'Kaufen'), CATEGORY_IDS['Kaufen'])
    typ_id = TYPE_IDS.get(data.get('_typ', 'Wohnung'), TYPE_IDS['Wohnung'])

    payload = {
        "justimmo-id": data.get('justimmo-id', ''),
        "name": data.get('name', ''),
        "property-location": data.get('property-location', ''),
        "property-overview": data.get('property-overview', ''),
        "property-price": data.get('property-price', ''),
        "property-area": data.get('property-area', ''),
        "property-beds": data.get('property-beds', ''),
        "property-bathrooms": data.get('property-bathrooms', ''),
        "property-parking": data.get('property-parking', ''),
        "objektnummer": data.get('objektnummer', ''),
        "etage": data.get('etage', ''),
        "baujahr": data.get('baujahr', ''),
        "zustand": data.get('zustand', ''),
        "heizung": data.get('heizung', ''),
        "energieausweis": data.get('energieausweis', ''),
        "betriebskosten": data.get('nebenkosten', ''),
        "provision": data.get('provision', ''),
        "verfuegbar-ab": data.get('verfuegbar-ab', ''),
        "kaufpreis": data.get('kaufpreis', ''),
        "kaufpreis-netto": data.get('kaufpreis-netto', ''),
        "warmmiete": data.get('warmmiete', ''),
        "nettokaltmiete": data.get('nettokaltmiete', ''),
        "gesamtmiete": data.get('gesamtmiete', ''),
        "nebenkosten": data.get('nebenkosten', ''),
        "heizkosten": data.get('heizkosten', ''),
        "wohnflache-m2": data.get('wohnflache-m2', ''),
        "nutzflache-m2": data.get('nutzflache-m2', ''),
        "grundstucksflache-m2": data.get('grundstucksflache-m2', ''),
        "latitude-4": data.get('latitude-4', ''),
        "longitude-3": data.get('longitude-3', ''),
        "air-conditioner": data.get('air-conditioner', False),
        "washing-machine": data.get('washing-machine', False),
        "wardrobe": data.get('wardrobe', False),
        "keller": data.get('keller', False),
        "aufzug": data.get('aufzug', False),
        "barrierefrei": data.get('barrierefrei', False),
        "terrasse-balkon": data.get('terrasse-balkon', False),
        "agent-detail": DEFAULT_AGENT_ID,
        "property-locations": bundesland_id,
        "property-categories": kategorie_id,
        "property-type": typ_id,
    }
    return payload


def create_webflow_item(data):
    url = f"https://api.webflow.com/v2/collections/{COLLECTION_ID}/items"
    payload_data = build_webflow_payload(data)
    payload_data['slug'] = make_slug(data.get('name', ''))
    resp = requests.post(url, headers=WEBFLOW_HEADERS, json={"fieldData": payload_data, "isDraft": False})
    return resp.status_code, resp.json()


def update_webflow_item(item_id, data):
    url = f"https://api.webflow.com/v2/collections/{COLLECTION_ID}/items/{item_id}"
    resp = requests.patch(url, headers=WEBFLOW_HEADERS, json={"fieldData": build_webflow_payload(data)})
    return resp.status_code, resp.json()


def archive_webflow_item(item_id):
    url = f"https://api.webflow.com/v2/collections/{COLLECTION_ID}/items/{item_id}"
    resp = requests.patch(url, headers=WEBFLOW_HEADERS, json={"isArchived": True})
    return resp.status_code


def publish_all_items(item_ids):
    url = f"https://api.webflow.com/v2/collections/{COLLECTION_ID}/items/publish"
    for i in range(0, len(item_ids), 100):
        batch = item_ids[i:i+100]
        resp = requests.post(url, headers=WEBFLOW_HEADERS, json={"itemIds": batch})
        log.info(f"  Publish Batch {i//100+1}: Status {resp.status_code}")
        time.sleep(1)


def publish_site():
    url = f"https://api.webflow.com/v2/sites/{SITE_ID}/publish"
    resp = requests.post(url, headers=WEBFLOW_HEADERS, json={"publishToWebflowSubdomain": True})
    log.info(f"Site-Publish: {resp.status_code}")


# ============================================================
# HAUPTPROGRAMM
# ============================================================

def main():
    log.info("=" * 60)
    log.info(f"Justimmo → Webflow Sync gestartet: {datetime.now()}")
    log.info("=" * 60)

    log.info("\n[1/5] Lade alle aktiven Justimmo-IDs...")
    justimmo_ids = get_justimmo_all_ids()
    log.info(f"  {len(justimmo_ids)} aktive Immobilien bei Justimmo")

    log.info("\n[2/5] Lade alle Webflow CMS Items...")
    webflow_items = get_all_webflow_items()
    log.info(f"  {len(webflow_items)} Items in Webflow")

    webflow_by_justimmo_id = {
        str(item.get('fieldData', {}).get('justimmo-id', '')): item
        for item in webflow_items
        if item.get('fieldData', {}).get('justimmo-id', '')
    }

    justimmo_id_set = set(str(i) for i in justimmo_ids)
    webflow_id_set = set(webflow_by_justimmo_id.keys())

    new_ids = justimmo_id_set - webflow_id_set
    update_ids = justimmo_id_set & webflow_id_set
    remove_ids = webflow_id_set - justimmo_id_set

    log.info(f"\n  Neu anlegen:   {len(new_ids)}")
    log.info(f"  Aktualisieren: {len(update_ids)}")
    log.info(f"  Archivieren:   {len(remove_ids)}")

    stats = {'created': 0, 'updated': 0, 'archived': 0, 'errors': 0}
    published_ids = []

    log.info(f"\n[3/5] Lege {len(new_ids)} neue Immobilien an...")
    for i, jid in enumerate(sorted(new_ids)):
        log.info(f"  [{i+1}/{len(new_ids)}] Neu: {jid}")
        root = get_justimmo_detail(jid)
        if root is None:
            stats['errors'] += 1
            continue
        data = parse_justimmo_item(root, jid)
        log.info(f"    {data['name'][:60]}")
        status, result = create_webflow_item(data)
        if status in [200, 201]:
            published_ids.append(result.get('id', ''))
            stats['created'] += 1
            log.info(f"    ✓ Erstellt")
        else:
            log.error(f"    ✗ Fehler {status}: {result.get('message', '')}")
            stats['errors'] += 1
        time.sleep(0.5)

    log.info(f"\n[4/5] Aktualisiere {len(update_ids)} bestehende Immobilien...")
    for i, jid in enumerate(sorted(update_ids)):
        item_id = webflow_by_justimmo_id[jid]['id']
        log.info(f"  [{i+1}/{len(update_ids)}] Update: {jid}")
        root = get_justimmo_detail(jid)
        if root is None:
            stats['errors'] += 1
            continue
        data = parse_justimmo_item(root, jid)
        status, result = update_webflow_item(item_id, data)
        if status == 200:
            published_ids.append(item_id)
            stats['updated'] += 1
            log.info(f"    ✓ Aktualisiert")
        else:
            log.error(f"    ✗ Fehler {status}: {result.get('message', '')}")
            stats['errors'] += 1
        time.sleep(0.3)

    log.info(f"\n[5/5] Archiviere {len(remove_ids)} nicht mehr aktive Immobilien...")
    for jid in sorted(remove_ids):
        item_id = webflow_by_justimmo_id[jid]['id']
        name = webflow_by_justimmo_id[jid].get('fieldData', {}).get('name', '')[:50]
        log.info(f"  Archiviere: {name}")
        status = archive_webflow_item(item_id)
        if status == 200:
            stats['archived'] += 1
            log.info(f"    ✓ Archiviert")
        else:
            log.error(f"    ✗ Fehler {status}")
            stats['errors'] += 1
        time.sleep(0.3)

    log.info(f"\nPubliziere {len(published_ids)} Items...")
    if published_ids:
        publish_all_items(published_ids)

    log.info("\nPubliziere Site...")
    publish_site()

    log.info("\n" + "=" * 60)
    log.info("SYNC ABGESCHLOSSEN")
    log.info(f"  Neu angelegt:  {stats['created']}")
    log.info(f"  Aktualisiert:  {stats['updated']}")
    log.info(f"  Archiviert:    {stats['archived']}")
    log.info(f"  Fehler:        {stats['errors']}")
    log.info(f"  Abgeschlossen: {datetime.now()}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
