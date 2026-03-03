# Justimmo → Webflow CMS Sync

Automatischer täglicher Sync von Justimmo-Immobilien zu Webflow CMS.

## Was wird synchronisiert?

- **Neue Immobilien** werden automatisch in Webflow angelegt
- **Bestehende Immobilien** werden aktualisiert (Preis, Beschreibung, Koordinaten, etc.)
- **Nicht mehr aktive Immobilien** werden in Webflow archiviert
- **Site wird automatisch publiziert** nach dem Sync

## Zeitplan

Täglich um **0:00 Uhr** (UTC 23:00 = MEZ 0:00 Uhr)

## Manuell ausführen

Im GitHub Repository unter **Actions** → **Justimmo → Webflow Nacht-Sync** → **Run workflow**

## Konfiguration (GitHub Secrets)

| Secret | Beschreibung |
|--------|-------------|
| `JUSTIMMO_USER` | Justimmo API Benutzername |
| `JUSTIMMO_PASS` | Justimmo API Passwort |
| `WEBFLOW_TOKEN` | Webflow API Token |

## Felder die synchronisiert werden

- Titel, Adresse (PLZ+Ort), Beschreibung
- Preis (Kauf/Miete), Fläche, Zimmer, Badezimmer
- Bundesland, Immobilientyp, Vermarktungsart
- GPS-Koordinaten (für Kartenanzeige)
- Ausstattung (Klimaanlage, Aufzug, Keller, Terrasse, etc.)
- Etage, Baujahr, Zustand, Heizung, Energieausweis
