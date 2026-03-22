<script>
(function () {
  var DATA_URL = 'https://cdn.jsdelivr.net/gh/ImmoHub/justimmo-webflow-sync@main/filter-data.js';
  var STORAGE_KEY = 'ir_filter_v15';
  var path = window.location.pathname;

  // ── Slug → Bundesland-Name Mapping (URL-Slug zu filter-data.js Wert) ──
  var slugToBundesland = {
    'wien':               'Wien',
    'niederoesterreich':  'Niederösterreich',
    'burgenland':         'Burgenland',
    'oberoesterreich':    'Oberösterreich',
    'steiermark':         'Steiermark',
    'kaernten':           'Kärnten',
    'salzburg':           'Salzburg',
    'tirol':              'Tirol',
    'vorarlberg':         'Vorarlberg'
  };

  // ── Slug → Vermarktungsart Mapping ────────────────────────────
  var slugToVermarktung = {
    'kaufen':  'kaufen',
    'mieten':  'mieten',
    'anlage':  'anlage'
  };

  // ── Slug → Objektart Text Mapping (nur 4 erlaubte Typen) ───────
  var slugToObjektart = {
    'wohnung':               'wohnung',
    'haus':                  'haus',
    'grundstueck':           'grundstück',
    'zinshaus-renditeobjekt': 'zinshaus / renditeobjekt'
  };

  function getActive() {
    try { return JSON.parse(localStorage.getItem(STORAGE_KEY) || '{}'); }
    catch(e) { return {}; }
  }
  function setActive(obj) {
    var clean = {};
    Object.keys(obj).forEach(function(k) { if (obj[k]) clean[k] = obj[k]; });
    localStorage.setItem(STORAGE_KEY, JSON.stringify(clean));
  }

  // ── Filter-Unterseite erkannt ──────────────────────────────────
  var locMatch  = path.match(/\/property-locations\/([^\/]+)/);
  var catMatch  = path.match(/\/property-categories\/([^\/]+)/);
  var typeMatch = path.match(/\/propert-types\/([^\/]+)/) || path.match(/\/property-types\/([^\/]+)/);

  if (locMatch || catMatch || typeMatch) {
    var active = getActive();

    if (locMatch) {
      var slug = locMatch[1];
      // Slug in echten Bundesland-Namen umwandeln
      var bundeslandName = slugToBundesland[slug] || slug;
      // Toggle wenn gleich, sonst ersetzen
      active.bundesland = (active.bundesland === bundeslandName) ? null : bundeslandName;
    }
    if (catMatch) {
      var catSlug = catMatch[1];
      var vermarktungName = slugToVermarktung[catSlug] || catSlug;
      active.vermarktung = (active.vermarktung === vermarktungName) ? null : vermarktungName;
    }
    if (typeMatch) {
      var typeSlug = typeMatch[1];
      active.objektart = (active.objektart === typeSlug) ? null : typeSlug;
    }

    setActive(active);
    window.location.replace('/immobilien_suchen');
    return;
  }

  // ── Listenseite ────────────────────────────────────────────────
  if (path.indexOf('/immobilien_suchen') === -1 && path.indexOf('/immobilien-suchen') === -1) return;

  // Filter löschen wenn man von außerhalb kommt
  var ref = document.referrer || '';
  var fromFilter = ref.indexOf('/property-locations/') !== -1 ||
                   ref.indexOf('/property-categories/') !== -1 ||
                   ref.indexOf('/propert-types/') !== -1 ||
                   ref.indexOf('/property-types/') !== -1;

  if (!fromFilter) {
    localStorage.removeItem(STORAGE_KEY);
  }

  // ── Filter anwenden ────────────────────────────────────────────
  function applyFilters() {
    var active = getActive();
    var data = window.IR_FILTER_DATA || {};

    // Schutz: Filter aktiv aber Daten noch nicht geladen
    if ((active.bundesland || active.vermarktung) && Object.keys(data).length === 0) return;

    document.querySelectorAll('.property-item.w-dyn-item').forEach(function(card) {
      var a = card.querySelector('a[href*="/immobilien/"]');
      var slug = a ? a.getAttribute('href').split('/immobilien/')[1] : '';
      var d = data[slug] || {};
      var objEl = card.querySelector('.property-category');
      var objText = objEl ? objEl.textContent.trim().toLowerCase() : '';

      var show = true;
      // Bundesland: direkter String-Vergleich (beide sind jetzt echte Namen)
      if (active.bundesland && d.l !== active.bundesland) show = false;
      // Vermarktung: lowercase Vergleich
      if (active.vermarktung && (d.k || '').toLowerCase() !== active.vermarktung) show = false;
      // Objektart: Slug zu Text
      if (active.objektart) {
        var expected = slugToObjektart[active.objektart] || active.objektart.replace(/-/g, ' ');
        if (objText !== expected) show = false;
      }
      card.style.display = show ? '' : 'none';
    });

    updateLabels(active);
    var total = document.querySelectorAll('.property-item.w-dyn-item').length;
    var hidden = document.querySelectorAll('.property-item.w-dyn-item[style*="none"]').length;
    console.log('IR Filter v15 | Aktiv:', JSON.stringify(active), '| Sichtbar:', total - hidden, '/ ', total);
  }

  // ── Labels aktualisieren ───────────────────────────────────────
  function updateLabels(active) {
    document.querySelectorAll('.w-dropdown').forEach(function(dd) {
      var textEl = dd.querySelector('.w-dropdown-toggle .basic-text');
      if (!textEl) return;
      if (!textEl.getAttribute('data-ir-def')) textEl.setAttribute('data-ir-def', textEl.textContent.trim());
      var def = textEl.getAttribute('data-ir-def');
      var links = dd.querySelectorAll('a.filter-text');
      if (!links.length) return;
      var firstHref = links[0].getAttribute('href') || '';
      var label = def;

      if (firstHref.indexOf('/property-locations/') !== -1 && active.bundesland) {
        links.forEach(function(l) {
          var lSlug = (l.getAttribute('href') || '').split('/property-locations/')[1] || '';
          if (slugToBundesland[lSlug] === active.bundesland) label = l.textContent.trim();
        });
      } else if (firstHref.indexOf('/property-categories/') !== -1 && active.vermarktung) {
        links.forEach(function(l) {
          var lSlug = (l.getAttribute('href') || '').split('/property-categories/')[1] || '';
          if ((slugToVermarktung[lSlug] || lSlug) === active.vermarktung) label = l.textContent.trim();
        });
      } else if ((firstHref.indexOf('/propert-types/') !== -1 || firstHref.indexOf('/property-types/') !== -1) && active.objektart) {
        links.forEach(function(l) {
          var href = l.getAttribute('href') || '';
          var lSlug = href.split('/propert-types/')[1] || href.split('/property-types/')[1] || '';
          if (lSlug === active.objektart) label = l.textContent.trim();
        });
      }
      textEl.textContent = label;
    });
  }

  // ── Daten laden und Filter anwenden ───────────────────────────
  function loadDataAndFilter() {
    if (window.IR_FILTER_DATA) { applyFilters(); return; }
    var s = document.createElement('script');
    s.src = DATA_URL + '?h=' + Math.floor(Date.now() / 3600000);
    s.onload = applyFilters;
    s.onerror = applyFilters;
    document.head.appendChild(s);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', loadDataAndFilter);
  } else {
    loadDataAndFilter();
  }

})();
</script>
