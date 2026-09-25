// Garde l'appli coureur sur le telephone : si Termux ne tourne pas (raccourci ouvert avant de lancer
// Termux, Termux arrete par Android...), la page s'ouvre quand meme depuis cette copie et propose
// d'ouvrir Termux. Reseau d'abord : quand Termux tourne, c'est toujours sa version qui est servie.
var CACHE = 'coureur-v1';
var PRECACHE = ['coureur.html', 'vendor/leaflet.css', 'vendor/leaflet.js',
                'vendor/firebase-app-compat.js', 'vendor/firebase-firestore-compat.js'];

self.addEventListener('install', function(e){
  e.waitUntil(caches.open(CACHE).then(function(c){ return c.addAll(PRECACHE); }).then(function(){ return self.skipWaiting(); }));
});
self.addEventListener('activate', function(e){ e.waitUntil(self.clients.claim()); });

self.addEventListener('fetch', function(e){
  var url = new URL(e.request.url);
  // donnees en direct de Termux, et tout ce qui n'est pas servi par Termux : jamais de copie
  if(e.request.method !== 'GET' || url.origin !== self.location.origin || url.pathname.indexOf('/api/') === 0) return;
  e.respondWith(fetch(e.request).then(function(r){
    if(r.ok){ var copy = r.clone(); caches.open(CACHE).then(function(c){ c.put(e.request, copy); }); }
    return r;
  }).catch(function(){
    return caches.match(e.request, {ignoreSearch:true}).then(function(r){
      return r || (url.pathname === '/' ? caches.match('coureur.html') : Response.error());
    });
  }));
});
