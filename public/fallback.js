// Fallback: show error if app module fails to load within 10s.
// Design note: unconditional 10s timer with a positive guard. On a
// successful load app.js adds `is-hidden` to #loading-screen, so the
// timer's classList check no-ops. On a failed/slow load the loading
// screen stays visible and the timer surfaces a user-visible error.
// A clearTimeout(window.__loadFallback) handle from app.js was
// considered and rejected: it adds coupling without changing the
// user-visible behavior on either success or failure.
setTimeout(() => {
  const el = document.getElementById('loading-screen');
  if (!el || el.classList.contains('is-hidden')) {
    return;
  }
  const p = document.createElement('p');
  p.style.textAlign = 'center';
  p.style.padding = '2rem';
  p.style.color = '#f14668';
  p.textContent = 'Failed to load application. Check network connectivity and refresh.';
  el.replaceChildren(p);
}, 10000);
