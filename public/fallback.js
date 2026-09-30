// Show an error if app.js has not hidden #loading-screen within 10s.
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
