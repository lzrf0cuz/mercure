import { fetchEventSource } from 'https://cdn.jsdelivr.net/npm/@microsoft/fetch-event-source/+esm';

class RetriableError extends Error {}
class FatalError extends Error {}

const LOG_LEVEL = 'DEBUG';
const Logger = (() => {
  const levels = { DEBUG: 1, INFO: 2, WARN: 3, ERROR: 4 };
  const current = levels[LOG_LEVEL] || levels.INFO;
  const methods = {
    DEBUG: console.debug,
    INFO: console.info,
    WARN: console.warn,
    ERROR: console.error,
  };

  function log(level, message, ...args) {
    if (levels[level] < current) return;
    (methods[level] || console.log)(message, ...args);
  }

  return {
    debug: (message, ...args) => log('DEBUG', message, ...args),
    info: (message, ...args) => log('INFO', message, ...args),
    warn: (message, ...args) => log('WARN', message, ...args),
    error: (message, ...args) => log('ERROR', message, ...args),
  };
})();
(function initTheme() {
  const THEME_KEY = 'mercure-theme';
  const themes = ['auto', 'light', 'dark'];
  const icons = { auto: '◐', light: '☀', dark: '☾' };
  const titles = {
    auto: 'Theme: Auto (system)',
    light: 'Theme: Light',
    dark: 'Theme: Dark',
  };

  function getStoredTheme() {
    try {
      return localStorage.getItem(THEME_KEY) || 'auto';
    } catch (e) {
      Logger.debug('[Theme] localStorage unavailable, using default.', e);
      return 'auto';
    }
  }

  function setStoredTheme(theme) {
    try {
      localStorage.setItem(THEME_KEY, theme);
    } catch (e) {
      Logger.debug("[Theme] localStorage unavailable, theme won't persist.", e);
    }
  }

  function applyTheme(theme) {
    const html = document.documentElement;
    if (theme === 'auto') {
      html.removeAttribute('data-theme');
    } else {
      html.setAttribute('data-theme', theme);
    }
    updateHljsTheme(theme);
  }

  function updateHljsTheme(theme) {
    const hljsLink = document.getElementById('hljs-theme');
    if (!hljsLink) return;

    const baseUrl = 'https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.11.1/styles/';
    let isDark = theme === 'dark';
    if (theme === 'auto') {
      isDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
    }
    hljsLink.href = isDark ? `${baseUrl}github-dark.min.css` : `${baseUrl}github.min.css`;
  }

  function updateToggleButton(theme) {
    const icon = document.getElementById('theme-icon');
    const button = document.getElementById('theme-toggle');
    if (icon) icon.textContent = icons[theme];
    if (button) button.dataset.tooltip = titles[theme];
  }

  function cycleTheme() {
    const current = getStoredTheme();
    const nextIndex = (themes.indexOf(current) + 1) % themes.length;
    const next = themes[nextIndex];
    setStoredTheme(next);
    updateToggleButton(next);
    if (document.startViewTransition) {
      document.startViewTransition(() => applyTheme(next));
    } else {
      applyTheme(next);
    }
  }

  // Apply stored theme immediately (before DOMContentLoaded) to prevent flash
  applyTheme(getStoredTheme());

  // Set up the toggle button once DOM is ready
  document.addEventListener('DOMContentLoaded', () => {
    const theme = getStoredTheme();
    updateToggleButton(theme);
    const button = document.getElementById('theme-toggle');
    if (button) button.addEventListener('click', cycleTheme);
  });
})();

(() => {
  const defaultHubUrl = `${window.location.origin}/.well-known/mercure`;
  const CONFIG = {
    UI_JWKS_PATH: './fixtures/jwks.json',
    UI_PUBLIC_PEM_PATH: './fixtures/public-key.pem',
    UI_PUBLIC_JWK_PATH: './fixtures/public-jwk.json',
    UI_PRIVATE_JWK_PATH: './fixtures/private-jwk.json',
    UI_DEMO_TOKEN_PATH: './fixtures/tokens.json',
    UI_DEMO_TOKEN_KEY: 'rs256',
    UI_HS256_TOKEN_KEY: 'hs256',
    UI_HS256_SECRET: '!ChangeThisMercureHubJWTSecretKey!',
    UI_DEFAULT_HUB_URL: defaultHubUrl,
    // Demo pub/sub topic — must NOT sit under the hub path: validateTopics 403s
    // any topic containing "/.well-known/mercure" (reserved-namespace forgery
    // guard). Matches the demo token's subscribe claim
    // `{scheme}://{+host}/demo/books/{id}.jsonld`.
    UI_PLACEHOLDER_TOPIC: `${window.location.origin}/demo/books/1.jsonld`,
    // Discovery / cookie-auth URL — the demo discovery RESOURCE, served by the
    // hub's Demo reflector handler at /.well-known/mercure/ui/demo/ (under the hub
    // path by necessity: the Caddy module forwards only the /.well-known/mercure
    // path prefix — strings.HasPrefix in caddy/mercure.go — to the hub).
    // Distinct from the pub/sub topic above: discover/cookie are GET fetches of
    // this resource, so the reserved-topic guard (publish/POST only) doesn't apply.
    UI_DISCOVER_TOPIC: `${defaultHubUrl}/ui/demo/books/1.jsonld`,
    // Controls whether the subscription monitoring SSE stays open when the tab is hidden
    FES_OPEN_WHEN_HIDDEN_PRESENCE: true, // Stay alive to track all state changes
    FES_RETRY_BASE_DELAY_MS: 1000,
    FES_RETRY_MAX_DELAY_MS: 30000,
    FES_RETRY_SUCCESS_RESET_MS: 60000,
  };

  let cachedDemoTokens = null;

  async function fetchDemoTokens() {
    if (cachedDemoTokens) return cachedDemoTokens;

    try {
      const resp = await fetch(CONFIG.UI_DEMO_TOKEN_PATH);
      if (!resp.ok) throw new Error(`${resp.status} ${resp.statusText}`);
      cachedDemoTokens = await resp.json();
      Logger.debug('[Init] Cached demo tokens (RS256 and HS256).');
      return cachedDemoTokens;
    } catch (err) {
      Logger.warn('[Init] Failed to fetch demo tokens.', err);
      showNotification(
        'Could not load demo tokens. Enter a JWT manually or check network connectivity.',
        'warning',
        6000,
      );
      return null;
    }
  }

  function decodeBase64Url(str) {
    const base64 = str.replace(/-/g, '+').replace(/_/g, '/');
    const pad = base64.length % 4;
    const padded = pad ? base64 + '='.repeat(4 - pad) : base64;
    let binary;
    try {
      binary = atob(padded);
    } catch (e) {
      throw new Error('Invalid base64 encoding in JWT.', { cause: e });
    }
    // TextDecoder handles multi-byte UTF-8 that atob() alone mangles
    const bytes = Uint8Array.from(binary, (c) => c.charCodeAt(0));
    return new TextDecoder().decode(bytes);
  }

  function parseTopicList(text) {
    return text
      .split('\n')
      .map((t) => t.trim())
      .filter((t) => t !== '');
  }

  function truncateWithEllipsis(text, max) {
    return text.length > max ? `${text.slice(0, max)}…` : text;
  }

  // FatalError messages reach showNotification (textContent) and console
  // logs, so HTML injection isn't a risk — but a hub that ships a huge or
  // non-string payload would otherwise blow up the toast and log lines.
  // Objects get JSON.stringify'd so structured payloads survive instead of
  // collapsing to "[object Object]"; circular refs fall back to String().
  function sanitizeFatalEventData(raw) {
    if (raw == null) return '';
    let text;
    if (typeof raw === 'string') {
      text = raw;
    } else if (typeof raw === 'object') {
      try {
        text = JSON.stringify(raw);
      } catch {
        text = String(raw);
      }
    } else {
      text = String(raw);
    }
    return truncateWithEllipsis(text, 200);
  }

  // Subscriptions arrive both via the initial snapshot and the SSE stream;
  // a malformed entry from either source would throw downstream when
  // renderSubscriptionCard touches `s.topic.length` etc. Validate at both
  // entry points.
  function isValidSubscription(s) {
    return (
      s != null &&
      typeof s === 'object' &&
      typeof s.id === 'string' &&
      typeof s.topic === 'string' &&
      typeof s.subscriber === 'string'
    );
  }

  const DOM = {
    updatesEmpty: document.getElementById('updates-empty'),
    updatesList: document.getElementById('updates-list'),
    liveIndicator: document.getElementById('live-indicator'),
    eventCount: document.getElementById('event-count'),
    retryValue: document.getElementById('retry-value'),
    retryInput: document.getElementById('eventRetry'),
    retryDecrement: document.getElementById('retryDecrement'),
    retryIncrement: document.getElementById('retryIncrement'),
    subscriptionsEmpty: document.getElementById('subscriptions-empty'),
    subscriptionsGrid: document.getElementById('subscriptions-grid'),
    subscriptionCountTotal: document.getElementById('subscription-count-total'),
    subscriptionCountClient: document.getElementById('subscription-count-client'),
    subscriptionCountMonitor: document.getElementById('subscription-count-monitor'),
    settingsForm: document.forms.settings,
    discoverForm: document.forms.discover,
    subscribeForm: document.forms.subscribe,
    publishForm: document.forms.publish,
    subscriptionsForm: document.forms.subscriptions,
    updateTemplate: document.getElementById('update'),
    subscriptionTemplate: document.getElementById('subscription'),
    subscribeTopicsExamples: document.getElementById('subscribeTopicsExamples'),
    jwtHeader: document.getElementById('jwt-header'),
    jwtPayload: document.getElementById('jwt-payload'),
    copyPublicJwkButton: document.getElementById('copy-public-jwk'),
    copyPrivateJwkButton: document.getElementById('copy-private-jwk'),
    copyJwksButton: document.getElementById('copy-jwks'),
    copyPublicPemButton: document.getElementById('copy-public-pem'),
    copyJwtButton: document.getElementById('copy-jwt'),
    clearCookieButton: document.getElementById('clearCookie'),
    algorithmWarning: document.getElementById('algorithm-warning'),
    algorithmWarningHS256: document.getElementById('algorithm-warning-hs256'),
    algorithmWarningRS256: document.getElementById('algorithm-warning-rs256'),
    debugTokenRS256: document.getElementById('debug-token-rs256'),
    createTokenRS256: document.getElementById('create-token-rs256'),
    hubConfigRS256: document.getElementById('hub-config-rs256'),
    debugTokenHS256: document.getElementById('debug-token-hs256'),
    createTokenHS256: document.getElementById('create-token-hs256'),
    hubConfigHS256: document.getElementById('hub-config-hs256'),
    copyHS256SecretButton: document.getElementById('copy-hs256-secret'),
    copyHS256SecretVerifyButton: document.getElementById('copy-hs256-secret-verify'),
    copyHS256ConfigButton: document.getElementById('copy-hs256-config'),
    hs256ConfigPreview: document.getElementById('hs256-config-preview'),
  };

  let updateCtrl;
  let subscriptionCtrl;
  let notificationTimeoutId = null;
  let hasCookie = false;
  let eventCounter = 0;

  function propagateHubUrl(newHubUrl, { skipDiscover = false } = {}) {
    let newOrigin;
    try {
      newOrigin = new URL(newHubUrl).origin;
    } catch (e) {
      Logger.debug('[Settings] Could not propagate hub URL - invalid URL.', e);
      showNotification('Invalid hub URL.', 'warning', 2000);
      return;
    }

    const placeholderTopic = `${newOrigin}/demo/books/1.jsonld`;

    for (const field of [DOM.subscribeForm.topics, DOM.publishForm.topics]) {
      field.value = field.value
        .split('\n')
        .map((line) => replaceDomainInValue(line, newOrigin))
        .join('\n');
    }

    if (!skipDiscover) {
      DOM.discoverForm.topic.value = replaceDomainInValue(DOM.discoverForm.topic.value, newOrigin);
    }

    const jsonFields = skipDiscover
      ? [DOM.publishForm.data]
      : [DOM.discoverForm.body, DOM.publishForm.data];
    for (const field of jsonFields) {
      try {
        const data = JSON.parse(field.value);
        if (data['@id']) {
          data['@id'] = replaceDomainInValue(data['@id'], newOrigin);
          field.value = JSON.stringify(data, null, 2);
        }
      } catch (e) {
        Logger.debug('[Settings] Field is not valid JSON, skipping @id update.', e);
      }
    }

    DOM.subscribeTopicsExamples.textContent = `${newOrigin}/demo/books/{id}.jsonld\n${placeholderTopic}`;

    Logger.debug(`[Settings] Propagated hub origin: ${newOrigin}`);
  }

  function replaceDomainInValue(value, newOrigin) {
    try {
      new URL(value); // Validate it's a URL (throws for non-URLs like plain text)
      // Find where the path starts in the original string — first '/' after '://'.
      // This preserves the original path/query/hash verbatim, avoiding both
      // percent-encoding of URI template characters ({, }) and port normalization issues.
      const schemeEnd = value.indexOf('://');
      if (schemeEnd === -1) return value; // Non-hierarchical URIs (urn:, data:) — nothing to replace
      const pathStart = value.indexOf('/', schemeEnd + 3);
      const suffix = pathStart !== -1 ? value.slice(pathStart) : '/';
      return newOrigin.replace(/\/+$/, '') + suffix;
    } catch (e) {
      Logger.debug('[Settings] Value is not a URL, returning as-is.', {
        value,
        error: e.message,
      });
      return value;
    }
  }

  function updateFormStates() {
    const mainSubscribed = !DOM.subscribeForm.elements.unsubscribe.disabled;
    const presenceSubscribed = !DOM.subscriptionsForm.elements.unsubscribe.disabled;
    const disabled = mainSubscribed || presenceSubscribed;

    DOM.settingsForm.hubUrl.disabled = disabled;
    DOM.settingsForm.jwt.disabled = disabled;

    for (const radio of DOM.settingsForm.querySelectorAll('input[name="authorization"]')) {
      radio.disabled = disabled;
    }
    DOM.clearCookieButton.disabled = disabled || !hasCookie;

    DOM.discoverForm.topic.disabled = disabled;
    DOM.discoverForm.body.disabled = disabled;

    DOM.discoverForm.querySelector('button[name="discover"]').disabled = disabled;

    for (const radio of DOM.settingsForm.querySelectorAll('input[name="jwtAlgorithm"]')) {
      radio.disabled = disabled;
    }
  }

  function updateAlgorithmSections() {
    const isHS256 = DOM.settingsForm.jwtAlgorithm.value === 'hs256';

    if (DOM.algorithmWarningHS256) {
      DOM.algorithmWarningHS256.style.display = isHS256 ? 'inline' : 'none';
    }
    if (DOM.algorithmWarningRS256) {
      DOM.algorithmWarningRS256.style.display = isHS256 ? 'none' : 'inline';
    }

    const pairs = [
      [DOM.debugTokenRS256, DOM.debugTokenHS256],
      [DOM.createTokenRS256, DOM.createTokenHS256],
      [DOM.hubConfigRS256, DOM.hubConfigHS256],
    ];
    for (const [rs256El, hs256El] of pairs) {
      if (rs256El) rs256El.style.display = isHS256 ? 'none' : 'block';
      if (hs256El) hs256El.style.display = isHS256 ? 'block' : 'none';
    }

    if (isHS256 && DOM.hs256ConfigPreview) {
      DOM.hs256ConfigPreview.textContent = `subscriber_jwt ${CONFIG.UI_HS256_SECRET}\npublisher_jwt ${CONFIG.UI_HS256_SECRET}`;
    }
  }
  function applyTokenForCurrentAlgorithm(tokens, logPrefix) {
    const isHS256 = DOM.settingsForm.jwtAlgorithm?.value === 'hs256';
    const algLabel = isHS256 ? 'HS256' : 'RS256';
    const tokenKey = isHS256 ? CONFIG.UI_HS256_TOKEN_KEY : CONFIG.UI_DEMO_TOKEN_KEY;
    const token = tokens[tokenKey];
    if (!token) {
      Logger.warn(`${logPrefix} Token key "${tokenKey}" not found in cached tokens.`, tokens);
      showNotification(`Demo token for ${algLabel} not found in fixtures.`, 'warning');
    }
    DOM.settingsForm.jwt.value = token || '';
    Logger.debug(`${logPrefix} Applied ${algLabel} JWT token from cache.`);
  }
  function loadTokenForAlgorithm() {
    if (!cachedDemoTokens) {
      Logger.warn('[Auth] No cached tokens available for algorithm switch.');
      showNotification('Tokens not loaded. Try refreshing the page.', 'warning');
      return;
    }
    applyTokenForCurrentAlgorithm(cachedDemoTokens, '[Auth]');
    updateJwtPayloadDisplay();
  }
  function setLiveUpdatesState(state) {
    const indicator = DOM.liveIndicator;
    const empty = DOM.updatesEmpty;
    const list = DOM.updatesList;

    indicator.classList.remove('is-active');

    switch (state) {
      case 'idle':
        empty.style.display = 'flex';
        empty.querySelector('p').textContent = 'Not subscribed';
        empty.querySelector('span').textContent = 'Click Subscribe to receive events';
        list.replaceChildren();
        eventCounter = 0;
        updateEventCount();
        DOM.retryValue.textContent = `${CONFIG.FES_RETRY_BASE_DELAY_MS}ms`;
        break;
      case 'connecting':
        empty.style.display = 'flex';
        empty.querySelector('p').textContent = 'Connecting...';
        empty.querySelector('span').textContent = 'Establishing SSE connection';
        break;
      case 'waiting':
        empty.style.display = 'flex';
        empty.querySelector('p').textContent = 'Waiting for events';
        empty.querySelector('span').textContent = 'Connected, listening for updates';
        indicator.classList.add('is-active');
        break;
      case 'active':
        empty.style.display = 'none';
        indicator.classList.add('is-active');
        break;
    }
  }
  function updateEventCount() {
    const text = eventCounter === 1 ? '1 event' : `${eventCounter} events`;
    DOM.eventCount.textContent = text;
  }
  function updateSubscriptionCount() {
    const cards = DOM.subscriptionsGrid.children;
    const total = cards.length;
    let clientCount = 0;
    let monitorCount = 0;

    for (const card of cards) {
      const badge = card.querySelector('.subscription-badge');
      if (badge?.classList.contains('is-monitor')) {
        monitorCount++;
      } else if (badge?.classList.contains('is-client')) {
        clientCount++;
      }
    }

    DOM.subscriptionCountTotal.textContent = `${total} total`;

    DOM.subscriptionCountClient.textContent = `${clientCount} client${clientCount !== 1 ? 's' : ''}`;
    DOM.subscriptionCountClient.dataset.count = clientCount.toString();

    DOM.subscriptionCountMonitor.textContent = `${monitorCount} monitor${monitorCount !== 1 ? 's' : ''}`;
    DOM.subscriptionCountMonitor.dataset.count = monitorCount.toString();

    DOM.subscriptionsEmpty.style.display = total === 0 ? 'flex' : 'none';
  }
  function updateCookieButtonState(cookieSet) {
    hasCookie = cookieSet;
    DOM.clearCookieButton.disabled = !hasCookie;
  }
  function showNotification(message, type = 'error', duration = 5000) {
    clearTimeout(notificationTimeoutId);
    const existingToast = document.querySelector('.app-notification');
    if (existingToast) existingToast.remove();

    const bulmaType = type === 'error' ? 'danger' : type;
    const notification = document.createElement('div');
    notification.className = `notification app-notification is-${bulmaType}`;
    notification.textContent = message;
    notification.setAttribute('role', type === 'error' ? 'alert' : 'status');
    notification.setAttribute('aria-live', type === 'error' ? 'assertive' : 'polite');
    document.body.appendChild(notification);

    setTimeout(() => notification.classList.add('is-visible'), 10);

    notificationTimeoutId = setTimeout(() => {
      notification.classList.remove('is-visible');
      notification.addEventListener('transitionend', () => notification.remove(), { once: true });
      setTimeout(() => {
        if (notification.parentNode) notification.remove();
      }, 500);
    }, duration);
  }
  function clearValidationError(input) {
    input.classList.remove('is-danger');
    input.removeAttribute('aria-invalid');
    input.removeAttribute('aria-describedby');
    if (input.hasAttribute('data-original-placeholder')) {
      input.placeholder = input.getAttribute('data-original-placeholder');
      input.removeAttribute('data-original-placeholder');
    }
    const errorHelp = input.closest('.field')?.querySelector('.help.is-danger.validation-error');
    if (errorHelp) errorHelp.remove();
  }
  function showValidationError(input, message, asPlaceholder = false) {
    clearValidationError(input);
    input.classList.add('is-danger');
    input.setAttribute('aria-invalid', 'true');

    if (asPlaceholder) {
      input.setAttribute('data-original-placeholder', input.placeholder || '');
      input.placeholder = message;
    } else {
      const field = input.closest('.field');
      if (field) {
        const errorId = `${input.id || input.name}-error`;
        const helpText = document.createElement('p');
        helpText.id = errorId;
        helpText.className = 'help is-danger validation-error';
        helpText.textContent = message;
        field.appendChild(helpText);
        input.setAttribute('aria-describedby', errorId);
      }
    }
  }
  function validateInput(input) {
    clearValidationError(input);

    if (input.hasAttribute('required') && !input.value.trim()) {
      showValidationError(input, 'Required', true);
      return false;
    } else if (input.type === 'url' && input.value.trim()) {
      try {
        new URL(input.value.trim());
      } catch (err) {
        Logger.debug('[Validation] Invalid URL.', {
          value: input.value,
          error: err.message,
        });
        showValidationError(input, 'Please enter a valid URL (e.g., https://example.com)');
        return false;
      }
    }
    return true;
  }
  function validateForm(form, options = {}) {
    let isValid = true;

    for (const input of form.querySelectorAll('input, textarea')) {
      if (!validateInput(input)) {
        isValid = false;
      }
    }

    if (options.includeHubUrl) {
      if (!validateInput(DOM.settingsForm.hubUrl)) {
        isValid = false;
      }
    }

    if (options.includeJwt) {
      if (!validateInput(DOM.settingsForm.jwt)) {
        isValid = false;
      }
    }

    return isValid;
  }
  function parseLinkHeaders(resp) {
    const links = {};
    const linkHeader = resp.headers.get('Link');
    if (linkHeader) {
      // Split by comma, but only when followed by '<' to avoid splitting on commas in quoted values
      const linkValues = linkHeader.split(/,(?=\s*<)/);

      for (const linkValue of linkValues) {
        const urlMatch = linkValue.match(/<([^>]+)>/);
        // Match rel parameter anywhere in the link value (handles any parameter order)
        const relMatch = linkValue.match(/;\s*rel=["']?([^"';\s,]+)["']?/i);

        if (urlMatch && relMatch) {
          links[relMatch[1]] = urlMatch[1];
        }
      }
    }

    if (!links.mercure) {
      throw new Error('Invalid response from hub: "mercure" Link header is required.');
    }

    return links;
  }
  function createStatefulSSECallbacks(
    signal,
    { onFatal = () => {}, context = 'event stream' } = {},
  ) {
    let retryCount = 0;
    let successfulConnectionTimeout;
    let lastServerRetry = null;

    const cleanup = () => clearTimeout(successfulConnectionTimeout);
    signal.addEventListener('abort', () => {
      Logger.debug(`[SSE] Abort signal received for ${context}.`);
      cleanup();
    });

    return {
      captureRetry(event) {
        if (typeof event.retry === 'number') {
          lastServerRetry = event.retry;
          Logger.debug(`[SSE] Server-sent retry: ${lastServerRetry}ms.`);
          return lastServerRetry;
        }
        // No retry field in this event — preserve any previously received server value.
        return lastServerRetry ?? CONFIG.FES_RETRY_BASE_DELAY_MS;
      },

      async onopen(response) {
        if (response.ok && response.headers.get('content-type')?.includes('text/event-stream')) {
          Logger.info(`[SSE] Connected to ${context}.`);
          showNotification(`Connected to ${context}.`, 'success', 3000);

          cleanup();
          successfulConnectionTimeout = setTimeout(() => {
            // After a stable connection, reset the state for the next failure cycle.
            retryCount = 0;
            lastServerRetry = null;
            Logger.debug(`[SSE] Connection to ${context} stable, retry state reset.`);
          }, CONFIG.FES_RETRY_SUCCESS_RESET_MS);
          return;
        }
        if (response.status >= 400 && response.status < 500 && response.status !== 429) {
          const errorBody = await response.text();
          throw new FatalError(
            `Client-side error: ${response.status} ${response.statusText} - ${errorBody}`,
          );
        }
        throw new RetriableError();
      },

      onclose() {
        cleanup();
        Logger.warn(`[SSE] Connection to ${context} closed by hub, will reconnect.`);
        throw new RetriableError();
      },

      onerror(err) {
        cleanup();
        if (err instanceof FatalError) {
          Logger.error(`[SSE] Fatal error on ${context}, halting retries.`, err);
          showNotification(
            `Failed to subscribe to ${context}: ${formatErrorMessage(err)}.`,
            'error',
          );
          onFatal();
          throw err;
        }

        let retryInterval;
        const jitter = (Math.random() - 0.5) * CONFIG.FES_RETRY_BASE_DELAY_MS;

        // Stage 1: Use server-sent value for the first attempt only.
        if (retryCount === 0 && lastServerRetry !== null) {
          Logger.debug(`[SSE] Using server-sent delay: ${lastServerRetry}ms.`);
          // The hub's value is the effective max; we just ensure it's not below our absolute minimum.
          retryInterval = Math.max(CONFIG.FES_RETRY_BASE_DELAY_MS, lastServerRetry + jitter);
        } else {
          // Stage 2: Use client-side exponential backoff, capped at 30 seconds.
          const baseDelay = CONFIG.FES_RETRY_BASE_DELAY_MS * 2 ** retryCount;
          retryInterval = Math.min(
            CONFIG.FES_RETRY_MAX_DELAY_MS,
            Math.max(CONFIG.FES_RETRY_BASE_DELAY_MS, baseDelay + jitter),
          );
        }

        retryCount++;
        Logger.warn(
          `[SSE] Connection to ${context} lost, retry #${retryCount} in ${Math.round(retryInterval)}ms.`,
          err,
        );
        showNotification(
          `Connection to ${context} lost. Retrying (attempt ${retryCount})...`,
          'info',
          2000,
        );
        return retryInterval;
      },
    };
  }
  function getAuthOptions({ anonymous = false } = {}) {
    if (anonymous) {
      return { credentials: 'omit' };
    }
    const options = {};
    const authType = DOM.settingsForm.authorization.value;
    if (authType === 'header') {
      options.headers = {
        Authorization: `Bearer ${DOM.settingsForm.jwt.value}`,
      };
    } else if (authType === 'cookie') {
      options.credentials = 'include';
    }
    return options;
  }
  function formatErrorMessage(err) {
    const msg = err.message?.trim() || 'Unknown error';
    return msg.toLowerCase().includes('failed to fetch')
      ? 'Could not reach hub (network error or CORS)'
      : msg;
  }
  function openJwtIo() {
    const token = DOM.settingsForm.jwt.value;
    const jwtUrl = token ? `https://jwt.io/#token=${encodeURIComponent(token)}` : 'https://jwt.io/';
    window.open(jwtUrl, '_blank', 'noopener');
  }
  async function fetchAndCopy(url, successMessage, { withJwtIo = false } = {}) {
    if (withJwtIo) openJwtIo();
    let text;
    try {
      const resp = await fetch(url);
      if (!resp.ok) throw new Error(`${resp.status} ${resp.statusText}`);
      text = await resp.text();
    } catch (err) {
      showNotification(`Failed to load file: ${formatErrorMessage(err)}.`, 'error');
      Logger.error('[Fetch] Failed to load file.', { url, error: err });
      return;
    }
    await copyAndNotify(text, successMessage);
  }
  async function copyAndNotify(text, successMessage, { withJwtIo = false } = {}) {
    if (withJwtIo) openJwtIo();
    try {
      await navigator.clipboard.writeText(text);
      showNotification(successMessage, 'success', 4000);
    } catch (err) {
      showNotification('Failed to copy to clipboard.', 'error');
      Logger.error('[Clipboard] Failed to write to clipboard.', err);
    }
  }
  function makeClickable(el) {
    el.setAttribute('tabindex', '0');
    el.setAttribute('role', 'button');
    el.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        el.click();
      }
    });
  }
  async function copyToClipboard(text, label) {
    try {
      await navigator.clipboard.writeText(text);
      showNotification(`${label} copied.`, 'success', 1500);
    } catch (err) {
      showNotification(`Failed to copy ${label.toLowerCase()}.`, 'warning', 1500);
      Logger.warn(`[Clipboard] Failed to copy ${label.toLowerCase()}.`, err);
    }
  }
  async function handleDiscoverSubmit(e) {
    e.preventDefault();
    if (!validateForm(e.target, { includeHubUrl: false })) return;
    const {
      elements: { topic, body },
    } = e.target;
    const jwt = DOM.settingsForm.jwt.value;
    let url;
    try {
      url = new URL(topic.value);
    } catch (err) {
      showNotification('Invalid topic URL.', 'error');
      Logger.debug('[Discovery] Invalid topic URL.', {
        value: topic.value,
        error: err,
      });
      return;
    }
    if (body.value) url.searchParams.append('body', body.value);
    // Demo handler reads ?jwt= to set the auth cookie (demo.go). This is distinct from
    // the Mercure spec's ?authorization= param and RFC 6750's ?access_token= param.
    if (jwt) {
      url.searchParams.append('jwt', jwt);
    } else {
      Logger.warn('[Discovery] No JWT provided - auth cookie will be cleared.');
      showNotification('No JWT provided - auth cookie will be cleared.', 'warning', 3000);
    }

    Logger.info(`[Discovery] Fetching topic: ${url.href}`);
    try {
      const resp = await fetch(url, { credentials: 'include' });
      if (!resp.ok) throw new Error(`${resp.status} ${resp.statusText}`);

      const links = parseLinkHeaders(resp);
      // Fall back to topic URL per spec: "If the Link with rel=self is omitted,
      // the current URL of the resource MUST be used as a fallback."
      const selfUrl = new URL(links.self || topic.value, topic.value);
      const cleanSelfUrl = selfUrl.origin + selfUrl.pathname;

      // Auto-populate hub URL and propagate the new origin to all fields.
      // Skip discover fields — the user's topic URL triggered this discovery.
      // The Link rel="mercure" value is third-party-controlled, so strip
      // any userinfo a malicious topic server tried to plant; otherwise it
      // would silently land in the hubUrl field and `fetch` would emit a
      // Basic auth header on every later subscribe/publish.
      const hubUrl = new URL(links.mercure, topic.value);
      if (hubUrl.username || hubUrl.password) {
        Logger.warn('[Discovery] Hub URL contained userinfo; stripping.', {
          original: hubUrl.href,
        });
        showNotification(
          'Hub URL credentials stripped (security): topic server tried to plant userinfo.',
          'warning',
          5000,
        );
        hubUrl.username = '';
        hubUrl.password = '';
      }
      DOM.settingsForm.hubUrl.value = hubUrl.toString();
      propagateHubUrl(DOM.settingsForm.hubUrl.value, { skipDiscover: true });

      body.value = await resp.text();
      showNotification('Discovery successful.', 'success');
      Logger.info(
        `[Discovery] Success. Hub: ${DOM.settingsForm.hubUrl.value}, Topic: ${cleanSelfUrl}`,
      );
      DOM.settingsForm.authorization.value = 'cookie';

      // Track cookie state: cookie is set if JWT was provided, deleted if not
      updateCookieButtonState(!!jwt);
    } catch (err) {
      showNotification(`Discovery failed: ${formatErrorMessage(err)}.`, 'error');
      Logger.error('[Discovery] Failed.', err);
    } finally {
      // Re-enable cookie radio (disabled during cookie-radio-triggered discovery)
      const cookieRadio = DOM.settingsForm.querySelector(
        'input[name="authorization"][value="cookie"]',
      );
      if (cookieRadio) cookieRadio.disabled = false;
    }
  }
  function handleSubscribeSubmit(e) {
    e.preventDefault();
    const isAnonymous = e.target.elements.anonymous.checked;
    const needsJwt = !isAnonymous && DOM.settingsForm.authorization.value === 'header';
    if (!validateForm(e.target, { includeHubUrl: true, includeJwt: needsJwt })) return;
    if (updateCtrl) updateCtrl.abort();
    updateCtrl = new AbortController();

    const {
      elements: { topics, lastEventId, subscribe, unsubscribe },
    } = e.target;
    let u;
    try {
      u = new URL(DOM.settingsForm.hubUrl.value);
    } catch (err) {
      showNotification('Invalid hub URL.', 'error');
      Logger.debug('[Subscribe] Invalid hub URL.', {
        value: DOM.settingsForm.hubUrl.value,
        error: err,
      });
      return;
    }
    const topicList = parseTopicList(topics.value);
    for (const topic of topicList) {
      u.searchParams.append('topic', topic);
    }
    if (lastEventId.value) {
      u.searchParams.append('lastEventID', lastEventId.value);
    }

    Logger.info('[SSE] Subscribing to main event stream.', {
      topics: topicList,
      lastEventId: lastEventId.value || '(none)',
      anonymous: isAnonymous,
    });
    showNotification(
      isAnonymous ? 'Connecting anonymously...' : 'Connecting to main event stream...',
      'info',
    );
    setLiveUpdatesState('connecting');

    const { onopen, onclose, onerror, captureRetry } = createStatefulSSECallbacks(
      updateCtrl.signal,
      {
        context: 'main event stream',
        onFatal: () => {
          subscribe.disabled = false;
          unsubscribe.disabled = true;
          e.target.elements.anonymous.disabled = false;
          e.target.elements.lastEventId.disabled = false;
          e.target.elements.openWhenHidden.disabled = false;
          setLiveUpdatesState('idle');
          updateFormStates();
        },
      },
    );

    const options = {
      ...getAuthOptions({ anonymous: isAnonymous }),
      signal: updateCtrl.signal,
      onopen: async (response) => {
        await onopen(response);
        if (response.ok && DOM.updatesList.children.length === 0) {
          setLiveUpdatesState('waiting');
        }
      },
      onmessage(event) {
        const effectiveRetry = captureRetry(event);
        DOM.retryValue.textContent = `${effectiveRetry}ms`;

        Logger.debug('[SSE] Received event.', {
          id: event.id,
          type: event.event || '(default)',
          dataLength: event.data?.length ?? 0,
        });
        if (event.event === 'FatalError') throw new FatalError(sanitizeFatalEventData(event.data));

        if (event.data !== undefined) {
          // "" is valid (signal-only event)
          setLiveUpdatesState('active');

          const li = document.importNode(DOM.updateTemplate.content, true);

          const idEl = li.querySelector('.event-id');
          idEl.textContent = event.id;
          idEl.title = 'Click to copy';
          makeClickable(idEl);
          idEl.addEventListener('click', (e) => {
            e.stopPropagation();
            copyToClipboard(event.id, 'Event ID');
          });

          const typeTag = li.querySelector('.event-type');
          if (event.event) {
            typeTag.textContent = event.event;
          } else {
            typeTag.style.display = 'none';
          }

          const codeBlock = li.querySelector('.event-data code');
          if (event.data === '') {
            // Empty data is valid (signal-only event) - show indicator
            codeBlock.textContent = '(no data)';
            codeBlock.classList.add('is-empty-data');
          } else {
            let displayData = event.data;
            let isJson = false;
            try {
              const parsed = JSON.parse(event.data);
              displayData = JSON.stringify(parsed, null, 2);
              isJson = true;
            } catch (err) {
              Logger.debug('[SSE] Event data is not JSON, displaying as plain text.', err);
            }
            codeBlock.textContent = displayData;
            if (isJson && window.hljs) {
              codeBlock.classList.add('language-json');
              hljs.highlightElement(codeBlock);
            }
          }

          const eventItem = li.querySelector('.event-item');
          eventItem.classList.add('is-new');

          const list = DOM.updatesList;
          list.firstChild ? list.insertBefore(li, list.firstChild) : list.appendChild(li);

          eventCounter++;
          updateEventCount();

          // Cap DOM size to prevent memory exhaustion during long sessions
          while (list.children.length > 100) {
            list.removeChild(list.lastChild);
          }
        }
      },
      onclose,
      onerror,
      openWhenHidden: e.target.elements.openWhenHidden.checked,
    };

    fetchEventSource(u, options).catch((err) => {
      if (updateCtrl.signal.aborted) {
        Logger.debug('[SSE] fetchEventSource aborted.', err);
        return;
      }
      if (err instanceof FatalError) return; // Already handled in onerror
      Logger.error('[SSE] Unexpected fetchEventSource rejection.', err);
      showNotification('Unexpected error in event stream. Check console.', 'error');
    });

    subscribe.disabled = true;
    unsubscribe.disabled = false;
    e.target.elements.anonymous.disabled = true;
    e.target.elements.lastEventId.disabled = true;
    e.target.elements.openWhenHidden.disabled = true;
    updateFormStates();
  }
  async function handlePublishSubmit(e) {
    e.preventDefault();
    const needsJwt = DOM.settingsForm.authorization.value === 'header';
    if (!validateForm(e.target, { includeHubUrl: true, includeJwt: needsJwt })) return;
    const {
      elements: { topics, data, priv, id, type, retry },
    } = e.target;
    const body = new URLSearchParams({ data: data.value });
    if (id.value) body.append('id', id.value);
    if (type.value) body.append('type', type.value);
    if (retry.value) body.append('retry', retry.value);
    const topicList = parseTopicList(topics.value);
    for (const topic of topicList) {
      body.append('topic', topic);
    }
    if (priv.checked) body.append('private', 'on');

    const options = { ...getAuthOptions(), method: 'POST', body };

    Logger.info('[Publish] Sending update.', {
      topics: topicList,
      private: priv.checked,
    });
    try {
      const resp = await fetch(DOM.settingsForm.hubUrl.value, options);
      if (!resp.ok) throw new Error(`${resp.status} ${resp.statusText}`);
      const eventId = await resp.text();
      showNotification('Update published.', 'success');
      Logger.info(`[Publish] Success. Event ID: ${eventId.trim() || '(not returned)'}`);
    } catch (err) {
      showNotification(`Publish failed: ${formatErrorMessage(err)}.`, 'error');
      Logger.error('[Publish] Failed.', err);
    }
  }
  function renderSubscriptionCard(s) {
    const card = document.importNode(DOM.subscriptionTemplate.content, true);
    const article = card.querySelector('article');
    article.id = s.id;
    const isMonitor = s.topic.includes('/.well-known/mercure/subscriptions');

    const topicWrapper = article.querySelector('.subscription-topic-wrapper');
    const topicEl = article.querySelector('.subscription-topic');
    topicEl.textContent = s.topic;
    const truncatedTopic = truncateWithEllipsis(s.topic, 60);
    topicWrapper.dataset.tooltip = `${truncatedTopic}\n\nClick to copy`;
    makeClickable(topicEl);
    topicEl.addEventListener('click', () => copyToClipboard(s.topic, 'Topic'));

    const badge = article.querySelector('.subscription-badge');
    if (isMonitor) {
      badge.textContent = 'Monitor';
      badge.classList.add('is-monitor');
    } else {
      badge.textContent = 'Client';
      badge.classList.add('is-client');
    }

    const subEl = article.querySelector('.subscription-subscriber');
    subEl.textContent = s.subscriber;
    const truncatedSub = truncateWithEllipsis(s.subscriber, 60);
    subEl.dataset.tooltip = `${truncatedSub}\n\nClick to copy`;
    makeClickable(subEl);
    subEl.addEventListener('click', () => copyToClipboard(s.subscriber, 'Subscriber'));

    // Payload is optional (from JWT mercure.payload claim)
    const payloadDetails = article.querySelector('.subscription-payload-details');
    if (s.payload && Object.keys(s.payload).length > 0) {
      const payloadCode = article.querySelector('.subscription-payload code');
      payloadCode.textContent = JSON.stringify(s.payload, null, 2);
      if (window.hljs) hljs.highlightElement(payloadCode);
    } else {
      payloadDetails.remove();
    }

    if (isMonitor) {
      const firstClient = DOM.subscriptionsGrid
        .querySelector('.is-client')
        ?.closest('.subscription-card');
      DOM.subscriptionsGrid.insertBefore(card, firstClient || null);
    } else {
      DOM.subscriptionsGrid.appendChild(card);
    }
    updateSubscriptionCount();
  }
  // Replace the subscriptions grid with the absolute state from the hub's
  // /subscriptions snapshot. Used both on the initial Subscribe click and on
  // every successful SSE (re)connect — without the on-reconnect call, ghost
  // cards from before a hub restart linger because the new hub instance never
  // emits "destroyed" events for subscribers it doesn't know about.
  //
  // Cosmetic note: during a hub restart the two SSE streams (main + presence)
  // reconnect with independent jitter, so the presence stream can win the
  // race by a few hundred ms. While main is still mid-reconnect, this
  // snapshot truthfully reports it as absent and the client card briefly
  // disappears, then reappears when main's reconnect emits its
  // subscription-created delta. Functionally correct; intentionally not
  // smoothed (debounce/coordination would obscure real subscription churn).
  async function syncSubscriptionsSnapshot(subscriptionsUrl, fetchOptions) {
    const resp = await fetch(subscriptionsUrl, fetchOptions);
    if (!resp.ok) throw new Error(`${resp.status} ${resp.statusText}`);
    const json = await resp.json();
    if (!Array.isArray(json?.subscriptions)) {
      throw new Error('Malformed subscriptions response: "subscriptions" array missing.');
    }
    DOM.subscriptionsGrid.replaceChildren();
    let rendered = 0;
    for (const subscription of json.subscriptions) {
      if (!isValidSubscription(subscription)) {
        Logger.warn('[Presence] Skipping malformed snapshot entry.', subscription);
        continue;
      }
      renderSubscriptionCard(subscription);
      rendered++;
    }
    updateSubscriptionCount();
    Logger.info(`[Presence] Snapshot loaded: ${json.subscriptions.length} subscription(s).`);
    // If the hub returned entries but every one of them failed validation,
    // the grid is now empty without any user-visible signal beyond a
    // console warn. Surface a toast so the operator knows the empty
    // panel is "everything was malformed", not "nothing live".
    if (json.subscriptions.length > 0 && rendered === 0) {
      showNotification(
        `Subscriptions snapshot returned ${json.subscriptions.length} entr${json.subscriptions.length === 1 ? 'y' : 'ies'}, all malformed. Grid cleared. Check console for details.`,
        'warning',
        5000,
      );
    }
  }

  function handleSubscriptionsSubmit(e) {
    e.preventDefault();
    // Active Subscriptions needs Hub URL and JWT (no form fields of its own)
    const needsJwt = DOM.settingsForm.authorization.value === 'header';
    if (
      !validateInput(DOM.settingsForm.hubUrl) ||
      (needsJwt && !validateInput(DOM.settingsForm.jwt))
    )
      return;
    if (subscriptionCtrl) subscriptionCtrl.abort();
    subscriptionCtrl = new AbortController();
    const {
      elements: { subscribe, unsubscribe },
    } = e.target;

    DOM.subscriptionsGrid.replaceChildren();
    updateSubscriptionCount();
    showNotification('Connecting to active subscriptions stream...', 'info');

    const fetchOptions = {
      ...getAuthOptions(),
      signal: subscriptionCtrl.signal,
    };
    // Build the subscriptions URL via the URL API so we don't double-slash
    // when the hub URL was entered with a trailing slash, and don't lose
    // the existing path when joining.
    const subscriptionsUrl = new URL(DOM.settingsForm.hubUrl.value);
    subscriptionsUrl.pathname = `${subscriptionsUrl.pathname.replace(/\/+$/, '')}/subscriptions`;

    const u = new URL(DOM.settingsForm.hubUrl.value);
    u.searchParams.append('topic', '/.well-known/mercure/subscriptions{/topic}{/subscriber}');
    // No lastEventID on the SSE URL: the snapshot in onopen is authoritative
    // for state at (re)connect time, and skipping replay avoids the
    // race where historical events get applied on top of a fresher snapshot.

    const { onopen, onclose, onerror, captureRetry } = createStatefulSSECallbacks(
      subscriptionCtrl.signal,
      {
        context: 'active subscriptions stream',
        onFatal: () => {
          subscribe.disabled = false;
          unsubscribe.disabled = true;
          updateFormStates();
        },
      },
    );

    const sseOptions = {
      ...fetchOptions,
      async onopen(response) {
        await onopen(response);
        if (subscriptionCtrl.signal.aborted) return;
        try {
          await syncSubscriptionsSnapshot(subscriptionsUrl, fetchOptions);
        } catch (err) {
          if (subscriptionCtrl.signal.aborted || err.name === 'AbortError') return;
          Logger.warn(
            '[Presence] Snapshot fetch failed; live deltas will still apply but the grid may be stale.',
            err,
          );
          showNotification(
            `Could not refresh subscriptions snapshot: ${formatErrorMessage(err)}.`,
            'warning',
            4000,
          );
        }
      },
      onmessage(event) {
        captureRetry(event);
        Logger.debug('[Presence] Received event.', {
          id: event.id,
          dataLength: event.data?.length ?? 0,
        });
        if (event.event === 'FatalError') throw new FatalError(sanitizeFatalEventData(event.data));

        // Subscription events require valid JSON data; empty strings are not valid JSON
        if (event.data) {
          let s;
          try {
            s = JSON.parse(event.data);
          } catch (e) {
            Logger.error('[Presence] Failed to parse event data as JSON.', e, {
              eventId: event.id,
              raw: event.data,
            });
            showNotification(
              `Received malformed subscription data (event ${event.id || 'unknown'}).`,
              'warning',
            );
            return;
          }
          if (!isValidSubscription(s)) {
            Logger.warn('[Presence] Subscription event missing required fields, skipping.', {
              eventId: event.id,
              parsed: s,
            });
            return;
          }
          const existingSub = document.getElementById(s.id);
          if (s.active) {
            // Refresh card if metadata changed, or create new if doesn't exist
            if (existingSub) {
              const existingPayload =
                existingSub.querySelector('.subscription-payload code')?.textContent ?? null;
              const hasPayload = s.payload && Object.keys(s.payload).length > 0;
              const newPayload = hasPayload ? JSON.stringify(s.payload, null, 2) : null;
              if (existingPayload !== newPayload) {
                existingSub.remove();
                renderSubscriptionCard(s);
              }
            } else {
              renderSubscriptionCard(s);
            }
          } else {
            if (existingSub) {
              existingSub.remove();
              updateSubscriptionCount();
            }
          }
        }
      },
      onclose,
      onerror,
      openWhenHidden: CONFIG.FES_OPEN_WHEN_HIDDEN_PRESENCE,
    };

    fetchEventSource(u, sseOptions).catch((err) => {
      if (subscriptionCtrl.signal.aborted) {
        Logger.debug('[Presence] fetchEventSource aborted.', err);
        return;
      }
      if (err instanceof FatalError) return; // Already handled in onerror
      Logger.error('[Presence] Unexpected fetchEventSource rejection.', err);
      showNotification('Unexpected error in subscription stream. Check console.', 'error');
    });

    subscribe.disabled = true;
    unsubscribe.disabled = false;
    updateFormStates();
  }
  function updateJwtPayloadDisplay() {
    const token = DOM.settingsForm.jwt.value;
    const headerContainer = DOM.jwtHeader;
    const payloadContainer = DOM.jwtPayload;

    headerContainer.textContent = '';
    payloadContainer.textContent = '';
    headerContainer.classList.remove('has-text-danger', 'hljs', 'language-json');
    payloadContainer.classList.remove('has-text-danger', 'hljs', 'language-json');
    delete headerContainer.dataset.highlighted;
    delete payloadContainer.dataset.highlighted;

    if (!token) {
      headerContainer.textContent = 'Enter a JWT to see its header.';
      payloadContainer.textContent = 'Enter a JWT to see its payload.';
      headerContainer.classList.add('has-text-danger');
      payloadContainer.classList.add('has-text-danger');
      return;
    }

    const parts = token.split('.');
    if (parts.length !== 3) {
      const errorMsg = 'Malformed JWT: Must contain 3 parts separated by dots.';
      headerContainer.textContent = errorMsg;
      payloadContainer.textContent = errorMsg;
      headerContainer.classList.add('has-text-danger');
      payloadContainer.classList.add('has-text-danger');
      Logger.warn(`[Auth] Invalid JWT structure: expected 3 parts, got ${parts.length}.`);
      return;
    }

    const [headerB64, payloadB64] = parts;
    decodeAndDisplayJwtPart(headerB64, headerContainer, 'Header');
    decodeAndDisplayJwtPart(payloadB64, payloadContainer, 'Payload');
  }
  function decodeAndDisplayJwtPart(base64Str, container, label) {
    try {
      const decoded = decodeBase64Url(base64Str);
      const parsed = JSON.parse(decoded);
      container.textContent = JSON.stringify(parsed, null, 2);
      if (window.hljs) {
        container.classList.add('language-json');
        hljs.highlightElement(container);
      }
    } catch (e) {
      container.textContent = `Malformed ${label}: Could not decode.`;
      container.classList.add('has-text-danger');
      Logger.warn(`[Auth] Failed to decode JWT ${label.toLowerCase()}.`, e);
    }
  }
  async function setDefaultValues() {
    DOM.settingsForm.hubUrl.value = CONFIG.UI_DEFAULT_HUB_URL;

    const tokens = await fetchDemoTokens();
    if (tokens) {
      applyTokenForCurrentAlgorithm(tokens, '[Init]');
    } else {
      DOM.settingsForm.jwt.value = '';
    }

    DOM.subscribeForm.topics.value = CONFIG.UI_PLACEHOLDER_TOPIC;
    DOM.publishForm.topics.value = CONFIG.UI_PLACEHOLDER_TOPIC;
    DOM.discoverForm.topic.value = CONFIG.UI_DISCOVER_TOPIC;
    DOM.discoverForm.body.value = JSON.stringify(
      {
        '@id': CONFIG.UI_PLACEHOLDER_TOPIC,
        availability: 'https://schema.org/InStock',
      },
      null,
      2,
    );

    DOM.publishForm.data.value = JSON.stringify(
      {
        '@id': CONFIG.UI_PLACEHOLDER_TOPIC,
        availability: 'https://schema.org/OutOfStock',
      },
      null,
      2,
    );

    DOM.subscribeTopicsExamples.textContent = `${window.location.origin}/demo/books/{id}.jsonld\n${CONFIG.UI_PLACEHOLDER_TOPIC}`;
  }
  function initializeEventListeners() {
    DOM.discoverForm.addEventListener('submit', handleDiscoverSubmit);
    DOM.subscribeForm.addEventListener('submit', handleSubscribeSubmit);
    DOM.publishForm.addEventListener('submit', handlePublishSubmit);
    DOM.subscriptionsForm.addEventListener('submit', handleSubscriptionsSubmit);
    DOM.settingsForm.jwt.addEventListener('input', updateJwtPayloadDisplay);

    DOM.retryValue.textContent = `${CONFIG.FES_RETRY_BASE_DELAY_MS}ms`;

    for (const input of document.querySelectorAll('input, textarea')) {
      input.addEventListener('input', () => clearValidationError(input));
    }

    const cookieAuthRadio = DOM.settingsForm.querySelector(
      'input[name="authorization"][value="cookie"]',
    );
    DOM.settingsForm.hubUrl.addEventListener('change', () => {
      propagateHubUrl(DOM.settingsForm.hubUrl.value);
    });

    cookieAuthRadio.addEventListener('click', (e) => {
      // Prevent the radio from selecting until discovery succeeds.
      // handleDiscoverSubmit sets authorization.value = 'cookie' on success.
      e.preventDefault();
      if (!validateInput(DOM.settingsForm.jwt)) return;
      cookieAuthRadio.disabled = true;
      DOM.discoverForm.requestSubmit();
    });

    const headerAuthRadio = DOM.settingsForm.querySelector(
      'input[name="authorization"][value="header"]',
    );
    headerAuthRadio.addEventListener('click', () => {
      if (!hasCookie) return;
      // Clear stale cookie when switching to Header mode to prevent it from interfering
      DOM.clearCookieButton.click();
    });

    // Clear Cookie button - triggers Discover without JWT to delete the auth cookie
    DOM.clearCookieButton.addEventListener('click', async () => {
      const topicUrl = DOM.discoverForm.topic.value;
      if (!topicUrl) {
        showNotification('Enter a topic URL first to clear the cookie.', 'warning');
        return;
      }

      Logger.info('[Auth] Clearing cookie via discovery without JWT.');
      try {
        const resp = await fetch(topicUrl, { credentials: 'include' });
        if (!resp.ok) throw new Error(`${resp.status} ${resp.statusText}`);
        updateCookieButtonState(false);
        showNotification('Auth cookie cleared.', 'success');
        Logger.info('[Auth] Cookie cleared successfully.');
      } catch (err) {
        showNotification(`Failed to clear cookie: ${formatErrorMessage(err)}.`, 'error');
        Logger.error('[Auth] Failed to clear cookie.', err);
      }
    });

    DOM.copyPublicJwkButton.addEventListener('click', () =>
      fetchAndCopy(
        CONFIG.UI_PUBLIC_JWK_PATH,
        "Public JWK copied. Paste in jwt.io's signature section.",
        {
          withJwtIo: true,
        },
      ),
    );
    DOM.copyPrivateJwkButton.addEventListener('click', () =>
      fetchAndCopy(
        CONFIG.UI_PRIVATE_JWK_PATH,
        "Private JWK copied. Paste in jwt.io's 'Sign JWT' field.",
        {
          withJwtIo: true,
        },
      ),
    );
    DOM.copyJwksButton.addEventListener('click', () =>
      fetchAndCopy(CONFIG.UI_JWKS_PATH, 'JWKS copied. Host it and point publisher_jwks_url at it.'),
    );
    DOM.copyPublicPemButton.addEventListener('click', () =>
      fetchAndCopy(
        CONFIG.UI_PUBLIC_PEM_PATH,
        'PEM public key copied. Use with publisher_jwt + RS256.',
      ),
    );

    DOM.copyJwtButton.addEventListener('click', async () => {
      const token = DOM.settingsForm.jwt.value;
      if (!token) {
        showNotification('No token to copy.', 'warning');
        return;
      }
      await copyAndNotify(token, 'Token copied.');
    });

    for (const radio of DOM.settingsForm.querySelectorAll('input[name="jwtAlgorithm"]')) {
      radio.addEventListener('change', () => {
        updateAlgorithmSections();
        loadTokenForAlgorithm();
      });
    }

    DOM.copyHS256SecretButton.addEventListener('click', () =>
      copyAndNotify(
        CONFIG.UI_HS256_SECRET,
        "Secret copied. Paste in jwt.io's SIGN JWT: SECRET field.",
        {
          withJwtIo: true,
        },
      ),
    );
    DOM.copyHS256SecretVerifyButton.addEventListener('click', () =>
      copyAndNotify(CONFIG.UI_HS256_SECRET, "Secret copied. Paste in jwt.io's SECRET field.", {
        withJwtIo: true,
      }),
    );
    DOM.copyHS256ConfigButton.addEventListener('click', () =>
      copyAndNotify(
        CONFIG.UI_HS256_SECRET,
        'Secret copied. Add to publisher_jwt and subscriber_jwt config.',
      ),
    );

    DOM.subscribeForm.elements.unsubscribe.addEventListener('click', (e) => {
      e.preventDefault();
      if (updateCtrl) updateCtrl.abort();
      e.currentTarget.disabled = true;
      DOM.subscribeForm.elements.subscribe.disabled = false;
      DOM.subscribeForm.elements.anonymous.disabled = false;
      DOM.subscribeForm.elements.lastEventId.disabled = false;
      DOM.subscribeForm.elements.openWhenHidden.disabled = false;
      setLiveUpdatesState('idle');
      updateFormStates();
      showNotification('Unsubscribed from main event stream.', 'info');
      Logger.info('[SSE] Unsubscribed from main event stream.');
    });

    DOM.subscriptionsForm.elements.unsubscribe.addEventListener('click', (e) => {
      e.preventDefault();
      if (subscriptionCtrl) subscriptionCtrl.abort();
      e.currentTarget.disabled = true;
      DOM.subscriptionsForm.elements.subscribe.disabled = false;
      DOM.subscriptionsGrid.replaceChildren();
      updateSubscriptionCount();
      updateFormStates();
      showNotification('Unsubscribed from active subscriptions stream.', 'info');
      Logger.info('[Presence] Unsubscribed from active subscriptions stream.');
    });

    if (DOM.retryInput && DOM.retryDecrement && DOM.retryIncrement) {
      const min = 1000;
      const max = 30000;
      const step = 500;
      DOM.retryDecrement.addEventListener('click', () => {
        const current = parseInt(DOM.retryInput.value, 10) || min;
        DOM.retryInput.value = Math.max(min, current - step);
      });
      DOM.retryIncrement.addEventListener('click', () => {
        const current = parseInt(DOM.retryInput.value, 10) || min;
        DOM.retryInput.value = Math.min(max, current + step);
      });
      DOM.retryInput.addEventListener('change', () => {
        const cleaned = DOM.retryInput.value.replace(/\D/g, '');
        const value = parseInt(cleaned, 10);
        if (Number.isNaN(value) || cleaned === '') {
          DOM.retryInput.value = '';
        } else if (value < min) {
          DOM.retryInput.value = min;
        } else if (value > max) {
          DOM.retryInput.value = max;
        } else {
          DOM.retryInput.value = value;
        }
      });
    }
  }
  async function injectVersion() {
    const pill = document.querySelector('.navbar-context-label');
    const footer = document.getElementById('app-version');
    let server;
    try {
      const resp = await fetch(window.location.pathname, { method: 'HEAD' });
      if (!resp.ok) Logger.debug(`[Version] HEAD request returned ${resp.status}, continuing.`);
      server = resp.headers.get('Server');
    } catch (err) {
      Logger.debug('[Version] Could not discover server version.', err);
    }
    if (server) {
      if (pill) pill.dataset.tooltip = server;
      if (footer) footer.textContent = server;
    }
    // Ensure the UI always shows something, even if the fetch failed or returned no header.
    if (footer && !footer.textContent) footer.textContent = 'Mercure';
  }
  async function init() {
    Logger.debug('[Init] Application starting.');
    try {
      initializeEventListeners();
      injectVersion(); // Fire-and-forget; version display is non-critical.
      await setDefaultValues();
      updateJwtPayloadDisplay();
      updateAlgorithmSections();

      // Clear any stale auth cookie from a previous session.
      // The cookie is HttpOnly so JS can't detect it, but it persists across reloads
      // and would silently authenticate requests even though the UI defaults to Header auth.
      try {
        const topicUrl = DOM.discoverForm.topic.value;
        if (topicUrl) {
          const resp = await fetch(topicUrl, { credentials: 'include' });
          if (resp.ok) Logger.debug('[Init] Cleared stale auth cookie.');
          else Logger.debug('[Init] Stale cookie cleanup got non-OK response:', resp.status);
        }
      } catch (err) {
        Logger.debug('[Init] Could not clear stale auth cookie (non-critical).', err);
      }

      const loadingScreen = document.getElementById('loading-screen');
      if (loadingScreen) {
        loadingScreen.classList.add('is-hidden');
      } else {
        Logger.warn('[Init] Loading screen element not found.');
      }
      Logger.info('[Init] Application ready.');
    } catch (err) {
      Logger.error('[Init] Application failed to initialize.', err);
      showNotification(
        `Application failed to load: ${formatErrorMessage(err)}. Please refresh the page.`,
        'error',
        10000,
      );
      const loadingScreen = document.getElementById('loading-screen');
      if (loadingScreen) loadingScreen.classList.add('is-hidden');
    }
  }

  document.addEventListener('DOMContentLoaded', init);
})();
