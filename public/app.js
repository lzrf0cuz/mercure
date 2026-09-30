import { createParser } from './vendor/eventsource-parser.js';
import hljs from './vendor/highlight.js';

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

// Reconnection policy of streamEvents: retry n waits 1s * 2^(n-1), up to 30s, except that the
// first retry waits the hub's retry: value when it sent one, and a 429 waits at least its
// Retry-After seconds.
const SSE_RETRY_BASE_DELAY_MS = 1000;
const SSE_RETRY_MAX_DELAY_MS = 30000;
// A stream open this long is stable: the retry count resets, and when the hub ends it (its
// write_timeout) the stream reopens at once instead of backing off.
const SSE_STABLE_CONNECTION_MS = 10000;
// A stream the hub ends cleanly after this long (a short write_timeout) reopens after the base
// delay without counting as a retry; a shorter one is a counted retry.
const SSE_SHORT_STREAM_MS = 1000;
const MAX_TIMER_MS = 2 ** 31 - 1;

function clampDelay(ms) {
  return Number.isNaN(ms) ? 0 : Math.min(Math.max(ms, 0), MAX_TIMER_MS);
}

// Retry-After in delay-seconds, in milliseconds; 0 when absent or an HTTP date.
function retryAfterMs(response) {
  const value = response.headers.get('Retry-After')?.trim() ?? '';
  return /^\d+$/.test(value) ? Number(value) * 1000 : 0;
}

function cancelBody(response) {
  if (response.body && !response.bodyUsed) response.body.cancel().catch(() => {});
}

// Reads a Server-Sent Events stream with fetch, so requests can carry an Authorization header,
// and reconnects until options.signal aborts or the hub answers with a status that is not
// retried (2xx text/event-stream opens; 429, 5xx and a 2xx of another type are retried).
// Callbacks, none of which runs after the signal aborts:
// - onStatus('connecting') before each request; onStatus('retrying', { delay, attempt, cause,
//   opened, resumed }) before each wait, where attempt 0 is an uncounted reopen of a stream the
//   hub ended (at once when it was stable, after the base delay when it lived at least
//   SSE_SHORT_STREAM_MS), opened tells whether the attempt had opened its stream, and resumed
//   marks the rest of a wait interrupted while the page was hidden;
//   onStatus('paused') when the page is hidden and openWhenHidden is off.
// - onOpen(response) once the stream is accepted, awaited before its events are read.
// - onMessage({ id, event, data }) for each event with data.
// - onFatal(response, error) for a status that is not retried, or with no response and an error
//   when the request cannot be built (a header value a header cannot carry); the stream ends
//   after it.
// The cursor travels as the last_event_id query parameter, not as a Last-Event-ID header, whose
// value fetch would trim: the last id received replaces the one in url, and an empty id removes
// it. With trackLastEventId off, no attempt sends either.
// The returned promise resolves when the stream ends.
function streamEvents(url, options) {
  const {
    signal,
    headers,
    credentials,
    trackLastEventId = true,
    openWhenHidden = false,
    onStatus = () => {},
    onOpen = () => {},
    onMessage = () => {},
    onFatal = () => {},
  } = options;

  return new Promise((resolve, reject) => {
    let done = false;
    let attempt = null; // AbortController of the open request or stream
    let lastEventId; // undefined until the stream sends an id
    let serverRetry = null;
    let failures = 0;
    let timer = null;
    let waiting = false; // a wait is pending, running or held while the page is hidden
    let waitMs = 0;
    let dueAt = 0;

    const live = (ctrl) => !done && !ctrl.signal.aborted;

    function teardown() {
      done = true;
      clearTimeout(timer);
      attempt?.abort();
      signal.removeEventListener('abort', finish);
      document.removeEventListener('visibilitychange', onVisibility);
    }

    function finish() {
      if (done) return;
      teardown();
      resolve();
    }

    function run() {
      connect().catch((err) => {
        if (done) return;
        teardown();
        reject(err);
      });
    }

    function fire() {
      timer = null;
      waiting = false;
      run();
    }

    function wait(delay, detail) {
      waiting = true;
      waitMs = clampDelay(delay);
      dueAt = performance.now() + waitMs;
      onStatus('retrying', { delay: waitMs, ...detail });
      if (!done) timer = setTimeout(fire, waitMs);
    }

    // opened: whether the attempt that failed had opened its stream (lost) or not (failed).
    function retry(cause, minDelay = 0, opened = true) {
      failures++;
      const delay =
        failures === 1 && serverRetry !== null
          ? serverRetry
          : Math.min(SSE_RETRY_BASE_DELAY_MS * 2 ** (failures - 1), SSE_RETRY_MAX_DELAY_MS);
      wait(Math.max(delay, minDelay), { attempt: failures, cause, opened });
    }

    // Hiding the page closes the stream or holds the pending wait; showing it reopens the stream
    // once the wait is over.
    function onVisibility() {
      if (document.hidden) {
        attempt?.abort();
        attempt = null;
        clearTimeout(timer);
        timer = null;
        onStatus('paused');
        return;
      }
      if (attempt || timer) return;
      const remaining = waiting ? Math.min(Math.max(dueAt - performance.now(), 0), waitMs) : 0;
      if (remaining === 0) {
        waiting = false;
        run();
        return;
      }
      onStatus('retrying', { delay: remaining, attempt: failures, resumed: true });
      if (!done) timer = setTimeout(fire, remaining);
    }

    async function fail(response, error) {
      // A later hide and show must not reopen a stream the hub refused.
      document.removeEventListener('visibilitychange', onVisibility);
      try {
        await onFatal(response, error);
      } catch (err) {
        Logger.error('[SSE] Fatal response handler failed.', err);
      }
      if (response) cancelBody(response);
      finish();
    }

    // A request that cannot be built fails the same way on every attempt: not retried.
    function buildRequest(attemptSignal) {
      const requestUrl = new URL(url);
      let requestHeaders;
      try {
        requestHeaders = new Headers(headers);
      } catch (err) {
        throw new Error(
          'Request not sent: a request header value (JWT or extra header) contains characters a header cannot carry',
          { cause: err },
        );
      }
      if (!requestHeaders.has('Accept')) requestHeaders.set('Accept', 'text/event-stream');
      if (!trackLastEventId) {
        requestHeaders.delete('Last-Event-ID');
        requestUrl.searchParams.delete('last_event_id');
      } else if (lastEventId) {
        requestUrl.searchParams.set('last_event_id', lastEventId);
      } else if (lastEventId === '') {
        requestUrl.searchParams.delete('last_event_id');
      }
      try {
        return new Request(requestUrl, {
          headers: requestHeaders,
          credentials,
          signal: attemptSignal,
        });
      } catch (err) {
        // The browser's message may quote the URL; drop its user name and password.
        const message = err.message.replace(/\/\/[^/?#\s]*@/g, '//');
        throw new Error(`Request not sent: ${message}`, { cause: err });
      }
    }

    async function connect() {
      const ctrl = new AbortController();
      attempt = ctrl;
      onStatus('connecting');
      if (!live(ctrl)) return;

      let request;
      try {
        request = buildRequest(ctrl.signal);
      } catch (err) {
        await fail(undefined, err);
        return;
      }
      let response;
      try {
        response = await fetch(request);
      } catch (err) {
        if (!live(ctrl)) return;
        attempt = null;
        retry(err, 0, false);
        return;
      }
      if (!live(ctrl)) {
        cancelBody(response);
        return;
      }

      const type = response.headers.get('Content-Type')?.toLowerCase() ?? '';
      if (!response.ok || !type.startsWith('text/event-stream') || !response.body) {
        const { status } = response;
        if (!response.ok && status !== 429 && status < 500) {
          await fail(response);
          return;
        }
        attempt = null;
        cancelBody(response);
        retry(
          new Error(`${status} response${response.ok ? ` of type "${type}"` : ''}`),
          status === 429 ? retryAfterMs(response) : 0,
          false,
        );
        return;
      }

      try {
        await onOpen(response);
      } catch (err) {
        Logger.error('[SSE] Open handler failed.', err);
      }
      // The stream's lifetime counts from here: a slow onOpen is not time spent connected.
      const readingSince = performance.now();

      // A new parser per stream: a partial event left by a dropped stream is discarded, and the
      // id of an event counts only once its block is complete.
      const parser = createParser({
        onId(id) {
          if (trackLastEventId && live(ctrl)) lastEventId = id;
        },
        onRetry(ms) {
          if (live(ctrl)) serverRetry = clampDelay(ms);
        },
        onEvent(message) {
          if (!live(ctrl)) return;
          try {
            onMessage(message);
          } catch (err) {
            Logger.error('[SSE] Event handler failed.', err);
          }
        },
      });
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let cause;
      try {
        while (live(ctrl)) {
          const { done: ended, value } = await reader.read();
          if (ended) break;
          if (live(ctrl)) parser.feed(decoder.decode(value, { stream: true }));
        }
      } catch (err) {
        cause = err;
      }
      // A stream that lived the stable time resets the retry count however it ended, including
      // when hiding the page closed it.
      const lived = performance.now() - readingSince;
      const stable = lived >= SSE_STABLE_CONNECTION_MS;
      if (stable) failures = 0;
      if (!live(ctrl)) return;

      attempt = null;
      const ended = new Error('stream ended by the hub');
      if (stable && cause === undefined) {
        wait(0, { attempt: 0, cause: ended, opened: true });
      } else if (lived >= SSE_SHORT_STREAM_MS && cause === undefined) {
        // The hub delivered for a while, so it is healthy: start a fresh retry cycle.
        failures = 0;
        wait(SSE_RETRY_BASE_DELAY_MS, { attempt: 0, cause: ended, opened: true });
      } else {
        retry(cause ?? ended);
      }
    }

    if (signal.aborted) {
      resolve();
      return;
    }
    signal.addEventListener('abort', finish);
    if (openWhenHidden) {
      run();
      return;
    }
    document.addEventListener('visibilitychange', onVisibility);
    if (document.hidden) {
      onStatus('paused');
    } else {
      run();
    }
  });
}
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

    const baseUrl = './vendor/highlight/';
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
    UI_HS256_SECRET: '!ChangeThisMercureHubJWTSecretKey!',
    UI_DEFAULT_HUB_URL: defaultHubUrl,
    UI_MAX_EVENTS: 100,
    // Controls whether the subscription monitoring SSE stays open when the tab is hidden
    PRESENCE_OPEN_WHEN_HIDDEN: true, // Stay alive to track all state changes
  };

  let runtimeConfig = { playground: false, anonymous: false, subscriptions: false, cookieName: '' };
  // True once the hub has put a token in the JWT field (playground-token answered 200).
  let playgroundTokenMinted = false;
  let configFailed = false;

  // Example topic and URLPattern. The playground serves /playground/ resources; other modes
  // get a neutral same-origin example, since topics are only strings.
  function exampleTopicPath() {
    return runtimeConfig.playground ? '/playground/books/1.jsonld' : '/books/1';
  }
  function exampleTopicPattern() {
    return runtimeConfig.playground ? '/playground/books/:id.jsonld' : '/books/:id';
  }

  async function loadRuntimeConfig() {
    try {
      const response = await fetch('./config.json');
      if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
      runtimeConfig = await response.json();
    } catch (err) {
      configFailed = true;
      Logger.warn('[Init] Could not load hub settings; playground features disabled.', err);
    }
    document.getElementById('cookie-name').textContent = runtimeConfig.cookieName;
    DOM.clearCookieButton.dataset.tooltip = `Clear ${runtimeConfig.cookieName} cookie\n(triggers Discover without JWT)`;
    document.getElementById('discovery-cookie-name').textContent = runtimeConfig.cookieName;
    document.getElementById('playground-tools').hidden = !runtimeConfig.playground;
    // Discover and cookie auth need the playground endpoint; without it they 404,
    // and Discover would put a pasted token in the access log as ?jwt=.
    for (const id of ['discover', 'discover-hint', 'cookie-auth', 'cookie-hint']) {
      document.getElementById(id).classList.toggle('is-hidden', !runtimeConfig.playground);
    }
    for (const button of [
      DOM.copyPublicJwkButton,
      DOM.copyPrivateJwkButton,
      DOM.copyJwksButton,
      DOM.copyPublicPemButton,
      DOM.copyHS256SecretButton,
      DOM.copyHS256SecretVerifyButton,
      DOM.copyHS256ConfigButton,
    ]) {
      button.disabled = !runtimeConfig.playground;
    }
    DOM.subscribeForm.elements.anonymous.disabled = !runtimeConfig.anonymous;
    DOM.subscriptionsForm.elements.subscribe.disabled = !runtimeConfig.subscriptions;
    document.getElementById('subscriptions-hint').textContent = subscriptionsHintText();
    // Playground is the only mode with a banner; the token message lives in the JWT help.
    const modeHint = document.getElementById('mode-hint');
    modeHint.textContent = runtimeConfig.playground
      ? 'Playground: insecure test mode, for development only.'
      : '';
    modeHint.classList.toggle('is-hidden', !runtimeConfig.playground);
  }

  function subscriptionsHintText() {
    if (configFailed)
      return 'Hub settings could not be loaded, so the subscriptions API is off here.';
    return runtimeConfig.subscriptions
      ? ''
      : 'The subscriptions API is disabled in this hub configuration.';
  }

  // The single token message, shown in the JWT field's help. It states what this page observed:
  // the hub settings, and whether the hub put a token in the field at load.
  function renderTokenHelp() {
    const command = document.createElement('code');
    command.textContent = 'caddy mercure-token';
    const mintHint = [' Mint one with ', command, '.'];
    let parts;
    if (configFailed) {
      parts = ['The hub settings (config.json) could not be loaded: paste a token.'];
    } else if (runtimeConfig.playground && playgroundTokenMinted) {
      parts = ['The hub filled in an all-access token on load.'];
    } else if (runtimeConfig.playground) {
      parts = ['This hub did not provide a token: paste one.', ...mintHint];
    } else {
      parts = [
        runtimeConfig.anonymous
          ? 'Paste a token: optional to subscribe anonymously, required to publish or to subscribe to private topics.'
          : 'Paste a token: required to subscribe and to publish.',
        ...mintHint,
      ];
    }
    document.getElementById('jwt-token-message').replaceChildren(...parts, ' ');
  }

  async function loadPlaygroundToken() {
    if (!runtimeConfig.playground) return;
    try {
      const response = await fetch('./playground-token');
      if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
      DOM.settingsForm.jwt.value = (await response.text()).trim();
      playgroundTokenMinted = true;
    } catch (err) {
      // No token route: the key is not a static HMAC key, or the embedder set no token callback.
      Logger.debug('[Init] The hub did not provide a playground token.', err);
    }
  }

  // The key family the token in the JWT field is signed with: 'hs' (HMAC) or 'rs' (RSA: RS*, PS*).
  // Empty for no token, an undecodable header or any other algorithm.
  function tokenKeyFamily() {
    try {
      const header = JSON.parse(decodeBase64Url(DOM.settingsForm.jwt.value.split('.')[0]));
      const algorithm = String(header.alg ?? '').toLowerCase();
      if (algorithm.startsWith('hs')) return 'hs';
      if (algorithm.startsWith('rs') || algorithm.startsWith('ps')) return 'rs';
    } catch {
      // Not a decodable token: no family.
    }
    return '';
  }

  // Subscribe with a token when there is one; anonymous only when there is none. The default is
  // applied again only when the JWT field changes between empty and non-empty, so a choice the
  // user made by hand survives edits to the token.
  let jwtWasEmpty = true;
  function applyAnonymousDefault() {
    jwtWasEmpty = !DOM.settingsForm.jwt.value;
    DOM.subscribeForm.elements.anonymous.checked = runtimeConfig.anonymous && jwtWasEmpty;
    syncAnonymousHelp();
  }

  // Neutral help while Anonymous is off; the warning only while it is on.
  function syncAnonymousHelp() {
    const checked = DOM.subscribeForm.elements.anonymous.checked;
    document.getElementById('anonymous-help').classList.toggle('is-hidden', checked);
    document.getElementById('anonymous-warning').classList.toggle('is-hidden', !checked);
  }

  // The CLI example uses this page's own topics, so the token covers the default topics. --dev
  // fixes aud at https://localhost/.well-known/mercure; other origins pass their own.
  function renderCreateTokenCommands() {
    const origin = window.location.origin;
    const hubUrl = CONFIG.UI_DEFAULT_HUB_URL;
    const aud = hubUrl === 'https://localhost/.well-known/mercure' ? [] : [`--aud ${hubUrl}`];
    for (const code of document.querySelectorAll('.create-token-command')) {
      const rs256 = code.dataset.alg === 'RS256';
      const args = [
        rs256 ? '--dev --alg RS256' : '--dev',
        ...(rs256 ? ['--key @fixtures/jwt/RS256.key'] : []),
        ...aud,
        `--publish '${origin}${exampleTopicPath()}'`,
        `--subscribe-urlpattern '${origin}${exampleTopicPattern()}'`,
        ...(rs256 ? [] : [`--payload '{"user": "alice"}'`]),
      ];
      code.textContent = `caddy mercure-token ${args.join(' \\\n  ')}`;
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
  // renderSubscriptionCard touches `s.match.length` etc. Validate at both
  // entry points. Modern subscriptions expose match/match_type; compatibility
  // builds also produce deprecated ones that expose topic.
  function isValidSubscription(s) {
    return (
      s != null &&
      typeof s === 'object' &&
      typeof s.id === 'string' &&
      (s.match !== undefined
        ? typeof s.match === 'string' && typeof s.match_type === 'string'
        : typeof s.topic === 'string') &&
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
    hubKeyTools: document.getElementById('hub-key-tools'),
    hubKeyHS256Button: document.getElementById('hub-key-hs256'),
    hubKeyRS256Button: document.getElementById('hub-key-rs256'),
    debugTokenRS256: document.getElementById('debug-token-rs256'),
    createTokenRS256: document.getElementById('create-token-rs256'),
    hubConfigRS256: document.getElementById('hub-config-rs256'),
    debugTokenHS256: document.getElementById('debug-token-hs256'),
    createTokenHS256: document.getElementById('create-token-hs256'),
    createTokenCustomRS256: document.getElementById('create-token-custom-rs256'),
    createTokenCustomHS256: document.getElementById('create-token-custom-hs256'),
    hubConfigHS256: document.getElementById('hub-config-hs256'),
    copyHS256SecretButton: document.getElementById('copy-hs256-secret'),
    copyHS256SecretVerifyButton: document.getElementById('copy-hs256-secret-verify'),
    copyHS256ConfigButton: document.getElementById('copy-hs256-config'),
    hs256ConfigPreview: document.getElementById('hs256-config-preview'),
    subscribedTarget: document.getElementById('subscribed-target'),
    subscribedUrl: document.getElementById('subscribed-url'),
    updatesCapNote: document.getElementById('updates-cap-note'),
    extraHeaderSummaryName: document.getElementById('extra-header-summary-name'),
    subscriptionsHint: document.getElementById('subscriptions-hint'),
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

    const placeholderTopic = `${newOrigin}${exampleTopicPath()}`;

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

    DOM.subscribeTopicsExamples.textContent = `${placeholderTopic}\nURLPattern: ${newOrigin}${exampleTopicPattern()}`;

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

  // True while the main stream or the subscriptions monitor is open.
  function isStreamOpen() {
    return (
      !DOM.subscribeForm.elements.unsubscribe.disabled ||
      !DOM.subscriptionsForm.elements.unsubscribe.disabled
    );
  }

  function updateFormStates() {
    const disabled = isStreamOpen();

    DOM.settingsForm.hubUrl.disabled = disabled;
    DOM.settingsForm.jwt.disabled = disabled;
    DOM.settingsForm.extraHeaderName.disabled = disabled;
    DOM.settingsForm.extraHeaderValue.disabled = disabled;

    for (const radio of DOM.settingsForm.querySelectorAll('input[name="authorization"]')) {
      radio.disabled = disabled;
    }
    DOM.clearCookieButton.disabled = disabled || !hasCookie;

    DOM.discoverForm.topic.disabled = disabled;
    DOM.discoverForm.body.disabled = disabled;

    DOM.discoverForm.querySelector('button[name="discover"]').disabled = disabled;
  }

  // The key the Hub Key & New Tokens section shows; the token's family selects it, a click overrides it.
  let hubKey = 'hs';

  // Debug Token follows the token in the JWT field, and so does the Hub Key preselection. Run on
  // load and on every change of the field. The fixture keys and the dev secret are playground
  // material: without playground mode the copy buttons are off, so the steps are replaced by a
  // note saying why.
  function syncTokenTools() {
    const family = tokenKeyFamily();
    if (family) hubKey = family;
    const tools = runtimeConfig.playground;
    DOM.debugTokenHS256.style.display = tools && family === 'hs' ? 'block' : 'none';
    DOM.debugTokenRS256.style.display = tools && family === 'rs' ? 'block' : 'none';
    for (const note of document.querySelectorAll('.token-tools-note')) {
      note.classList.toggle('is-hidden', tools);
    }
    for (const intro of document.querySelectorAll('.token-tools-only')) {
      intro.classList.toggle('is-hidden', !(tools && playgroundTokenMinted));
    }
    DOM.hubKeyTools.style.display = tools ? 'block' : 'none';
    renderHubKey();
  }

  function renderHubKey() {
    const isHS = hubKey === 'hs';
    for (const [button, pressed] of [
      [DOM.hubKeyHS256Button, isHS],
      [DOM.hubKeyRS256Button, !isHS],
    ]) {
      button.setAttribute('aria-pressed', String(pressed));
      button.classList.toggle('is-primary', pressed);
      button.classList.toggle('is-selected', pressed);
    }
    for (const [rs256El, hs256El] of [
      [DOM.createTokenRS256, DOM.createTokenHS256],
      [DOM.createTokenCustomRS256, DOM.createTokenCustomHS256],
      [DOM.hubConfigRS256, DOM.hubConfigHS256],
    ]) {
      rs256El.style.display = isHS ? 'none' : 'block';
      hs256El.style.display = isHS ? 'block' : 'none';
    }
    DOM.hs256ConfigPreview.textContent = `issuer https://localhost {\n  publisher {\n    jwt ${CONFIG.UI_HS256_SECRET} HS256\n  }\n  subscriber {\n    jwt ${CONFIG.UI_HS256_SECRET} HS256\n  }\n}`;
  }
  // The indicator's label and class, and the text shown while no event is listed. Events already
  // listed stay visible through a reconnect; the indicator then carries the state.
  const LIVE_UPDATES_STATES = {
    idle: {
      label: 'Not subscribed',
      text: ['Not subscribed', 'Click Subscribe to receive events'],
    },
    connecting: {
      label: 'Connecting',
      className: 'is-pending',
      text: ['Connecting...', 'Establishing SSE connection'],
    },
    reconnecting: {
      label: 'Reconnecting',
      className: 'is-pending',
      text: ['Reconnecting...', 'Connection lost, retrying'],
    },
    // A routine reopen of a stream the hub ended (write_timeout): nothing was lost.
    reopening: {
      label: 'Reconnecting',
      className: 'is-pending',
      text: ['Reconnecting...', 'The hub closed the stream; reopening it'],
    },
    paused: {
      label: 'Paused while the tab is hidden',
      text: ['Paused', 'The tab is hidden; the stream reopens when it is shown'],
    },
    waiting: {
      label: 'Connected',
      className: 'is-active',
      text: ['Waiting for events', 'Connected, listening for updates'],
    },
    active: { label: 'Connected', className: 'is-active' },
  };
  function setLiveUpdatesState(state) {
    const { label, className, text } = LIVE_UPDATES_STATES[state];
    const indicator = DOM.liveIndicator;
    const empty = DOM.updatesEmpty;
    const list = DOM.updatesList;

    indicator.classList.remove('is-active', 'is-pending');
    if (className) indicator.classList.add(className);
    indicator.title = label;
    indicator.setAttribute('aria-label', label);

    if (state === 'idle') {
      DOM.subscribedTarget.classList.add('is-hidden');
      DOM.retryValue.textContent = `${SSE_RETRY_BASE_DELAY_MS}ms`;
    }
    if (text && list.children.length === 0) {
      empty.style.display = 'flex';
      empty.querySelector('p').textContent = text[0];
      empty.querySelector('span').textContent = text[1];
    } else {
      empty.style.display = 'none';
    }
  }
  // Events stay listed after the stream stops on an error; Unsubscribe and Subscribe clear them.
  function clearLiveUpdates() {
    DOM.updatesList.replaceChildren();
    eventCounter = 0;
    updateEventCount();
    DOM.updatesCapNote.classList.add('is-hidden');
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

    DOM.subscriptionCountClient.textContent = `${clientCount} client subscription${clientCount !== 1 ? 's' : ''}`;
    DOM.subscriptionCountClient.dataset.count = clientCount.toString();

    DOM.subscriptionCountMonitor.textContent = `${monitorCount} monitor subscription${monitorCount !== 1 ? 's' : ''}`;
    DOM.subscriptionCountMonitor.dataset.count = monitorCount.toString();

    DOM.subscriptionsEmpty.style.display = total === 0 ? 'flex' : 'none';
  }
  function updateCookieButtonState(cookieSet) {
    hasCookie = cookieSet;
    updateFormStates();
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
    // An error inside a closed collapsible (the extra header) would otherwise stay hidden.
    const section = input.closest('details');
    if (section) section.open = true;

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
  // RFC 9110 section 5.6.2 token.
  const HEADER_NAME_TOKEN = /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/;
  // RFC 9110 section 5.5 field-value without obs-text: visible ASCII, space and tab.
  const HEADER_VALUE_ASCII = /^[\t\x20-\x7E]+$/;
  const EXTRA_HEADER_PAIR_ERROR = 'Set both the header name and value, or neither';
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
    } else if (input.name === 'extraHeaderName') {
      const name = input.value.trim();
      const value = DOM.settingsForm.extraHeaderValue.value.trim();
      if (name && !HEADER_NAME_TOKEN.test(name)) {
        showValidationError(input, 'Header name must be an RFC 9110 token (no spaces or colons)');
        return false;
      }
      if (name.toLowerCase() === 'authorization') {
        showValidationError(input, 'Authorization is set from the JWT field');
        return false;
      }
      if (name.toLowerCase() === 'last-event-id') {
        showValidationError(input, 'Last-Event-ID is set from the Last Event ID field');
        return false;
      }
      if (!name && value) {
        showValidationError(input, EXTRA_HEADER_PAIR_ERROR);
        return false;
      }
    } else if (input.name === 'extraHeaderValue') {
      const value = input.value.trim();
      const name = DOM.settingsForm.extraHeaderName.value.trim();
      if (value && !HEADER_VALUE_ASCII.test(value)) {
        showValidationError(input, 'Only ASCII header values are supported');
        return false;
      }
      if (!value && name) {
        showValidationError(input, EXTRA_HEADER_PAIR_ERROR);
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

    if (options.includeExtraHeader) {
      // Both fields are validated (no short-circuit) so each shows its own error.
      const nameValid = validateInput(DOM.settingsForm.extraHeaderName);
      const valueValid = validateInput(DOM.settingsForm.extraHeaderValue);
      if (!nameValid || !valueValid) {
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
      throw new Error('The topic response has no Link header with rel="mercure".');
    }

    return links;
  }
  // Logs, toasts and fatal handling shared by the main stream and the subscriptions monitor.
  function createStreamCallbacks(
    signal,
    { onFatal = () => {}, context = 'event stream', target, hubRequest = true } = {},
  ) {
    function fail(err) {
      Logger.error(`[SSE] Fatal error on ${context}, halting retries.`, err);
      showNotification(`Failed to subscribe to ${context}: ${formatErrorMessage(err)}.`, 'error');
      onFatal();
    }

    return {
      fail,

      onOpen() {
        Logger.info(`[SSE] Connected to ${context}.`);
        showNotification(`Connected to ${context}.`, 'success', 3000);
      },

      onRetry({ delay, attempt, cause, opened, resumed }) {
        if (resumed) {
          Logger.debug(`[SSE] Tab shown, retry of ${context} in ${Math.round(delay)}ms.`);
          return;
        }
        if (attempt === 0) {
          Logger.info(`[SSE] Hub ended ${context}, reconnecting in ${Math.round(delay)}ms.`);
          return;
        }
        // An attempt that never opened its stream failed; one that had opened was lost.
        const outcome = opened ? 'lost' : 'failed';
        Logger.warn(
          `[SSE] Connection to ${context} ${outcome}, retry #${attempt} in ${Math.round(delay)}ms.`,
          cause,
        );
        const hint = crossOriginHint(target);
        showNotification(
          `Connection to ${context} ${outcome}${hint ? `: ${hint}` : ''}. Retrying (attempt ${attempt})...`,
          'info',
          hint ? 5000 : 2000,
        );
      },

      async onFatal(response, error) {
        const message = response ? await describeFailure(response, { hubRequest }) : error.message;
        if (!signal.aborted) fail(new FatalError(message));
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
    // Optional extra header for hubs with a claim-header binding (require_claim_header).
    const extraName = DOM.settingsForm.extraHeaderName.value.trim();
    const extraValue = DOM.settingsForm.extraHeaderValue.value.trim();
    if (extraName && extraValue) {
      options.headers = { ...options.headers, [extraName]: extraValue };
    }
    return options;
  }
  function isCrossOrigin(target) {
    try {
      return new URL(target, window.location.href).origin !== window.location.origin;
    } catch {
      return false;
    }
  }
  // The hub serves this page with Content-Security-Policy: default-src 'self', so the browser
  // refuses every request to another origin and reports it only as "Failed to fetch".
  function crossOriginHint(target) {
    return isCrossOrigin(target)
      ? `blocked by this page's Content-Security-Policy, which only allows requests to ${window.location.origin}, or unreachable`
      : '';
  }
  // target: the URL the failed request went to, when known. Trailing periods are dropped
  // because callers add their own.
  function formatErrorMessage(err, target) {
    const msg = (err.message?.trim() || 'Unknown error').replace(/\.+$/, '');
    if (!msg.toLowerCase().includes('failed to fetch')) return msg;
    return crossOriginHint(target) || 'Could not reach hub (network error or CORS)';
  }
  // The error and error_description parameters of a WWW-Authenticate challenge (RFC 6750).
  function wwwAuthenticateReason(header) {
    if (!header) return '';
    const reasons = [];
    for (const [, name, value] of header.matchAll(/\b(error|error_description)="([^"]*)"/g)) {
      reasons.push(`${name}=${value}`);
    }
    return reasons.join(', ');
  }
  // HTTP/2 carries no reason phrase, so resp.statusText is empty there and a hub's bare body
  // ("Bad Request") would read as an explanation.
  const REASON_PHRASES = {
    400: 'Bad Request',
    401: 'Unauthorized',
    403: 'Forbidden',
    404: 'Not Found',
    413: 'Request Entity Too Large',
    429: 'Too Many Requests',
    500: 'Internal Server Error',
    503: 'Service Unavailable',
  };
  // Describes a failed hub response for an error toast: the status, the challenge's reason and
  // the (bounded) body text, which is where the hub explains a rejected request.
  // hubRequest: false for requests that never carry the extra request header (Discover, cookie
  // clear, anonymous subscribe). The extra-header hint is only for a bare 400: when the body
  // explains the rejection, the hint would misdirect.
  async function describeFailure(resp, { hubRequest = true } = {}) {
    const statusText = resp.statusText || REASON_PHRASES[resp.status] || '';
    const parts = [`${resp.status}${statusText ? ` ${statusText}` : ''}`];
    const reason = wwwAuthenticateReason(resp.headers.get('WWW-Authenticate'));
    if (reason) parts.push(reason);
    let body = '';
    try {
      body = (await resp.text()).trim();
    } catch (err) {
      Logger.debug('[Http] Could not read the error response body.', err);
    }
    const bodyShown = Boolean(body) && body.toLowerCase() !== statusText.toLowerCase();
    if (bodyShown) {
      parts.push(truncateWithEllipsis(body, 200));
    }
    let message = parts.join(': ');
    if (resp.status === 401) {
      message +=
        ". Check the token: header typ at+jwt, iss trusted by the hub, aud equal to the hub's resource identifier, exp in the future, signed with the issuer's key";
    } else if (
      resp.status === 400 &&
      hubRequest &&
      !reason &&
      !bodyShown &&
      !(
        DOM.settingsForm.extraHeaderName.value.trim() &&
        DOM.settingsForm.extraHeaderValue.value.trim()
      )
    ) {
      message +=
        '. If the hub binds a token claim to a request header, set it under Settings > Advanced Options > Extra Request Header.';
    }
    return message;
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
  // The URL for display: percent-encoding undone and userinfo left out. % & # = stay encoded
  // inside a component, so the text reads back unambiguously. The raw href when a sequence does
  // not decode.
  function readableUrl(u) {
    const bare = new URL(u.href);
    bare.username = '';
    bare.password = '';
    try {
      const part = (x) => x.replace(/[%&#=]/g, encodeURIComponent);
      const query = [...bare.searchParams].map(([k, v]) => `${part(k)}=${part(v)}`).join('&');
      const path = decodeURI(bare.pathname).replaceAll('%', '%25');
      const hash = decodeURI(bare.hash).replaceAll('%', '%25');
      return `${bare.protocol}//${bare.host}${path}${query ? `?${query}` : ''}${hash}`;
    } catch (err) {
      Logger.debug('[SSE] Could not decode the subscribed URL for display.', err);
      return bare.href;
    }
  }
  async function handleDiscoverSubmit(e) {
    e.preventDefault();
    // The Cookie radio click disables itself before submitting; every early return puts the
    // form state back.
    if (!validateForm(e.target, { includeHubUrl: false })) {
      updateFormStates();
      return;
    }
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
      updateFormStates();
      return;
    }
    // The token rides in the query string, and the page's CSP refuses another origin while the
    // browser still logs the failed URL: stop before the token is added.
    if (isCrossOrigin(url)) {
      showNotification(`Discovery failed: ${crossOriginHint(url)}.`, 'error');
      Logger.warn('[Discovery] Cross-origin topic URL; not fetched.', { origin: url.origin });
      updateFormStates();
      return;
    }
    if (body.value) url.searchParams.append('body', body.value);
    // Playground handler reads ?jwt= to set the auth cookie (playground.go). This is distinct from
    // the Mercure spec's ?authorization= param and RFC 6750's ?access_token= param.
    if (jwt) {
      url.searchParams.append('jwt', jwt);
    } else {
      Logger.warn('[Discovery] No JWT provided - auth cookie will be cleared.');
    }

    // The query string carries the token (?jwt=), so it is never logged.
    Logger.info(`[Discovery] Fetching topic: ${url.origin}${url.pathname}`);
    try {
      const resp = await fetch(url, { credentials: 'include' });
      if (!resp.ok) throw new Error(await describeFailure(resp, { hubRequest: false }));

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
          origin: hubUrl.origin,
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
      if (jwt) {
        showNotification('Discovery successful.', 'success');
      } else {
        // One toast: a second one would replace the warning at once.
        showNotification('Discovery successful. No JWT provided: auth cookie cleared.', 'warning');
      }
      Logger.info(
        `[Discovery] Success. Hub: ${DOM.settingsForm.hubUrl.value}, Topic: ${cleanSelfUrl}`,
      );
      // The locked Settings must keep showing what an open stream uses, so a Discover that
      // finishes after Subscribe leaves the mode alone. A Discover that leaves no cookie must
      // not leave Cookie selected: that would send anonymous requests.
      if (!isStreamOpen()) DOM.settingsForm.authorization.value = jwt ? 'cookie' : 'header';

      // Track cookie state: cookie is set if JWT was provided, deleted if not
      updateCookieButtonState(!!jwt);
    } catch (err) {
      showNotification(`Discovery failed: ${formatErrorMessage(err, url)}.`, 'error');
      Logger.error('[Discovery] Failed.', err);
    } finally {
      // Re-enable the cookie radio (disabled during cookie-radio-triggered discovery) unless a
      // stream opened meanwhile and locked the settings.
      updateFormStates();
    }
  }
  // The fields that shape the open main stream are locked with it, so the form keeps showing
  // what the stream uses.
  function lockSubscribeForm(form) {
    for (const name of ['topics', 'matcherType', 'anonymous', 'lastEventId', 'openWhenHidden']) {
      form.elements[name].disabled = true;
    }
  }
  function unlockSubscribeForm(form) {
    for (const name of ['topics', 'matcherType', 'lastEventId', 'openWhenHidden']) {
      form.elements[name].disabled = false;
    }
    form.elements.anonymous.disabled = !runtimeConfig.anonymous;
  }
  function handleSubscribeSubmit(e) {
    e.preventDefault();
    const isAnonymous = e.target.elements.anonymous.checked;
    const needsJwt = !isAnonymous && DOM.settingsForm.authorization.value === 'header';
    if (
      !validateForm(e.target, {
        includeHubUrl: true,
        includeJwt: needsJwt,
        includeExtraHeader: !isAnonymous,
      })
    )
      return;
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
      u.searchParams.append(DOM.subscribeForm.elements.matcherType.value, topic);
    }
    if (lastEventId.value) {
      // set, not append: the field wins over a last_event_id already in the Hub URL.
      u.searchParams.set('last_event_id', lastEventId.value);
    }

    clearLiveUpdates();
    DOM.subscribedUrl.textContent = readableUrl(u);
    DOM.subscribedTarget.classList.remove('is-hidden');

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

    const controller = updateCtrl;
    const { signal } = controller;
    const stopped = () => {
      subscribe.disabled = false;
      unsubscribe.disabled = true;
      unlockSubscribeForm(e.target);
      setLiveUpdatesState('idle');
      updateFormStates();
    };
    const callbacks = createStreamCallbacks(signal, {
      context: 'main event stream',
      target: u,
      // An anonymous subscribe never sends the extra header, and the hub skips bindings for it.
      hubRequest: !isAnonymous,
      onFatal: stopped,
    });

    // The state shown while connecting: reconnecting after a counted retry, reopening after a
    // routine reopen (attempt 0), until the stream opens again.
    let connectingState = 'connecting';
    const options = {
      ...getAuthOptions({ anonymous: isAnonymous }),
      signal,
      onStatus(status, detail) {
        if (status === 'connecting') {
          setLiveUpdatesState(connectingState);
        } else if (status === 'paused') {
          setLiveUpdatesState('paused');
        } else if (status === 'retrying') {
          // The scheduled wait before the next attempt; a wait resumed after the tab was shown
          // keeps showing it.
          if (!detail.resumed) DOM.retryValue.textContent = `${Math.round(detail.delay)}ms`;
          callbacks.onRetry(detail);
          connectingState = detail.attempt > 0 ? 'reconnecting' : 'reopening';
          setLiveUpdatesState(connectingState);
        }
      },
      onOpen() {
        connectingState = 'connecting';
        callbacks.onOpen();
        setLiveUpdatesState('waiting');
      },
      onMessage(event) {
        Logger.debug('[SSE] Received event.', {
          id: event.id,
          type: event.event || '(default)',
          dataLength: event.data?.length ?? 0,
        });
        if (event.event === 'FatalError') {
          controller.abort();
          callbacks.fail(new FatalError(sanitizeFatalEventData(event.data)));
          return;
        }

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
            if (isJson && hljs) {
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
          while (list.children.length > CONFIG.UI_MAX_EVENTS) {
            list.removeChild(list.lastChild);
            DOM.updatesCapNote.classList.remove('is-hidden');
          }
        }
      },
      onFatal: callbacks.onFatal,
      openWhenHidden: e.target.elements.openWhenHidden.checked,
    };

    // Lock first: a request that cannot be built fails (and unlocks) before streamEvents returns.
    subscribe.disabled = true;
    unsubscribe.disabled = false;
    lockSubscribeForm(e.target);
    updateFormStates();

    streamEvents(u, options).catch((err) => {
      Logger.error('[SSE] Unexpected event stream failure.', err);
      showNotification('Unexpected error in event stream. Check console.', 'error');
      stopped();
    });
  }
  async function handlePublishSubmit(e) {
    e.preventDefault();
    const needsJwt = DOM.settingsForm.authorization.value === 'header';
    if (
      !validateForm(e.target, {
        includeHubUrl: true,
        includeJwt: needsJwt,
        includeExtraHeader: true,
      })
    )
      return;
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
      if (!resp.ok) throw new Error(await describeFailure(resp));
      const eventId = await resp.text();
      showNotification('Update published.', 'success');
      Logger.info(`[Publish] Success. Event ID: ${eventId.trim() || '(not returned)'}`);
    } catch (err) {
      showNotification(
        `Publish failed: ${formatErrorMessage(err, DOM.settingsForm.hubUrl.value)}.`,
        'error',
      );
      Logger.error('[Publish] Failed.', err);
    }
  }
  function renderSubscriptionCard(s) {
    const card = document.importNode(DOM.subscriptionTemplate.content, true);
    const article = card.querySelector('article');
    article.id = s.id;
    const modern = s.match !== undefined;
    const match = modern ? s.match : s.topic;
    const isMonitor = match.includes('/.well-known/mercure/subscriptions');

    const topicWrapper = article.querySelector('.subscription-topic-wrapper');
    const topicEl = article.querySelector('.subscription-topic');
    topicEl.textContent = `${modern ? s.match_type : 'topic'}: ${match}`;
    const truncatedTopic = truncateWithEllipsis(match, 60);
    topicWrapper.dataset.tooltip = `${truncatedTopic}\n\nClick to copy`;
    makeClickable(topicEl);
    topicEl.addEventListener('click', () => copyToClipboard(match, 'Topic'));

    const badge = article.querySelector('.subscription-badge');
    if (isMonitor) {
      badge.textContent = 'Monitor';
      badge.classList.add('is-monitor');
    } else {
      badge.textContent = 'Client';
      badge.classList.add('is-client');
    }

    const subWrapper = article.querySelector('.subscription-subscriber-wrapper');
    const subEl = article.querySelector('.subscription-subscriber');
    subEl.textContent = s.subscriber;
    const truncatedSub = truncateWithEllipsis(s.subscriber, 60);
    subWrapper.dataset.tooltip = `${truncatedSub}\n\nClick to copy`;
    makeClickable(subEl);
    subEl.addEventListener('click', () => copyToClipboard(s.subscriber, 'Subscriber'));

    // Payload is optional (the subscribe detail's payload in the token's authorization_details)
    const payloadDetails = article.querySelector('.subscription-payload-details');
    if (s.payload !== undefined) {
      const payloadCode = article.querySelector('.subscription-payload code');
      payloadCode.textContent = JSON.stringify(s.payload, null, 2);
      if (hljs) hljs.highlightElement(payloadCode);
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
  // During a hub restart the presence stream can reconnect before the main
  // stream, so a client card may disappear briefly; this is left unsmoothed.
  async function syncSubscriptionsSnapshot(subscriptionsUrl, fetchOptions) {
    const resp = await fetch(subscriptionsUrl, fetchOptions);
    if (!resp.ok) {
      throw Object.assign(new Error(await describeFailure(resp)), { status: resp.status });
    }
    const json = await resp.json();
    // The monitor may have been stopped or replaced while the body was read.
    if (fetchOptions.signal.aborted) return;
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
    const subscriptionsValid = [
      validateInput(DOM.settingsForm.hubUrl),
      !needsJwt || validateInput(DOM.settingsForm.jwt),
      validateInput(DOM.settingsForm.extraHeaderName),
      validateInput(DOM.settingsForm.extraHeaderValue),
    ].every(Boolean);
    if (!subscriptionsValid) return;
    if (subscriptionCtrl) subscriptionCtrl.abort();
    subscriptionCtrl = new AbortController();
    const controller = subscriptionCtrl;
    const {
      elements: { subscribe, unsubscribe },
    } = e.target;

    DOM.subscriptionsGrid.replaceChildren();
    updateSubscriptionCount();
    DOM.subscriptionsHint.textContent = subscriptionsHintText();
    showNotification('Connecting to active subscriptions stream...', 'info');

    const fetchOptions = {
      ...getAuthOptions(),
      signal: controller.signal,
    };
    // Build the subscriptions URL via the URL API so we don't double-slash
    // when the hub URL was entered with a trailing slash, and don't lose
    // the existing path when joining.
    const subscriptionsUrl = new URL(DOM.settingsForm.hubUrl.value);
    subscriptionsUrl.pathname = `${subscriptionsUrl.pathname.replace(/\/+$/, '')}/subscriptions`;

    const u = new URL(DOM.settingsForm.hubUrl.value);
    u.searchParams.append(
      'match_urlpattern',
      '/.well-known/mercure/subscriptions/:match_type/:match/:subscriber',
    );
    // No last_event_id on the SSE URL and no Last-Event-ID on reconnect: the snapshot in onOpen
    // is authoritative for state at (re)connect time, and skipping replay avoids the
    // race where historical events get applied on top of a fresher snapshot.

    const stopped = () => {
      subscribe.disabled = false;
      unsubscribe.disabled = true;
      updateFormStates();
    };
    const callbacks = createStreamCallbacks(controller.signal, {
      context: 'active subscriptions stream',
      target: u,
      onFatal: stopped,
    });

    const sseOptions = {
      ...fetchOptions,
      trackLastEventId: false,
      onStatus(status, detail) {
        if (status === 'retrying') callbacks.onRetry(detail);
      },
      async onOpen() {
        callbacks.onOpen();
        try {
          await syncSubscriptionsSnapshot(subscriptionsUrl, fetchOptions);
        } catch (err) {
          if (controller.signal.aborted || err.name === 'AbortError') return;
          if (err.status === 401 || err.status === 403) {
            // The stream itself is public, so it opens without credentials, but subscription
            // events are private: without a usable token the grid would stay empty and look
            // like "no subscribers". Stop and say what is missing.
            Logger.warn('[Presence] Not authorized to read subscriptions; stopping.', err);
            controller.abort();
            subscribe.disabled = !runtimeConfig.subscriptions;
            unsubscribe.disabled = true;
            updateFormStates();
            DOM.subscriptionsHint.textContent = `Not authorized to read subscriptions (${err.message}). Subscription events are private: the token needs a subscribe grant covering the /.well-known/mercure/subscriptions/ topics (and the cookie must be set when Cookie is selected).`;
            showNotification(`Active subscriptions stopped: ${formatErrorMessage(err)}.`, 'error');
            return;
          }
          Logger.warn(
            '[Presence] Snapshot fetch failed; live deltas will still apply but the grid may be stale.',
            err,
          );
          showNotification(
            `Could not refresh subscriptions snapshot: ${formatErrorMessage(err, subscriptionsUrl)}.`,
            'warning',
            4000,
          );
        }
      },
      onMessage(event) {
        Logger.debug('[Presence] Received event.', {
          id: event.id,
          dataLength: event.data?.length ?? 0,
        });
        if (event.event === 'FatalError') {
          controller.abort();
          callbacks.fail(new FatalError(sanitizeFatalEventData(event.data)));
          return;
        }

        // Subscription events require valid JSON data; empty strings are not valid JSON
        if (event.event === 'mercure' && event.data) {
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
              const hasPayload = s.payload !== undefined;
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
      onFatal: callbacks.onFatal,
      openWhenHidden: CONFIG.PRESENCE_OPEN_WHEN_HIDDEN,
    };

    // Lock first: a request that cannot be built fails (and unlocks) before streamEvents returns.
    subscribe.disabled = true;
    unsubscribe.disabled = false;
    updateFormStates();

    streamEvents(u, sseOptions).catch((err) => {
      Logger.error('[Presence] Unexpected event stream failure.', err);
      showNotification('Unexpected error in subscription stream. Check console.', 'error');
      stopped();
    });
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
      if (hljs) {
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
    document.getElementById('page-origin').textContent = window.location.origin;

    await loadRuntimeConfig();
    await loadPlaygroundToken();
    renderTokenHelp();
    applyAnonymousDefault();

    const exampleTopic = `${window.location.origin}${exampleTopicPath()}`;
    DOM.subscribeForm.topics.value = exampleTopic;
    DOM.publishForm.topics.value = exampleTopic;
    DOM.discoverForm.topic.value = exampleTopic;
    // Same-origin placeholders: the page's Content-Security-Policy blocks other origins.
    DOM.publishForm.topics.placeholder = exampleTopic;
    DOM.discoverForm.topic.placeholder = exampleTopic;
    DOM.discoverForm.body.value = runtimeConfig.playground
      ? JSON.stringify(
          {
            '@id': exampleTopic,
            availability: 'https://schema.org/InStock',
          },
          null,
          2,
        )
      : '';

    DOM.publishForm.data.value = JSON.stringify(
      {
        '@id': exampleTopic,
        availability: 'https://schema.org/OutOfStock',
      },
      null,
      2,
    );

    DOM.subscribeTopicsExamples.textContent = `${exampleTopic}\nURLPattern: ${window.location.origin}${exampleTopicPattern()}`;
    renderCreateTokenCommands();
  }
  function initializeEventListeners() {
    DOM.subscribeForm.elements.anonymous.addEventListener('change', syncAnonymousHelp);
    DOM.discoverForm.addEventListener('submit', handleDiscoverSubmit);
    DOM.subscribeForm.addEventListener('submit', handleSubscribeSubmit);
    DOM.publishForm.addEventListener('submit', handlePublishSubmit);
    DOM.subscriptionsForm.addEventListener('submit', handleSubscriptionsSubmit);
    DOM.settingsForm.jwt.addEventListener('input', () => {
      updateJwtPayloadDisplay();
      if (!DOM.settingsForm.jwt.value !== jwtWasEmpty) applyAnonymousDefault();
      syncTokenTools();
    });

    DOM.retryValue.textContent = `${SSE_RETRY_BASE_DELAY_MS}ms`;

    DOM.settingsForm.extraHeaderName.addEventListener('input', () => {
      const name = DOM.settingsForm.extraHeaderName.value.trim();
      DOM.extraHeaderSummaryName.textContent = name ? `: ${name}` : '';
    });

    for (const input of document.querySelectorAll('input, textarea')) {
      input.addEventListener('input', () => {
        clearValidationError(input);
        // The pair check reports on the other field, so editing either one clears both.
        if (
          input === DOM.settingsForm.extraHeaderName ||
          input === DOM.settingsForm.extraHeaderValue
        ) {
          clearValidationError(DOM.settingsForm.extraHeaderName);
          clearValidationError(DOM.settingsForm.extraHeaderValue);
        }
      });
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
        if (!resp.ok) throw new Error(await describeFailure(resp, { hubRequest: false }));
        // Cookie would now send no credentials at all, so go back to Header.
        if (!isStreamOpen()) DOM.settingsForm.authorization.value = 'header';
        updateCookieButtonState(false);
        showNotification('Auth cookie cleared.', 'success');
        Logger.info('[Auth] Cookie cleared successfully.');
      } catch (err) {
        showNotification(`Failed to clear cookie: ${formatErrorMessage(err, topicUrl)}.`, 'error');
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
      fetchAndCopy(
        CONFIG.UI_JWKS_PATH,
        "JWKS copied. Host it and point the issuer's publisher and subscriber jwks_uri at it.",
      ),
    );
    DOM.copyPublicPemButton.addEventListener('click', () =>
      fetchAndCopy(
        CONFIG.UI_PUBLIC_PEM_PATH,
        "PEM public key copied. Use it as the issuer's publisher and subscriber jwt key with RS256.",
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

    // The toggle selects this section's key; it never replaces the token.
    for (const [button, key] of [
      [DOM.hubKeyHS256Button, 'hs'],
      [DOM.hubKeyRS256Button, 'rs'],
    ]) {
      button.addEventListener('click', () => {
        hubKey = key;
        renderHubKey();
      });
    }

    DOM.copyHS256SecretButton.addEventListener('click', () =>
      copyAndNotify(
        CONFIG.UI_HS256_SECRET,
        "Default development secret copied. Paste in jwt.io's SIGN JWT: SECRET field.",
        {
          withJwtIo: true,
        },
      ),
    );
    DOM.copyHS256SecretVerifyButton.addEventListener('click', () =>
      copyAndNotify(
        CONFIG.UI_HS256_SECRET,
        "Default development secret copied. Paste in jwt.io's SECRET field.",
        {
          withJwtIo: true,
        },
      ),
    );
    DOM.copyHS256ConfigButton.addEventListener('click', () =>
      copyAndNotify(
        CONFIG.UI_HS256_SECRET,
        "Default development secret copied. Use it as the issuer's publisher and subscriber jwt key.",
      ),
    );

    DOM.subscribeForm.elements.unsubscribe.addEventListener('click', (e) => {
      e.preventDefault();
      if (updateCtrl) updateCtrl.abort();
      e.currentTarget.disabled = true;
      DOM.subscribeForm.elements.subscribe.disabled = false;
      unlockSubscribeForm(DOM.subscribeForm);
      clearLiveUpdates();
      setLiveUpdatesState('idle');
      updateFormStates();
      showNotification('Unsubscribed from main event stream.', 'info');
      Logger.info('[SSE] Unsubscribed from main event stream.');
    });

    DOM.subscriptionsForm.elements.unsubscribe.addEventListener('click', (e) => {
      e.preventDefault();
      if (subscriptionCtrl) subscriptionCtrl.abort();
      e.currentTarget.disabled = true;
      DOM.subscriptionsForm.elements.subscribe.disabled = !runtimeConfig.subscriptions;
      DOM.subscriptionsHint.textContent = subscriptionsHintText();
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
      syncTokenTools();

      // Clear any stale auth cookie from a previous session.
      // The cookie is HttpOnly so JS can't detect it, but it persists across reloads
      // and would silently authenticate requests even though the UI defaults to Header auth.
      try {
        const topicUrl = DOM.discoverForm.topic.value;
        if (runtimeConfig.playground && topicUrl) {
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
