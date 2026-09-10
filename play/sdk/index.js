/**
 * @dodopizza/game-sdk — вход игры в экосистему Game Guild.
 *
 * Правила, которые SDK берёт на себя:
 * - identity игрока — подписанные query-параметры запуска (контракт mapi), игра их не трогает;
 * - игра шлёт только события; счёт и награду считает game-connector;
 * - результат партии игрок видит из вердикта finish(), а не из подсчётов игры.
 */
import { sendGameLoaded } from './bridge.js';
export { sendGameLoaded };
export class GgSession {
    constructor(opts, sessionId, sessionToken, player, t0) {
        this.opts = opts;
        this.sessionId = sessionId;
        this.sessionToken = sessionToken;
        this.player = player;
        this.t0 = t0;
        this.queue = [];
        this.seq = 0;
        this.flushing = false;
        this.timer = setInterval(() => void this.flush(), this.opts.flushIntervalMs);
        // страница может закрыться до finish — дослать хвост буфера
        if (typeof addEventListener === 'function') {
            addEventListener('pagehide', () => void this.flush(true));
        }
    }
    /** Сообщает хосту gameLoaded и открывает сессию в коннекторе. */
    static async init(gameId, options) {
        sendGameLoaded();
        const launchParams = options.launchParams ?? (typeof location !== 'undefined' ? location.search : '');
        const f = options.fetchImpl ?? fetch;
        const response = await f(`${options.connectorUrl}/api/v1/games/${encodeURIComponent(gameId)}/sessions`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                launchParams,
                platform: detectPlatform(),
                gameVersion: options.gameVersion,
                countryId: options.countryId,
            }),
        });
        if (!response.ok) {
            throw new Error(`game-connector: start session failed with ${response.status}`);
        }
        const started = (await response.json());
        const player = parsePlayer(launchParams);
        return new GgSession({ batchSize: 20, flushIntervalMs: 3000, ...options }, started.sessionId, started.sessionToken, player, now());
    }
    /** Записать событие. Синхронно, в буфер; отправка батчами в фоне. */
    track(type, payload = {}) {
        this.queue.push({ seq: this.seq++, t: Math.round(now() - this.t0), type, payload });
        if (this.queue.length >= this.opts.batchSize) {
            void this.flush();
        }
    }
    /** Дослать события и получить вердикт сервера. Повторный вызов вернёт тот же вердикт. */
    async finish() {
        clearInterval(this.timer);
        for (let attempt = 0; attempt < 3 && (this.queue.length > 0 || attempt === 0); attempt++) {
            await this.flush();
        }
        const response = await this.fetch(`/api/v1/sessions/${this.sessionId}/finish`, '');
        if (!response.ok) {
            throw new Error(`game-connector: finish failed with ${response.status}`);
        }
        return (await response.json());
    }
    async flush(keepalive = false) {
        if (this.flushing || this.queue.length === 0) {
            return;
        }
        this.flushing = true;
        const batch = this.queue;
        this.queue = [];
        try {
            const response = await this.fetch(`/api/v1/sessions/${this.sessionId}/events`, JSON.stringify({ events: batch }), keepalive);
            if (!response.ok && response.status !== 409) {
                this.queue = [...batch, ...this.queue]; // вернуть в очередь, дошлём следующим флашем (seq сохранён)
            }
        }
        catch {
            this.queue = [...batch, ...this.queue];
        }
        finally {
            this.flushing = false;
        }
    }
    fetch(path, body, keepalive = false) {
        const f = this.opts.fetchImpl ?? fetch;
        return f(`${this.opts.connectorUrl}${path}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${this.sessionToken}` },
            body,
            keepalive,
        });
    }
}
function parsePlayer(launchParams) {
    const q = new URLSearchParams(launchParams.startsWith('?') ? launchParams.slice(1) : launchParams);
    const num = (v) => {
        const parsed = v === null || v === 'null' ? NaN : Number(v);
        return Number.isFinite(parsed) ? parsed : undefined;
    };
    return {
        clientId: q.get('UID') ?? '',
        username: q.get('Username') ?? undefined,
        coinsBalance: num(q.get('CoinsBalance')),
        counterBalance: num(q.get('CounterBalance')),
    };
}
function detectPlatform() {
    if (typeof navigator === 'undefined') {
        return 'unknown';
    }
    const ua = navigator.userAgent;
    if (/iPhone|iPad|iPod/.test(ua)) {
        return 'ios';
    }
    return /Android/.test(ua) ? 'android' : 'web';
}
function now() {
    return typeof performance !== 'undefined' ? performance.now() : Date.now();
}
