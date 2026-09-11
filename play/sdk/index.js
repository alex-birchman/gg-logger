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
/** Ошибка SDK с машиночитаемым кодом — игра может решить, показывать ретрай или нет. */
export class GgError extends Error {
    constructor(code, message, status) {
        super(message);
        this.code = code;
        this.status = status;
        this.name = 'GgError';
    }
}
const RETRY_BACKOFF_MS = [300, 1200, 3000];
const MAX_QUEUE = 5000;
const DEFAULT_TIMEOUT_MS = 10000;
export class GgSession {
    constructor(opts, sessionId, sessionToken, player, t0, 
    /** Серверные правила игры: курс награды, лимиты. Пусто, если сервер их не прислал. */
    config, 
    /** Сид для игр со случайностью — тем же сидом сервер повторит партию. */
    seed, serverTimeUtc) {
        this.opts = opts;
        this.sessionId = sessionId;
        this.sessionToken = sessionToken;
        this.player = player;
        this.t0 = t0;
        this.config = config;
        this.seed = seed;
        this.serverTimeUtc = serverTimeUtc;
        this.queue = [];
        this.seq = 0;
        /** У действий свой счётчик: на нём держится дедупликация на хосте, он не пересекается с событиями. */
        this.actionSeq = 0;
        this.inflight = Promise.resolve();
        this.finished = false;
        this.disposed = false;
        this.onHide = () => {
            if (typeof document === 'undefined' || document.visibilityState === 'hidden') {
                void this.flush(true);
            }
        };
        this.timer = setInterval(() => void this.flush(), this.opts.flushIntervalMs);
        if (typeof addEventListener === 'function') {
            // страница может закрыться до finish — дослать хвост.
            // visibilitychange надёжнее pagehide в WKWebView, слушаем оба.
            addEventListener('pagehide', this.onHide);
            addEventListener('visibilitychange', this.onHide);
        }
    }
    /**
     * Статический запрос до начала партии: правила, доступность, «сегодня уже играл».
     * Сессии ещё нет, поэтому identity — подписанные launch-параметры.
     * gameLoaded здесь НЕ шлём: это дело init, иначе лоадер приложения снимется раньше времени.
     */
    static async query(gameId, name, options) {
        const f = options.fetchImpl ?? fetch;
        const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
        const launchParams = options.launchParams ?? (typeof location !== 'undefined' ? location.search : '');
        let response;
        try {
            response = await f(`${options.connectorUrl}/api/v1/games/${encodeURIComponent(gameId)}/query`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ launchParams, name, payload: options.payload ?? {} }),
                signal: timeoutSignal(timeoutMs),
            });
        }
        catch (e) {
            throw new GgError('network', `game-connector unreachable: ${e.message}`);
        }
        if (!response.ok) {
            throw new GgError(codeForStatus(response.status), `query "${name}" failed with ${response.status}`, response.status);
        }
        return (await response.json());
    }
    /** Сообщает хосту gameLoaded и открывает сессию в коннекторе. */
    static async init(gameId, options) {
        sendGameLoaded();
        const launchParams = options.launchParams ?? (typeof location !== 'undefined' ? location.search : '');
        const f = options.fetchImpl ?? fetch;
        const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
        let response;
        try {
            response = await f(`${options.connectorUrl}/api/v1/games/${encodeURIComponent(gameId)}/sessions`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    launchParams,
                    platform: detectPlatform(),
                    gameVersion: options.gameVersion,
                    countryId: options.countryId,
                }),
                signal: timeoutSignal(timeoutMs),
            });
        }
        catch (e) {
            throw new GgError('network', `game-connector unreachable: ${e.message}`);
        }
        if (!response.ok) {
            throw new GgError(codeForStatus(response.status), `game-connector: start session failed with ${response.status}`, response.status);
        }
        const started = (await response.json());
        return new GgSession({
            ...options,
            batchSize: options.batchSize ?? 20,
            flushIntervalMs: options.flushIntervalMs ?? 3000,
            timeoutMs,
        }, started.sessionId, started.sessionToken, parsePlayer(launchParams), now(), 
        // у игры без onStart сервер присылает config: null — игре обещан undefined
        started.config ?? undefined, started.seed ?? null, started.serverTimeUtc);
    }
    /** Записать событие. Синхронно, в буфер; отправка батчами в фоне. */
    track(type, payload = {}) {
        if (this.finished) {
            // eslint-disable-next-line no-console
            console.warn(`gg: track("${type}") after finish() — событие отброшено`);
            return;
        }
        // сериализуем здесь, а не в отправке: иначе один нерасжёвываемый payload
        // (объект three.js, DOM-узел, циклическая ссылка) навсегда заблокировал бы очередь
        try {
            JSON.stringify(payload);
        }
        catch {
            // eslint-disable-next-line no-console
            console.error(`gg: payload события "${type}" не сериализуется — событие отброшено`);
            return;
        }
        if (this.queue.length >= MAX_QUEUE) {
            // eslint-disable-next-line no-console
            console.error('gg: очередь событий переполнена — событие отброшено');
            return;
        }
        this.queue.push({ seq: this.seq++, t: Math.round(now() - this.t0), type, payload });
        if (this.queue.length >= this.opts.batchSize) {
            void this.flush();
        }
    }
    /**
     * Игровое действие, которое обрабатывает плагин игры на сервере.
     *
     * Это точка синхронизации: track() возвращается сразу, поэтому в момент вызова часть
     * партии ещё не на сервере. Сначала дожидаемся отправки событий, потом шлём действие
     * с меткой последнего события — коннектор без него ответит 409 и плагин не запустит.
     *
     * Медленнее track(): здесь есть сетевой круг. Для обычных игровых событий используйте track().
     */
    async action(name, payload = {}) {
        if (this.finished) {
            // eslint-disable-next-line no-console
            console.warn(`gg: action("${name}") after finish() — отказ`);
            throw new GgError('after_finish', `action("${name}") вызван после finish()`);
        }
        // метка ставится ДО отправки: всё, что игра записала к этому моменту, должно быть на сервере
        const afterEventSeq = this.seq > 0 ? this.seq - 1 : null;
        // seq действия не меняется между попытками: на нём держится дедупликация на хосте
        const seq = this.actionSeq++;
        const body = JSON.stringify({ afterEventSeq, actions: [{ seq, name, payload }] });
        await this.flush();
        let conflictRetried = false;
        for (let attempt = 0;; attempt++) {
            if (this.disposed) {
                throw new GgError('after_finish', `action("${name}") отменён: сессия закрыта`);
            }
            let response;
            try {
                response = await this.send(`/api/v1/sessions/${this.sessionId}/action`, body);
            }
            catch (e) {
                if (attempt < RETRY_BACKOFF_MS.length) {
                    await delay(RETRY_BACKOFF_MS[attempt]);
                    continue;
                }
                throw new GgError('network', `action "${name}": ${e.message}`);
            }
            if (response.ok) {
                const parsed = (await response.json());
                // плагин вправе ничего не вернуть — тогда результат null, а не всё тело ответа
                return (parsed?.results ? parsed.results[0] : parsed);
            }
            // 409 — сервер ещё не увидел события до метки: один досыл и одна попытка
            if (response.status === 409) {
                if (conflictRetried) {
                    throw new GgError('events_not_delivered', `action "${name}": события до метки ${afterEventSeq} не доставлены`, 409);
                }
                conflictRetried = true;
                await this.flush();
                continue;
            }
            if (isRetryable(response.status) && attempt < RETRY_BACKOFF_MS.length) {
                await delay(RETRY_BACKOFF_MS[attempt]);
                continue;
            }
            throw new GgError(codeForStatus(response.status), `action "${name}" failed with ${response.status}`, response.status);
        }
    }
    /** Дослать события и получить вердикт сервера. Повторный вызов вернёт тот же вердикт. */
    async finish() {
        if (this.verdict) {
            return this.verdict;
        }
        this.finished = true;
        this.dispose();
        // дожидаемся того, что уже в полёте, и досылаем хвост с бэкоффом
        await this.flush();
        for (let attempt = 0; attempt < RETRY_BACKOFF_MS.length && this.queue.length > 0; attempt++) {
            await delay(RETRY_BACKOFF_MS[attempt]);
            await this.flush();
        }
        if (this.queue.length > 0) {
            // просить вердикт по неполному логу нельзя: сервер посчитает не ту партию
            throw new GgError('events_not_delivered', `gg: ${this.queue.length} событий не доставлено, вердикт не запрашивался`);
        }
        const response = await this.send(`/api/v1/sessions/${this.sessionId}/finish`, '');
        if (!response.ok) {
            throw new GgError(codeForStatus(response.status), `game-connector: finish failed with ${response.status}`, response.status);
        }
        this.verdict = (await response.json());
        return this.verdict;
    }
    /**
     * Остановить фоновую отправку и отменить незавершённые повторы действий.
     * Вызывается из finish(); зовите вручную, если бросаете партию.
     */
    dispose() {
        this.disposed = true;
        if (this.timer !== undefined) {
            clearInterval(this.timer);
            this.timer = undefined;
        }
        if (typeof removeEventListener === 'function') {
            removeEventListener('pagehide', this.onHide);
            removeEventListener('visibilitychange', this.onHide);
        }
    }
    /**
     * Отправки выстраиваются в цепочку: параллельных запросов нет, и await действительно
     * дожидается предыдущей отправки — иначе finish() уходил бы с неотправленным хвостом.
     */
    flush(keepalive = false) {
        const next = this.inflight.then(() => this.sendBatch(keepalive), () => this.sendBatch(keepalive));
        this.inflight = next.catch(() => undefined);
        return next;
    }
    async sendBatch(keepalive) {
        if (this.queue.length === 0) {
            return;
        }
        // keepalive ограничен 64 КБ на всё — на этом пути шлём один батч
        const batch = this.queue.splice(0, this.opts.batchSize);
        try {
            const response = await this.send(`/api/v1/sessions/${this.sessionId}/events`, JSON.stringify({ events: batch }), keepalive);
            if (response.ok) {
                return;
            }
            if (isRetryable(response.status)) {
                this.queue.unshift(...batch);
                return;
            }
            // 4xx повторять бессмысленно: 409 — сессия уже закрыта, 401 — токен протух,
            // 400 — мы шлём мусор. Батч выкидываем, иначе он заблокирует очередь навсегда.
            // eslint-disable-next-line no-console
            console.error(`gg: батч из ${batch.length} событий отклонён (${response.status}) и отброшен`);
        }
        catch {
            this.queue.unshift(...batch);
        }
    }
    send(path, body, keepalive = false) {
        const f = this.opts.fetchImpl ?? fetch;
        return f(`${this.opts.connectorUrl}${path}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${this.sessionToken}` },
            body,
            keepalive,
            signal: keepalive ? undefined : timeoutSignal(this.opts.timeoutMs),
        });
    }
}
function isRetryable(status) {
    return status === 408 || status === 429 || status >= 500;
}
function codeForStatus(status) {
    if (status === 401) {
        return 'session_expired';
    }
    return status === 403 ? 'unauthorized' : 'server';
}
function timeoutSignal(ms) {
    return typeof AbortSignal !== 'undefined' && 'timeout' in AbortSignal ? AbortSignal.timeout(ms) : undefined;
}
function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
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
//# sourceMappingURL=index.js.map