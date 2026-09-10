/**
 * Платформенный мост игра → хост-приложение.
 * Контракт снят логгером (GGHCKTHN-13): нативный лоадер приложения висит,
 * пока игра не сообщит gameLoaded. Каналы — как в pizza-tower eventPublisher.ts.
 */
/** Сообщить хосту, что игра загрузилась. Безопасно звать сколько угодно раз и вне браузера. */
export function sendGameLoaded(win) {
    const target = win ?? (typeof window === 'undefined' ? undefined : window);
    if (!target) {
        return;
    }
    const w = target;
    try {
        w.JSBridge?.gameLoaded?.();
    }
    catch {
        /* моста нет — не наша платформа */
    }
    try {
        w.webkit?.messageHandlers?.gameLoaded?.postMessage('');
    }
    catch {
        /* ignore */
    }
    try {
        if (w.parent && w.parent !== target) {
            w.parent.postMessage(JSON.stringify({ event: 'gameLoaded' }), '*');
        }
    }
    catch {
        /* ignore */
    }
}
